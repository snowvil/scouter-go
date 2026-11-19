package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const (
	UDPCafe         uint32 = 0x43414645
	UDPJava         uint32 = 0x4a415641
	UDPCafeBatch    uint32 = 0x4341464e
	UDPJavaBatch    uint32 = 0x4a41564e
	UDPCafeFragment uint32 = 0x4341464d
	UDPJavaFragment uint32 = 0x4a4d5455
)

const (
	// MaxUDPPayload is the largest IPv4 UDP payload (65535 - 20 - 8).
	MaxUDPPayload = 65507
	// MaxUDPWorkers bounds decode goroutines whatever the configuration says.
	MaxUDPWorkers = 256
	// DefaultJavaUDPWorkers is Java _net_udp_worker_thread_count.
	DefaultJavaUDPWorkers = 3
	// DefaultUDPWorkers is the Go default without the key. Java's workers
	// decode and hand each pack to a per-type core queue; Go's workers run
	// the ingest under one engine lock, so a third worker adds lock
	// handoffs, not throughput (measured, local replay: at 16,000 mixed XLog/s the UDP
	// queue peaked at 223-262 with 2 workers and 425-687 with 3).
	DefaultUDPWorkers = 2
)

// UDPPacketBufferSize maps net_udp_packet_buffer_size (Java default 65535) to
// UDPConfig.MaxDatagramBytes. Values above MaxUDPPayload are clamped; values
// below 1 (which make Java drop every datagram or fail its receive loop) select
// MaxUDPPayload. adjusted reports either case so the caller can log it. A
// datagram larger than the result is still rejected, never parsed truncated.
func UDPPacketBufferSize(value int) (size int, adjusted bool) {
	if value < 1 || value > MaxUDPPayload {
		return MaxUDPPayload, true
	}
	return value, false
}

// UDPWorkerCount maps _net_udp_worker_thread_count to UDPConfig.Workers:
// values below 1 select the Java default 3, values above MaxUDPWorkers are
// clamped. adjusted reports either case.
func UDPWorkerCount(value int) (workers int, adjusted bool) {
	switch {
	case value < 1:
		return DefaultJavaUDPWorkers, true
	case value > MaxUDPWorkers:
		return MaxUDPWorkers, true
	}
	return value, false
}

// UDPConfig bounds payload memory plus Workers/QueueCapacity datagrams (at
// most MaxQueuedBytes of them) and MaxAssemblies*MaxFragments slice slots.
// Zero fields select finite defaults.
type UDPConfig struct {
	Workers, QueueCapacity, MaxDatagramBytes, MaxBatchPackets       int
	MaxFragments, MaxAssemblies, MaxAssemblyBytes, MaxBufferedBytes int
	MaxQueuedBytes                                                  int
	// ReadBuffer, when > 0, is set as SO_RCVBUF by NewUDPReceiver
	// (net_udp_so_rcvbuf_size) and reported in UDPStats. 0 leaves the socket
	// as the caller opened it.
	ReadBuffer  int
	FragmentTTL time.Duration
	CodecLimits protocol.Limits
}

func (c UDPConfig) normalized() (UDPConfig, error) {
	fields := []struct {
		value    *int
		fallback int
	}{
		// 8192 datagrams ride out scheduling stalls of a busy host (measured, local replay:
		// 1024 rejected bursts at loads 8192 kept whole); MaxQueuedBytes keeps
		// the queue's memory bounded whatever the datagram sizes.
		{&c.Workers, DefaultUDPWorkers}, {&c.QueueCapacity, 8192}, {&c.MaxDatagramBytes, 65507},
		{&c.MaxBatchPackets, 1024}, {&c.MaxFragments, 1024}, {&c.MaxAssemblies, 128},
		{&c.MaxAssemblyBytes, 8 << 20}, {&c.MaxBufferedBytes, 16 << 20}, {&c.MaxQueuedBytes, 32 << 20},
		{&c.ReadBuffer, 0},
	}
	for _, f := range fields {
		v, err := positive(*f.value, f.fallback)
		if err != nil {
			return c, err
		}
		*f.value = v
	}
	if c.MaxDatagramBytes > 65535 || c.MaxFragments > 32767 || c.MaxBatchPackets > 32767 || c.Workers > MaxUDPWorkers {
		return c, fmt.Errorf("%w: UDP configuration exceeds wire bounds", ErrMalformed)
	}
	var err error
	c.FragmentTTL, err = duration(c.FragmentTTL, time.Second)
	if err != nil {
		return c, err
	}
	return c, validCodecLimits(c.CodecLimits)
}

type fragmentKey struct {
	Sender netip.AddrPort
	ID     int64
	Object int32
}
type assembly struct {
	parts           [][]byte
	expires         time.Time
	bytes, received int
}
type ExpiredAssembly struct {
	Sender          netip.AddrPort
	PacketID        int64
	Bytes           int
	ObjectHash      int32
	Total, Received int
}

// expiredError is returned by Decode when a fragment arrives for an assembly
// whose TTL already passed; errors.Is(err, ErrExpired) holds.
type expiredError struct{ assembly ExpiredAssembly }

func (e *expiredError) Error() string { return ErrExpired.Error() }
func (e *expiredError) Unwrap() error { return ErrExpired }

func expiredFrom(key fragmentKey, a *assembly) ExpiredAssembly {
	return ExpiredAssembly{Sender: key.Sender, PacketID: key.ID, Bytes: a.bytes, ObjectHash: key.Object, Total: len(a.parts), Received: a.received}
}

// Reassembler retains owned fragment bytes. Its key intentionally isolates source
// address+port+object+pkid rather than Java's unsafe pkid-only global map.
type Reassembler struct {
	config     UDPConfig
	mu         sync.Mutex
	assemblies map[fragmentKey]*assembly
	buffered   int
}

func NewReassembler(config UDPConfig) (*Reassembler, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Reassembler{config: config, assemblies: make(map[fragmentKey]*assembly)}, nil
}
func (r *Reassembler) Pending() int       { r.mu.Lock(); defer r.mu.Unlock(); return len(r.assemblies) }
func (r *Reassembler) BufferedBytes() int { r.mu.Lock(); defer r.mu.Unlock(); return r.buffered }
func (r *Reassembler) remove(key fragmentKey) {
	a := r.assemblies[key]
	if a != nil {
		r.buffered -= a.bytes
		delete(r.assemblies, key)
	}
}
func (r *Reassembler) Expire(now time.Time) []ExpiredAssembly {
	r.mu.Lock()
	defer r.mu.Unlock()
	var expired []ExpiredAssembly
	for key, a := range r.assemblies {
		if !now.Before(a.expires) {
			expired = append(expired, expiredFrom(key, a))
			r.remove(key)
		}
	}
	return expired
}
func (r *Reassembler) clear() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.assemblies)
	clear(r.assemblies)
	r.buffered = 0
	return n
}

// Decode returns zero packets while a valid assembly is incomplete. A datagram
// batch is validated in full before any packet is returned to the dispatcher.
func (r *Reassembler) Decode(sender netip.AddrPort, data []byte, now time.Time) ([]protocol.Pack, error) {
	packets, _, err := r.decode(sender, data, now)
	return packets, err
}

// assembled describes a completed fragment assembly for the Java S160 line.
type assembled struct {
	object int32
	total  int
}

func (r *Reassembler) decode(sender netip.AddrPort, data []byte, now time.Time) ([]protocol.Pack, *assembled, error) {
	if !sender.IsValid() {
		return nil, nil, fmt.Errorf("%w: missing sender", ErrMalformed)
	}
	sender = netip.AddrPortFrom(sender.Addr().Unmap(), sender.Port())
	if len(data) > r.config.MaxDatagramBytes {
		return nil, nil, fmt.Errorf("%w: datagram bytes", ErrLimit)
	}
	// data is this datagram's own copy (Serve), read by nobody after the
	// decode: the packs may alias it instead of copying their byte fields.
	d := protocol.NewDecoder(data, r.config.CodecLimits)
	d.AdoptInput()
	magic := uint32(d.Int32())
	if d.Err() != nil {
		return nil, nil, d.Err()
	}
	switch magic {
	case UDPCafe, UDPJava:
		p := d.Pack()
		if d.Err() != nil {
			return nil, nil, d.Err()
		}
		if d.Remaining() != 0 {
			return nil, nil, protocol.ErrTrailing
		}
		return []protocol.Pack{p}, nil, nil
	case UDPCafeBatch, UDPJavaBatch:
		count := int(d.Int16())
		if d.Err() != nil {
			return nil, nil, d.Err()
		}
		if count < 0 {
			return nil, nil, fmt.Errorf("%w: negative batch count", ErrMalformed)
		}
		if count > r.config.MaxBatchPackets {
			return nil, nil, fmt.Errorf("%w: batch count", ErrLimit)
		}
		packets := make([]protocol.Pack, 0, count)
		for i := 0; i < count; i++ {
			p := d.Pack()
			if d.Err() != nil {
				return nil, nil, d.Err()
			}
			packets = append(packets, p)
		}
		if d.Remaining() != 0 {
			return nil, nil, protocol.ErrTrailing
		}
		return packets, nil, nil
	case UDPCafeFragment, UDPJavaFragment:
		key := fragmentKey{Sender: sender, Object: d.Int32(), ID: d.Int64()}
		total, index := int(d.Int16()), int(d.Int16())
		part := d.Blob()
		if d.Err() != nil {
			return nil, nil, d.Err()
		}
		if d.Remaining() != 0 {
			return nil, nil, protocol.ErrTrailing
		}
		if total <= 0 || index < 0 || index >= total {
			return nil, nil, fmt.Errorf("%w: fragment coordinates", ErrMalformed)
		}
		if total > r.config.MaxFragments {
			return nil, nil, fmt.Errorf("%w: fragment count", ErrAssemblyLimit)
		}
		complete, err := r.addFragment(key, total, index, part, now)
		if err != nil || complete == nil {
			return nil, nil, err
		}
		p, err := protocol.DecodePackAdopt(complete, r.config.CodecLimits) // complete is the assembly's own buffer
		if err != nil {
			return nil, nil, err
		}
		return []protocol.Pack{p}, &assembled{object: key.Object, total: total}, nil
	default:
		return nil, nil, fmt.Errorf("%w: UDP magic %08x", ErrUnsupported, magic)
	}
}
func (r *Reassembler) addFragment(key fragmentKey, total, index int, part []byte, now time.Time) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.assemblies[key]
	if a != nil && !now.Before(a.expires) {
		expired := expiredFrom(key, a)
		r.remove(key)
		return nil, &expiredError{assembly: expired}
	}
	if a == nil {
		if len(r.assemblies) >= r.config.MaxAssemblies {
			return nil, fmt.Errorf("%w: pending assemblies", ErrAssemblyLimit)
		}
		a = &assembly{parts: make([][]byte, total), expires: now.Add(r.config.FragmentTTL)}
		r.assemblies[key] = a
	}
	if len(a.parts) != total {
		r.remove(key)
		return nil, fmt.Errorf("%w: fragment count changed", ErrMalformed)
	}
	if a.parts[index] != nil {
		if !bytes.Equal(a.parts[index], part) {
			r.remove(key)
			return nil, fmt.Errorf("%w: conflicting fragment duplicate", ErrMalformed)
		}
		return nil, nil
	}
	if len(part) > r.config.MaxAssemblyBytes-a.bytes || len(part) > r.config.MaxBufferedBytes-r.buffered {
		r.remove(key)
		return nil, fmt.Errorf("%w: buffered fragments", ErrAssemblyLimit)
	}
	a.parts[index] = part
	a.bytes += len(part)
	r.buffered += len(part)
	a.received++
	if a.received != total {
		return nil, nil
	}
	output := make([]byte, 0, a.bytes)
	for _, p := range a.parts {
		output = append(output, p...)
	}
	r.remove(key)
	return output, nil
}

type Packet struct {
	Sender netip.AddrPort
	Pack   protocol.Pack
}

// PacketHandler must honor ctx and return promptly on cancellation. It receives
// detached packet data; retaining it does not borrow the socket read buffer.
type PacketHandler func(context.Context, Packet) error

// Rejected counts datagrams (queue/size/shutdown), Malformed counts decode failures
// other than reassembly bounds (AssemblyRejected),
// Packets/HandlerErrors/AbortedPackets count decoded Pack dispatches, and
// Expired/AbortedAssemblies count fragment assemblies. These are not TPS.
// ShutdownDiscarded counts accepted datagrams still queued at shutdown; Rejected
// counts oversize datagrams and queue saturation only.
// QueueLen/QueueCap are the current length and capacity of the worker queue
// (Java NetDataProcessor queue). Both are 0 while no Serve call owns a queue:
// before Serve and after Serve has drained and joined its workers.
type UDPStats struct {
	Datagrams, Packets, Rejected, Malformed, Expired, HandlerErrors, AbortedAssemblies, AbortedPackets uint64
	ShutdownDiscarded                                                                                  uint64
	QueueLen, QueueCap                                                                                 int
	// AssemblyRejected counts fragment datagrams refused by a reassembly
	// bound (ErrAssemblyLimit: pending assemblies, buffered bytes, bytes or
	// fragments of one assembly). They are not in Malformed.
	AssemblyRejected uint64
	// Socket diagnostics (udp_diag.go). ReadBufferRequested is
	// UDPConfig.ReadBuffer (0: the receiver did not set SO_RCVBUF);
	// ReadBufferEffective is what getsockopt(SO_RCVBUF) reports (Linux
	// reports twice the granted size, capped by net.core.rmem_max).
	ReadBufferRequested, ReadBufferEffective int
	// Kernel counters, Linux /proc only (udp_diag.go). KernelCounters is
	// true when this socket's row was read: SocketDrops (/proc/net/udp{,6}
	// drops) and SocketQueuedBytes (rx_queue, receive buffer memory in
	// truesize, more than the payload). HostCounters is true when the
	// host-wide UDP counters since boot were read (/proc/net/snmp{,6},
	// IPv4+IPv6, every UDP socket of the network namespace): HostInErrors
	// already includes HostRcvbufErrors, do not add them. KernelCountersError
	// says why a part was not read ("" when both were).
	KernelCounters, HostCounters                                   bool
	KernelCountersError                                            string
	SocketDrops, SocketQueuedBytes, HostRcvbufErrors, HostInErrors uint64
}
type udpCounters struct {
	datagrams, packets, rejected, malformed, expired, handlerErrors, aborted, abortedPackets, shutdownDiscarded atomic.Uint64
	assemblyRejected                                                                                            atomic.Uint64
}
type datagram struct {
	sender netip.AddrPort
	bytes  []byte
}

type UDPReceiver struct {
	conn            *net.UDPConn
	config          UDPConfig
	assembler       *Reassembler
	handler         PacketHandler
	errors          ErrorHandler
	stats           udpCounters
	serving, closed atomic.Bool
	closeOnce       sync.Once
	log             logSink
	objectName      atomic.Pointer[ObjectNameFunc]
	logMultipacket  atomic.Bool
	logExpired      atomic.Bool
	expiredLimit    rateLimit
	queue           atomic.Pointer[chan datagram] // set only while Serve owns it
	queuedBytes     atomic.Int64                  // payload bytes in queue
	diag            *udpDiag
}

// SetLogFunc installs the Java Logger bridge for S150/S160; nil disables them.
func (r *UDPReceiver) SetLogFunc(fn LogFunc) { r.log.set(fn) }

// SetObjectNameFunc supplies the object name printed by S150/S160.
func (r *UDPReceiver) SetObjectNameFunc(fn ObjectNameFunc) {
	if fn == nil {
		r.objectName.Store(nil)
		return
	}
	r.objectName.Store(&fn)
}

// SetLogUDPMultipacket is log_udp_multipacket (Java default false).
func (r *UDPReceiver) SetLogUDPMultipacket(enabled bool) { r.logMultipacket.Store(enabled) }

// SetLogExpiredMultipacket is log_expired_multipacket (Java default true).
func (r *UDPReceiver) SetLogExpiredMultipacket(enabled bool) { r.logExpired.Store(enabled) }

func (r *UDPReceiver) objectLabel(objHash int32) string {
	name, ok := "", false
	if fn := r.objectName.Load(); fn != nil {
		name, ok = (*fn)(objHash)
	}
	if !ok {
		name = "null" // StringBuffer.append((String) null)
	}
	return "object=(" + strconv.FormatInt(int64(objHash), 10) + ")" + sanitizeLogText(name)
}

// logExpiredAssembly is MultiPacketProcessor.checkExpired: Logger.println("S150", 10, ...).
func (r *UDPReceiver) logExpiredAssembly(a ExpiredAssembly, now time.Time) {
	if !r.logExpired.Load() || !r.log.enabled() || !r.expiredLimit.allow(now, 10*time.Second) {
		return
	}
	r.log.emit("S150", "MultiPacket total="+strconv.Itoa(a.Total)+" recv="+strconv.Itoa(a.Received)+" "+r.objectLabel(a.ObjectHash)+" "+javaInetAddress(a.Sender.Addr()))
}

// logAssembled is NetDataProcessor.processCafeMTU: Logger.println("S160", ...)
// after the reassembled Pack was processed.
func (r *UDPReceiver) logAssembled(p protocol.Pack, info *assembled, sender netip.AddrPort) {
	if info == nil || !r.logMultipacket.Load() || !r.log.enabled() {
		return
	}
	r.log.emit("S160", "recv "+packClassName(p)+" total="+strconv.Itoa(info.total)+" "+r.objectLabel(info.object)+" "+javaInetAddress(sender.Addr()))
}

// NewUDPReceiver transfers socket ownership to the receiver on success. Serve
// closes it and joins workers; Close may be used to stop a blocking socket read.
func NewUDPReceiver(conn *net.UDPConn, config UDPConfig, handler PacketHandler, onError ErrorHandler) (*UDPReceiver, error) {
	if conn == nil || handler == nil {
		return nil, fmt.Errorf("%w: UDP socket and handler required", ErrMalformed)
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	assembler, err := NewReassembler(config)
	if err != nil {
		return nil, err
	}
	diag, err := newUDPDiag(conn, config.ReadBuffer)
	if err != nil {
		return nil, fmt.Errorf("udp read buffer: %w", err)
	}
	receiver := &UDPReceiver{conn: conn, config: config, assembler: assembler, handler: handler, errors: onError, diag: diag}
	receiver.logExpired.Store(true)
	return receiver, nil
}
func (r *UDPReceiver) Close() error {
	var err error
	r.closeOnce.Do(func() { r.closed.Store(true); err = r.conn.Close() })
	return err
}
func (r *UDPReceiver) Stats() UDPStats {
	st := UDPStats{Datagrams: r.stats.datagrams.Load(), Packets: r.stats.packets.Load(), Rejected: r.stats.rejected.Load(), Malformed: r.stats.malformed.Load(),
		Expired: r.stats.expired.Load(), HandlerErrors: r.stats.handlerErrors.Load(), AbortedAssemblies: r.stats.aborted.Load(), AbortedPackets: r.stats.abortedPackets.Load(),
		ShutdownDiscarded: r.stats.shutdownDiscarded.Load(), AssemblyRejected: r.stats.assemblyRejected.Load()}
	// len/cap on a channel are safe concurrently with sends, receives and close.
	if queue := r.queue.Load(); queue != nil {
		st.QueueLen, st.QueueCap = len(*queue), cap(*queue)
	}
	r.diag.fill(&st, time.Now())
	return st
}

func (r *UDPReceiver) Serve(parent context.Context) error {
	if r.closed.Load() || !r.serving.CompareAndSwap(false, true) {
		return ErrClosed
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(ctx, func() { r.Close() })
	queue := make(chan datagram, r.config.QueueCapacity)
	r.queue.Store(&queue)
	var workers sync.WaitGroup
	for i := 0; i < r.config.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range queue {
				r.queuedBytes.Add(-int64(len(item.bytes)))
				if ctx.Err() != nil {
					r.stats.shutdownDiscarded.Add(1)
					continue
				}
				now := time.Now()
				packets, info, err := r.assembler.decode(item.sender, item.bytes, now)
				if err != nil {
					if errors.Is(err, ErrExpired) {
						r.stats.expired.Add(1)
						report(r.errors, "udp.expired", item.sender.String(), err)
						var expired *expiredError
						if errors.As(err, &expired) {
							r.logExpiredAssembly(expired.assembly, now)
						}
						continue
					}
					if errors.Is(err, ErrAssemblyLimit) {
						r.stats.assemblyRejected.Add(1)
						report(r.errors, "udp.assembly", item.sender.String(), err)
						continue
					}
					r.stats.malformed.Add(1)
					report(r.errors, "udp.decode", item.sender.String(), err)
					continue
				}
				for index, p := range packets {
					if ctx.Err() != nil {
						r.stats.abortedPackets.Add(uint64(len(packets) - index))
						break
					}
					if err := r.handler(ctx, Packet{Sender: item.sender, Pack: p}); err != nil {
						r.stats.handlerErrors.Add(1)
						report(r.errors, "udp.handler", item.sender.String(), err)
					} else {
						r.stats.packets.Add(1)
						r.logAssembled(p, info, item.sender)
					}
				}
			}
		}()
	}
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		interval := r.config.FragmentTTL / 2
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				for _, expired := range r.assembler.Expire(now) {
					r.stats.expired.Add(1)
					report(r.errors, "udp.expired", expired.Sender.String(), fmt.Errorf("%w: packet %d", ErrExpired, expired.PacketID))
					r.logExpiredAssembly(expired, now)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	var result error
	buffer := make([]byte, r.config.MaxDatagramBytes+1)
	for {
		n, _, flags, sender, err := r.conn.ReadMsgUDPAddrPort(buffer, nil)
		if err != nil {
			if parent.Err() != nil {
				result = parent.Err()
			} else if !r.closed.Load() {
				result = err
				report(r.errors, "udp.read", "", err)
			}
			break
		}
		r.stats.datagrams.Add(1)
		if n > r.config.MaxDatagramBytes || flags&syscall.MSG_TRUNC != 0 {
			r.stats.rejected.Add(1)
			report(r.errors, "udp.limit", sender.String(), ErrLimit)
			continue
		}
		if r.queuedBytes.Load()+int64(n) > int64(r.config.MaxQueuedBytes) {
			r.stats.rejected.Add(1)
			report(r.errors, "udp.queue", sender.String(), ErrLimit)
			continue
		}
		item := datagram{sender: sender, bytes: append([]byte(nil), buffer[:n]...)}
		r.queuedBytes.Add(int64(n))
		select {
		case queue <- item:
		default:
			r.queuedBytes.Add(-int64(n))
			r.stats.rejected.Add(1)
			report(r.errors, "udp.queue", sender.String(), ErrLimit)
		}
	}
	cancel()
	stop()
	r.Close()
	close(queue)
	workers.Wait()
	r.queue.Store(nil) // drained: workers discard the remainder before exiting
	sweeper.Wait()
	r.stats.aborted.Add(uint64(r.assembler.clear()))
	return result
}
