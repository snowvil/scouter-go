package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Scouter Client TCP protocol (TcpProxy): magic 0xcafe2001 once per connection,
// then per request Text(command) Int64(session) Pack(param); response is a
// sequence of 0x03+Pack terminated by 0x04, or 0x44 for an invalid session.
const (
	tcpClientMagic uint32 = 0xcafe2001
	flagHasNext    byte   = 0x03
	flagNoNext     byte   = 0x04
	flagInvalid    byte   = 0x44
)

var errInvalidSession = errors.New("server returned INVALID_SESSION")

// errClosedBeforeResponse: the server had closed the connection before this
// request got any response byte (EOF or reset on the write or on the first
// read), e.g. its net_tcp_client_so_timeout_ms idle close.
var errClosedBeforeResponse = errors.New("server closed the connection before any response byte")

type client struct {
	conn    net.Conn
	r       *bufio.Reader
	session int64
	timeout time.Duration
	// lastUsed is when the last complete response was read (the server's
	// idle timer starts no earlier).
	lastUsed time.Time
}

func peerClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNABORTED)
}

// scouterPassword is the Client-side SHA-256 used by the secure LOGIN.
func scouterPassword(password string) string {
	sum := sha256.Sum256([]byte("qwertyuiop!@#$%^&*()zxcvbnm,." + password))
	return hex.EncodeToString(sum[:])
}

func dialClient(ctx context.Context, addr, user, password string, timeout time.Duration) (*client, error) {
	if err := requireLoopback(addr); err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &client{conn: conn, r: bufio.NewReaderSize(conn, 64<<10), timeout: timeout}
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], tcpClientMagic)
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(magic[:]); err != nil {
		conn.Close()
		return nil, err
	}
	login := &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "id", Value: protocol.Value{Type: protocol.ValueText, Text: user}},
		{Key: "pass", Value: protocol.Value{Type: protocol.ValueText, Text: scouterPassword(password)}},
	}}
	var session int64
	err = c.call(ctx, "LOGIN", login, func(p protocol.Pack) error {
		if m, ok := p.(*protocol.MapPack); ok {
			for _, e := range m.Entries {
				if e.Key == "session" && e.Value.Type == protocol.ValueDecimal {
					session = e.Value.Int
				}
			}
		}
		return nil
	})
	if err == nil && session == 0 {
		err = errors.New("login failed: session 0")
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	c.session = session
	return c, nil
}

func (c *client) Close() error {
	c.conn.SetDeadline(time.Now().Add(time.Second))
	enc := protocol.NewEncoder(protocol.Limits{})
	enc.Text("CLOSE")
	if b, err := enc.Bytes(); err == nil {
		c.conn.Write(b)
	}
	return c.conn.Close()
}

// call sends one command (param nil: no body) and streams every response Pack to each. A transport or
// decode failure leaves the connection unusable; the caller must reconnect.
func (c *client) call(ctx context.Context, command string, param protocol.Pack, each func(protocol.Pack) error) error {
	deadline := time.Now().Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	c.conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { c.conn.SetDeadline(time.Now()) })
	defer stop()
	enc := protocol.NewEncoder(protocol.Limits{})
	enc.Text(command)
	enc.Int64(c.session)
	if param != nil { // nil: a command without body (TcpProxy.process with null param)
		enc.Pack(param)
	}
	raw, err := enc.Bytes()
	if err != nil {
		return err
	}
	if _, err := c.conn.Write(raw); err != nil {
		if peerClosed(err) {
			return fmt.Errorf("%s request: %w: %w", command, errClosedBeforeResponse, err)
		}
		return err
	}
	for first := true; ; first = false {
		flag, err := c.r.ReadByte()
		if err != nil {
			if first && peerClosed(err) {
				return fmt.Errorf("%s response: %w: %w", command, errClosedBeforeResponse, err)
			}
			return fmt.Errorf("%s response: %w", command, err)
		}
		switch flag {
		case flagNoNext:
			c.lastUsed = time.Now()
			return nil
		case flagInvalid:
			return errInvalidSession
		case flagHasNext:
			p, err := protocol.ReadPack(c.r, protocol.Limits{})
			if err != nil {
				return fmt.Errorf("%s pack: %w", command, err)
			}
			if err := each(p); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s: unexpected response flag 0x%02x", command, flag)
		}
	}
}

func decimal(n int64) protocol.Value { return protocol.Value{Type: protocol.ValueDecimal, Int: n} }
func hashList(objects []object) protocol.Value {
	values := make([]protocol.Value, len(objects))
	for i, o := range objects {
		values[i] = decimal(int64(o.Hash))
	}
	return protocol.Value{Type: protocol.ValueList, Values: values}
}
func mapGet(m *protocol.MapPack, key string) protocol.Value {
	for _, e := range m.Entries {
		if e.Key == key {
			return e.Value
		}
	}
	return protocol.Value{}
}

// realtimeCursor issues TRANX_REAL_TIME_GROUP (limit 0) from the (loop,index) cursor
// and advances it. Every returned XLog is passed to each.
func (c *client) realtimeCursor(ctx context.Context, loop *int64, index *int64, objects []object, each func(*protocol.XLogPack)) (int, error) {
	param := &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "loop", Value: decimal(*loop)}, {Key: "index", Value: decimal(*index)},
		{Key: "limit", Value: decimal(0)}, {Key: "objHash", Value: hashList(objects)},
	}}
	n, first := 0, true
	err := c.call(ctx, "TRANX_REAL_TIME_GROUP", param, func(p protocol.Pack) error {
		if first {
			first = false
			m, ok := p.(*protocol.MapPack)
			if !ok {
				return fmt.Errorf("realtime: first pack is %T, want MapPack", p)
			}
			*loop, *index = mapGet(m, "loop").Int, mapGet(m, "index").Int
			return nil
		}
		x, ok := p.(*protocol.XLogPack)
		if !ok {
			return fmt.Errorf("realtime: pack %T", p)
		}
		n++
		if each != nil {
			each(x)
		}
		return nil
	})
	return n, err
}

// latestCursor returns the realtime ring (loop,index) with one cheap request.
func (c *client) latestCursor(ctx context.Context) (int64, int64, error) {
	var loop, index int64
	first := true
	err := c.call(ctx, "TRANX_REAL_TIME_GROUP_LATEST", &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "loop", Value: decimal(0)}, {Key: "index", Value: decimal(0)}, {Key: "count", Value: decimal(1)},
	}}, func(p protocol.Pack) error {
		if first {
			first = false
			if m, ok := p.(*protocol.MapPack); ok {
				loop, index = mapGet(m, "loop").Int, mapGet(m, "index").Int
				return nil
			}
			return fmt.Errorf("latest: first pack is %T", p)
		}
		return nil
	})
	return loop, index, err
}

// history issues TRANX_LOAD_TIME_GROUP for [from,to] on date (max 0 = unlimited).
func (c *client) history(ctx context.Context, date string, from, to int64, maxRecords int, objects []object, each func(*protocol.XLogPack)) (int, error) {
	param := &protocol.MapPack{Entries: []protocol.Entry{
		{Key: "date", Value: protocol.Value{Type: protocol.ValueText, Text: date}},
		{Key: "stime", Value: decimal(from)}, {Key: "etime", Value: decimal(to)},
		{Key: "limit", Value: decimal(0)}, {Key: "max", Value: decimal(int64(maxRecords))},
		{Key: "reverse", Value: protocol.Value{Type: protocol.ValueBoolean, Bool: false}},
		{Key: "objHash", Value: hashList(objects)},
	}}
	n := 0
	err := c.call(ctx, "TRANX_LOAD_TIME_GROUP", param, func(p protocol.Pack) error {
		x, ok := p.(*protocol.XLogPack)
		if !ok {
			return fmt.Errorf("history: pack %T", p)
		}
		n++
		if each != nil {
			each(x)
		}
		return nil
	})
	return n, err
}
