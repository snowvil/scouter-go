package main

import (
	"encoding/binary"
	"fmt"
)

// UDP magics from Java NetCafe (CAFE single, CAFN batch, CAFM fragment).
const (
	magicCafe     uint32 = 0x43414645
	magicBatch    uint32 = 0x4341464e
	magicFragment uint32 = 0x4341464d
)

func frameSingle(p []byte) []byte {
	out := make([]byte, 4, 4+len(p))
	binary.BigEndian.PutUint32(out, magicCafe)
	return append(out, p...)
}
func frameBatch(count int, payload []byte) []byte {
	out := make([]byte, 6, 6+len(payload))
	binary.BigEndian.PutUint32(out, magicBatch)
	binary.BigEndian.PutUint16(out[4:], uint16(count))
	return append(out, payload...)
}

// javaBlob mirrors DataOutputX.writeBlob length prefixes.
func javaBlob(dst, b []byte) []byte {
	switch n := len(b); {
	case n <= 253:
		dst = append(dst, byte(n))
	case n <= 65535:
		dst = append(dst, 255)
		dst = binary.BigEndian.AppendUint16(dst, uint16(n))
	default:
		dst = append(dst, 254)
		dst = binary.BigEndian.AppendUint32(dst, uint32(n))
	}
	return append(dst, b...)
}

// frameFragments mirrors DataUdpAgent.writeMTU: total = ceil(len/size), 0-based
// num, each fragment carries the agent objHash, pkid and a Blob slice.
func frameFragments(objHash int32, pkid int64, data []byte, size int) ([][]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("fragment size %d", size)
	}
	total := (len(data) + size - 1) / size
	if total > 32767 {
		return nil, fmt.Errorf("fragment count %d exceeds int16", total)
	}
	out := make([][]byte, 0, total)
	for num := 0; num < total; num++ {
		part := data[num*size : min((num+1)*size, len(data))]
		b := make([]byte, 0, 4+4+8+2+2+5+len(part))
		b = binary.BigEndian.AppendUint32(b, magicFragment)
		b = binary.BigEndian.AppendUint32(b, uint32(objHash))
		b = binary.BigEndian.AppendUint64(b, uint64(pkid))
		b = binary.BigEndian.AppendUint16(b, uint16(total))
		b = binary.BigEndian.AppendUint16(b, uint16(num))
		b = javaBlob(b, part)
		out = append(out, b)
	}
	return out, nil
}

// datagram is one framed UDP payload plus the transactions whose XLog it carries.
type datagram struct {
	Kind  string // single | batch | fragment
	Bytes []byte
	// Txs are XLog transaction indexes carried by this datagram. Every fragment of a
	// fragmented XLog lists the tx (a lost fragment loses the XLog); Final marks the
	// datagram whose send completes the XLog (used for send-lag measurement).
	Txs   []int64
	Final bool
}

// batcher follows DataUdpAgent.write(List<byte[]>): a pack larger than maxBytes is
// fragmented immediately (ahead of buffered packs, as in Java); otherwise packs are
// appended until maxBytes would be exceeded. maxPacks additionally bounds a batch.
// maxPacks == 1 selects DataUdpAgent.write(byte[]) (CAFE single). The emit callback
// owns each datagram; the batcher retains at most one bounded buffer.
type batcher struct {
	maxBytes, maxPacks int
	objHash            int32
	nextPKID           func() int64
	buf                []byte
	count              int
	txs                []int64
	emit               func(datagram)
}

func (b *batcher) add(p pack) error {
	if len(p.Bytes) > b.maxBytes {
		frags, err := frameFragments(b.objHash, b.nextPKID(), p.Bytes, b.maxBytes)
		if err != nil {
			return err
		}
		for i, f := range frags {
			d := datagram{Kind: "fragment", Bytes: f, Final: i == len(frags)-1}
			if p.Tx >= 0 {
				d.Txs = []int64{p.Tx}
			}
			b.emit(d)
		}
		return nil
	}
	if b.maxPacks == 1 {
		d := datagram{Kind: "single", Bytes: frameSingle(p.Bytes), Final: true}
		if p.Tx >= 0 {
			d.Txs = []int64{p.Tx}
		}
		b.emit(d)
		return nil
	}
	if b.count > 0 && (len(p.Bytes)+len(b.buf) > b.maxBytes || b.count >= b.maxPacks) {
		b.flush()
	}
	b.buf = append(b.buf, p.Bytes...)
	b.count++
	if p.Tx >= 0 {
		b.txs = append(b.txs, p.Tx)
	}
	return nil
}
func (b *batcher) flush() {
	if b.count == 0 {
		return
	}
	b.emit(datagram{Kind: "batch", Bytes: frameBatch(b.count, b.buf), Txs: b.txs, Final: true})
	b.buf, b.count, b.txs = nil, 0, nil
}
