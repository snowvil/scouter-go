// Copyright 2015 Scouter authors, Apache-2.0.
// BatchPack.writeSimple/readSimple at pinned source dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5.
package protocol

// EncodeBatchSimple returns Java writeSimple's Blob body. It has no Pack type
// or HasNEXT byte and omits SQL details while preserving SQLTotalCount metadata.
func EncodeBatchSimple(p *BatchPack, limits Limits) ([]byte, error) {
	if p == nil || p.SQLTotalCount < 0 {
		return nil, ErrInvalid
	}
	var e Encoder
	e.init(limits, encoderSizeHint)
	start := e.beginBlob()
	b := &e
	b.Int64(p.StartTime)
	b.Int32(p.ObjHash)
	b.Text(p.BatchJobID)
	b.Text(p.Args)
	b.Int32(p.PID)
	b.Int64(p.ElapsedTime)
	b.Int32(p.ThreadCount)
	b.Int64(p.CPUTime)
	b.Int64(p.GCTime)
	b.Int64(p.GCCount)
	b.Int32(p.SQLTotalCount)
	b.Int64(p.SQLTotalTime)
	b.Int64(p.SQLTotalRows)
	b.Int64(p.SQLTotalRuns)
	b.Bool(p.IsStack)
	b.Text(p.ObjName)
	b.Text(p.ObjType)
	b.Int64(p.Position)
	e.endBlob(start)
	return e.Bytes()
}

// DecodeBatchSimple decodes a complete writeSimple Blob, with owned strings and
// no SQL arrays. A positive SQLTotalCount does not imply detail arrays are present.
func DecodeBatchSimple(data []byte, limits Limits) (*BatchPack, error) {
	var d Decoder
	d.init(data, limits)
	body := d.blobView()
	if d.Err() != nil {
		return nil, d.Err()
	}
	if d.Remaining() != 0 {
		return nil, ErrInvalid
	}
	d.enterBody(body)
	b := &d
	p := &BatchPack{StartTime: b.Int64(), ObjHash: b.Int32(), BatchJobID: b.Text(), Args: b.Text(), PID: b.Int32(), ElapsedTime: b.Int64(), ThreadCount: b.Int32(), CPUTime: b.Int64(), GCTime: b.Int64(), GCCount: b.Int64(), SQLTotalCount: b.Int32(), SQLTotalTime: b.Int64(), SQLTotalRows: b.Int64(), SQLTotalRuns: b.Int64(), IsStack: b.Bool(), ObjName: b.Text(), ObjType: b.Text(), Position: b.Int64()}
	if b.Err() != nil {
		return nil, b.Err()
	}
	if b.Remaining() != 0 || p.SQLTotalCount < 0 {
		return nil, ErrInvalid
	}
	return p, nil
}
