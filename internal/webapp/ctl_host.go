package webapp

import (
	"net/http"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// HostObjectRequestController (HostObjectRequestController.java:32-79,
// HostObjectRequestConsumer.java). A missing response is result null; a
// response without one of the column lists is 500 (Java NPE).

type processObject struct {
	PID  int32   `json:"pid"`
	User any     `json:"user"`
	CPU  float32 `json:"cpu"`
	Mem  int64   `json:"mem"`
	Time int64   `json:"time"`
	Name any     `json:"name"`
}

type hostDiskData struct {
	Device any     `json:"device"`
	Total  int64   `json:"total"`
	Used   int64   `json:"used"`
	Free   int64   `json:"free"`
	Pct    float64 `json:"pct"`
	Type   any     `json:"type"`
	Mount  any     `json:"mount"`
}

// hostColumns reads one getSingle response as columns; rows==nil means no response.
func (s *Server) hostColumns(r *http.Request, cmd string, keys []string) ([][]protocol.Value, bool, error) {
	hash, err := objHashRequest(r)
	if err != nil {
		return nil, false, err
	}
	m, err := s.single(r.Context(), cmd, objHashParam(hash))
	if err != nil || m == nil {
		return nil, false, err
	}
	lists, err := requireLists(cmd, m, keys...)
	if err != nil {
		return nil, false, err
	}
	rows := make([][]protocol.Value, len(lists[0]))
	for i := range rows {
		rows[i] = make([]protocol.Value, len(keys))
		for k := range keys {
			if rows[i][k], err = listAt(cmd, keys[k], lists[k], i); err != nil {
				return nil, false, err
			}
		}
	}
	return rows, true, nil
}

// GET /v1/object/host/realTime/top/ofObject/{objHash}: HOST_TOP.
func (s *Server) hostTop(w http.ResponseWriter, r *http.Request) {
	rows, ok, err := s.hostColumns(r, "HOST_TOP", []string{"PID", "USER", "CPU", "MEM", "TIME", "NAME"})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if !ok {
		s.writeJSON(w, r, nil)
		return
	}
	out := make([]processObject, 0, len(rows))
	for _, v := range rows {
		out = append(out, processObject{PID: numInt32(v[0]), User: valueString(v[1]), CPU: float32(numFloat64(v[2])),
			Mem: numInt64(v[3]), Time: numInt64(v[4]), Name: valueString(v[5])})
	}
	s.writeJSON(w, r, out)
}

// GET /v1/object/host/realTime/disk/ofObject/{objHash}: HOST_DISK_USAGE
// (pct is ListValue.getLong widened to double, as the original does).
func (s *Server) hostDisk(w http.ResponseWriter, r *http.Request) {
	rows, ok, err := s.hostColumns(r, "HOST_DISK_USAGE", []string{"Device", "Total", "Used", "Free", "Pct", "Type", "Mount"})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if !ok {
		s.writeJSON(w, r, nil)
		return
	}
	out := make([]hostDiskData, 0, len(rows))
	for _, v := range rows {
		out = append(out, hostDiskData{Device: valueString(v[0]), Total: numInt64(v[1]), Used: numInt64(v[2]), Free: numInt64(v[3]),
			Pct: float64(numInt64(v[4])), Type: valueString(v[5]), Mount: valueString(v[6])})
	}
	s.writeJSON(w, r, out)
}
