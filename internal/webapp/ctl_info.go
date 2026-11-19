package webapp

import (
	"net/http"
)

// InfoController (layer/controller/InfoController.java:42-79).

type serverView struct {
	ID         int32 `json:"id"`
	Name       any   `json:"name"`
	Connected  bool  `json:"connected"`
	ServerTime int64 `json:"serverTime"`
	Version    any   `json:"version"`
}

// GET /v1/info/server (@NoAuth). The single collector logs in on demand; a
// failed login is reported as connected=false, not as an error.
func (s *Server) infoServer(w http.ResponseWriter, r *http.Request) {
	if _, err := s.session(r.Context()); err != nil {
		s.logf("webapp login: %v", err)
	}
	s.login.mu.Lock()
	view := serverView{ID: s.id, Connected: s.login.session != 0, ServerTime: s.now().UnixMilli() + s.login.delta}
	if s.login.at.IsZero() {
		view.Name, view.Version = s.cfg.CollectorAddr, nil
	} else {
		view.Name, view.Version = s.login.name, s.login.version
		if s.login.name == "" {
			view.Name = s.cfg.CollectorAddr
		}
	}
	s.login.mu.Unlock()
	s.writeJSON(w, r, []serverView{view})
}

type counterData struct {
	Name        any  `json:"name"`
	DisplayName any  `json:"displayName"`
	Unit        any  `json:"unit"`
	Totalizable bool `json:"totalizable"`
	Icon        any  `json:"icon"`
	Master      bool `json:"master"`
}

type familyData struct {
	Name     any           `json:"name"`
	Counters []counterData `json:"counters"`
}

type objectTypeData struct {
	Name        any  `json:"name"`
	FamilyName  any  `json:"familyName"`
	DisplayName any  `json:"displayName"`
	Icon        any  `json:"icon"`
	SubObject   bool `json:"subObject"`
}

type counterModelData struct {
	Families []familyData     `json:"families"`
	ObjTypes []objectTypeData `json:"objTypes"`
}

// GET /v1/info/counter-model: CounterModelData.of(counterEngine).
func (s *Server) infoCounterModel(w http.ResponseWriter, r *http.Request) {
	if err := serverIDParam(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	e, err := s.counterEngine(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, buildCounterModel(e))
}

func buildCounterModel(e *counterEngine) counterModelData {
	out := counterModelData{Families: []familyData{}, ObjTypes: []objectTypeData{}}
	for _, name := range e.familyOrder {
		f := e.families[name]
		fd := familyData{Name: strPtr(f.name), Counters: []counterData{}}
		for _, c := range f.counters {
			fd.Counters = append(fd.Counters, counterData{
				Name: strPtr(c.name), DisplayName: strPtr(c.display), Unit: strPtr(c.unit),
				Totalizable: c.total, Icon: strPtr(c.icon),
				Master: c.name != nil && f.master != nil && *c.name == *f.master,
			})
		}
		out.Families = append(out.Families, fd)
	}
	for _, name := range e.typeOrder {
		t := e.types[name]
		td := objectTypeData{Name: strPtr(t.name), DisplayName: strPtr(t.display), Icon: strPtr(t.icon), SubObject: t.subObject}
		// Java: objType.getFamily().getName() throws NPE for an undefined family.
		if t.family != nil {
			td.FamilyName = strPtr(t.family.name)
		}
		out.ObjTypes = append(out.ObjTypes, td)
	}
	return out
}
