package server

import (
	"encoding/xml"
	"github.com/snowvil/scouter-go/internal/service"
)

// The unchanged Scouter counters.xml supplies default object's master counters.
func masterCounters(data []byte) (map[string]string, error) {
	var doc struct {
		Families []struct {
			Name   string `xml:"name,attr"`
			Master string `xml:"master,attr"`
		} `xml:"Familys>Family"`
		Types []struct {
			Name   string `xml:"name,attr"`
			Family string `xml:"family,attr"`
		} `xml:"Types>ObjectType"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	families := map[string]string{}
	for _, f := range doc.Families {
		families[f.Name] = f.Master
	}
	out := map[string]string{}
	for _, obj := range doc.Types {
		out[obj.Name] = families[obj.Family]
	}
	return out, nil
}

// counterTypes combines family and object counters as ObjectType.listCounters.
func counterTypes(data []byte) (map[string]service.CounterTypeMetadata, error) {
	var doc struct {
		Families []struct {
			Name     string `xml:"name,attr"`
			Counters []struct {
				Name string `xml:"name,attr"`
			} `xml:"Counter"`
		} `xml:"Familys>Family"`
		Types []struct {
			Name     string `xml:"name,attr"`
			Display  string `xml:"disp,attr"`
			Family   string `xml:"family,attr"`
			Counters []struct {
				Name string `xml:"name,attr"`
			} `xml:"Counter"`
		} `xml:"Types>ObjectType"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	families := map[string][]string{}
	for _, f := range doc.Families {
		for _, c := range f.Counters {
			families[f.Name] = append(families[f.Name], c.Name)
		}
	}
	out := map[string]service.CounterTypeMetadata{}
	for _, t := range doc.Types {
		meta := service.CounterTypeMetadata{DisplayName: t.Display, Counters: append([]string(nil), families[t.Family]...)}
		for _, c := range t.Counters {
			meta.Counters = append(meta.Counters, c.Name)
		}
		out[t.Name] = meta
	}
	return out, nil
}
