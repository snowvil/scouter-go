package server

import "testing"

func TestCounterTypesIncludeFamilyAndObjectNames(t *testing.T) {
	xml := []byte(`<Counters><Familys><Family name="base"><Counter name="cpu"/></Family></Familys><Types><ObjectType name="java" disp="Java" family="base"><Counter name="TPS"/></ObjectType></Types></Counters>`)
	catalog, err := counterTypes(xml)
	if err != nil || catalog["java"].DisplayName != "Java" || len(catalog["java"].Counters) != 2 || catalog["java"].Counters[0] != "cpu" || catalog["java"].Counters[1] != "TPS" {
		t.Fatal(catalog, err)
	}
	if _, err = counterTypes([]byte(`<bad`)); err == nil {
		t.Fatal("invalid XML accepted")
	}
}
