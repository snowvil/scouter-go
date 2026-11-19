package webapp

import (
	"net/http"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// DictionaryController (/v1/dictionary).

type bDictText struct {
	TextType string `json:"textType"`
	DictKey  int32  `json:"dictKey"`
	Text     string `json:"text"`
}

// bParseDictKeys is DictionaryRequest.setDictSets: ZZ.splitParamStringSet, then
// StringUtils.split(s, ':') -> (type, Integer.valueOf(hash)), grouped by type in
// a HashMap. Type order follows the emulated HashMap; within a type the emulated
// HashSet<String> order of the "type:hash" items is used (Java then regroups them
// into an identity-hash HashSet, which has no stable order).
func bParseDictKeys(raw string, present bool) ([]string, map[string][]int32, error) {
	if !present {
		return nil, nil, bConstraint("dictSets may not be null")
	}
	parts, err := bSplitParam("dictKeys", raw)
	if err != nil {
		return nil, nil, err
	}
	var types []string
	byType := map[string][]int32{}
	seen := map[string]bool{}
	for _, p := range bJavaStringSet(parts) {
		var fields []string
		for _, f := range strings.Split(p, ":") {
			if f != "" {
				fields = append(fields, f)
			}
		}
		if len(fields) < 2 {
			return nil, nil, validateError(`dictKeys item must be "type:hash": "` + p + `"`)
		}
		n, err := bParseJavaLong(fields[1], 32)
		if err != nil {
			return nil, nil, validateError(`dictKeys has an invalid number: "` + fields[1] + `"`)
		}
		if _, ok := byType[fields[0]]; !ok {
			types = append(types, fields[0])
		}
		key := fields[0] + ":" + fields[1]
		if seen[key] {
			continue
		}
		seen[key] = true
		byType[fields[0]] = append(byType[fields[0]], int32(n))
	}
	return bJavaStringSet(types), byType, nil
}

// GET /v1/dictionary/{yyyymmdd}?dictKeys=type:hash,...
func (s *Server) bDictionary(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	raw, present := bQueryRaw(r, "dictKeys")
	types, byType, err := bParseDictKeys(raw, present)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	date := r.PathValue("yyyymmdd")
	out := []bDictText{}
	for _, typ := range types {
		param := bMap(bEntry("date", bTextV(date)), bEntry("type", bTextV(typ)), bEntry("hash", bDecList(byType[typ])))
		err := s.call(r.Context(), "GET_TEXT_PACK", param, func(it Item) error {
			tp, ok := it.Pack.(*protocol.TextPack)
			if !ok {
				return bInternal("unexpected GET_TEXT_PACK response")
			}
			out = append(out, bDictText{TextType: tp.XType, DictKey: tp.Hash, Text: tp.Text})
			return nil
		})
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
	}
	s.bWriteStream(w, r, out)
}
