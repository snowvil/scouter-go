package webapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// GlobalKvStoreController (/v1/kv), CustomKvStoreController (/v1/kv/space),
// PrivateKvStoreController (/v1/kv-private), ShortenController (/v1/shortener)
// and SimpleViewController (/s/{key}).

const (
	bPrivateKeySpace   = "___WEB_SESSION_PRIVATE___"
	bShortenerKeySpace = "shortner" // ScouterConstants.SHORTENER_KEY_SPACE (sic)
)

// bReservedKeySpace: the custom store (/v1/kv/space/{keySpace}) must not reach
// the bearer-token store (__WEB_SESSION__, auth.go webSessionSpace) or another
// user's private store. The original webapp does not check this (unverified);
// Go answers 400 VALIDATE_ERROR (intended difference). The match is
// case-insensitive: the storage file name kv_<space> is reused as is, so a
// case-insensitive file system would alias __web_session__.
func bReservedKeySpace(keySpace string) error {
	if strings.EqualFold(keySpace, webSessionSpace) || strings.EqualFold(keySpace, bPrivateKeySpace) {
		return validateError("keySpace is reserved: " + keySpace)
	}
	return nil
}

type bKeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// ---- request bodies (Jackson coercion subset) ----

// bReadBody reads a JSON object body bounded by Config.MaxBodyBytes (A's
// readBody limit); larger bodies are 400 VALIDATE_ERROR.
func (s *Server) bReadBody(r *http.Request) (map[string]json.RawMessage, error) {
	limit := s.cfg.MaxBodyBytes
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, validateError("request body cannot be read")
	}
	if int64(len(data)) > limit {
		return nil, validateError("request body is too large")
	}
	if len(bytes.TrimSpace(data)) == 0 || string(bytes.TrimSpace(data)) == "null" {
		// Java: null entity -> NullPointerException 500 (intended difference: 400).
		return nil, validateError("request body is required")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, validateError("request body is not a JSON object")
	}
	return m, nil
}

// bJSONString coerces a JSON scalar to Java String the way Jackson does for a
// String property (numbers/booleans by their text); null -> nil.
func bJSONString(raw json.RawMessage, name string) (*string, error) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || string(t) == "null" {
		return nil, nil
	}
	switch t[0] {
	case '"':
		var s string
		if err := json.Unmarshal(t, &s); err != nil {
			return nil, validateError(name + " is invalid")
		}
		return &s, nil
	case '{', '[':
		return nil, validateError(name + " must be a scalar")
	}
	s := string(t)
	return &s, nil
}

// bJSONObjectString is Object.toString() of a Jackson-deserialized scalar
// (KeyValueData.value): String, Integer/Long, Double, Boolean.
func bJSONObjectString(raw json.RawMessage, name string) (string, error) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || string(t) == "null" {
		return "", validateError(name + " is required")
	}
	switch t[0] {
	case '"':
		var s string
		if err := json.Unmarshal(t, &s); err != nil {
			return "", validateError(name + " is invalid")
		}
		return s, nil
	case '{', '[':
		return "", validateError(name + " must be a scalar")
	case 't', 'f':
		return string(t), nil
	}
	if n, err := strconv.ParseInt(string(t), 10, 64); err == nil {
		return strconv.FormatInt(n, 10), nil
	}
	f, err := strconv.ParseFloat(string(t), 64)
	if err != nil {
		return "", validateError(name + " is invalid")
	}
	return bJavaDouble(f), nil
}

// bJSONLong reads a primitive long property: absent -> def, null -> 0, number or numeric string.
func bJSONLong(m map[string]json.RawMessage, key string, def int64) (int64, error) {
	raw, ok := m[key]
	if !ok {
		return def, nil
	}
	t := bytes.TrimSpace(raw)
	if string(t) == "null" {
		return 0, nil
	}
	if len(t) > 0 && t[0] == '"' {
		var s string
		if json.Unmarshal(t, &s) != nil {
			return 0, validateError(key + " is invalid")
		}
		t = []byte(strings.TrimSpace(s))
	}
	n, err := strconv.ParseInt(string(t), 10, 64)
	if err != nil {
		return 0, validateError(key + " is invalid")
	}
	return n, nil
}

type bSetKv struct {
	key, value *string
	ttl        int64
}

func (s *Server) bParseSetKv(r *http.Request) (bSetKv, error) {
	var q bSetKv
	m, err := s.bReadBody(r)
	if err != nil {
		return q, err
	}
	if _, err = bJSONLong(m, "serverId", 0); err != nil {
		return q, err
	}
	if q.key, err = bJSONString(m["key"], "key"); err != nil {
		return q, err
	}
	if q.value, err = bJSONString(m["value"], "value"); err != nil {
		return q, err
	}
	q.ttl, err = bJSONLong(m, "ttl", bTTLPermanent)
	return q, err
}

func (s *Server) bParseTTL(r *http.Request) (int64, error) {
	m, err := s.bReadBody(r)
	if err != nil {
		return 0, err
	}
	if _, err = bJSONLong(m, "serverId", 0); err != nil {
		return 0, err
	}
	return bJSONLong(m, "ttl", bTTLPermanent)
}

// bParseBulk is SetKvBulkRequest: kvList -> HashMap<String,String> (last
// duplicate wins, HashMap order emulated), ttl default -1.
func (s *Server) bParseBulk(r *http.Request, prefix string) ([]protocol.Entry, int64, error) {
	m, err := s.bReadBody(r)
	if err != nil {
		return nil, 0, err
	}
	if _, err = bJSONLong(m, "serverId", 0); err != nil {
		return nil, 0, err
	}
	raw, ok := m["kvList"]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		return nil, 0, validateError("kvList is required")
	}
	var list []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, 0, validateError("kvList must be an array of {key,value}")
	}
	values := map[string]string{}
	var keys []string
	for _, kv := range list {
		k, err := bJSONString(kv["key"], "key")
		if err != nil {
			return nil, 0, err
		}
		key := prefix + "null" // Java string concatenation of a null key
		if k != nil {
			key = prefix + *k
		} else if prefix == "" {
			return nil, 0, validateError("key is required")
		}
		v, err := bJSONObjectString(kv["value"], "value")
		if err != nil {
			return nil, 0, err
		}
		if _, dup := values[key]; !dup {
			keys = append(keys, key)
		}
		values[key] = v
	}
	ttl, err := bJSONLong(m, "ttl", bTTLPermanent)
	if err != nil {
		return nil, 0, err
	}
	keys = bJavaStringSet(keys)
	entries := make([]protocol.Entry, len(keys))
	for i, k := range keys {
		entries[i] = bEntry(k, bTextV(values[k]))
	}
	return entries, ttl, nil
}

// ---- collector helpers ----

func (s *Server) bKvGet(ctx context.Context, keySpace, key string, global bool) (*string, error) {
	var v *protocol.Value
	var err error
	if global {
		err = s.callValue(ctx, "GET_GLOBAL_KV", bTextV(key), func(it Item) error {
			if v == nil && it.Value != nil {
				v = it.Value
			}
			return nil
		})
	} else {
		v, err = s.bSingleValue(ctx, "GET_CUSTOM_KV", bMap(bEntry("keySpace", bTextV(keySpace)), bEntry("key", bTextV(key))))
	}
	if err != nil || v == nil {
		return nil, err
	}
	return bValueString(*v), nil
}

// bKvSetResult is the consumer's (Boolean) cast plus the service's
// "Error on setting value to kvstore!" RuntimeException.
func (s *Server) bKvSetResult(ctx context.Context, cmd string, param *protocol.MapPack) error {
	v, err := s.bSingleValue(ctx, cmd, param)
	if err != nil {
		return err
	}
	if v != nil && v.Type != protocol.ValueBoolean {
		return bInternal("unexpected " + cmd + " response")
	}
	if v == nil || !v.Bool {
		return bInternal("Error on setting value to kvstore!")
	}
	return nil
}

func (s *Server) bKvSet(ctx context.Context, keySpace string, global bool, key, value *string, ttl int64) error {
	if global {
		return s.bKvSetResult(ctx, "SET_GLOBAL_KV", bMap(bEntry("key", bOptText(key)), bEntry("value", bOptText(value)), bEntry("ttl", bDec(ttl))))
	}
	return s.bKvSetResult(ctx, "SET_CUSTOM_KV", bMap(bEntry("keySpace", bTextV(keySpace)), bEntry("key", bOptText(key)),
		bEntry("value", bOptText(value)), bEntry("ttl", bDec(ttl))))
}

func (s *Server) bKvTTL(ctx context.Context, keySpace string, global bool, key string, ttl int64) error {
	if global {
		return s.bKvSetResult(ctx, "SET_GLOBAL_TTL", bMap(bEntry("key", bTextV(key)), bEntry("ttl", bDec(ttl))))
	}
	return s.bKvSetResult(ctx, "SET_CUSTOM_TTL", bMap(bEntry("keySpace", bTextV(keySpace)), bEntry("key", bTextV(key)), bEntry("ttl", bDec(ttl))))
}

// bKvBulkGet returns KeyValueData{key, getText(key)} in result MapPack order.
func (s *Server) bKvBulkGet(ctx context.Context, keySpace string, global bool, keys []string) ([]bKeyValue, error) {
	cmd := "GET_GLOBAL_KV_BULK"
	param := bMap(bEntry("key", bTextList(keys)))
	if !global {
		cmd = "GET_CUSTOM_KV_BULK"
		param = bMap(bEntry("keySpace", bTextV(keySpace)), bEntry("key", bTextList(keys)))
	}
	return s.bKvBulkResult(ctx, cmd, param, false)
}

func (s *Server) bKvBulkSet(ctx context.Context, keySpace string, global bool, kv []protocol.Entry, ttl int64) ([]bKeyValue, error) {
	mv := protocol.Value{Type: protocol.ValueMap, Entries: kv}
	cmd := "SET_GLOBAL_KV_BULK"
	param := bMap(bEntry("kv", mv), bEntry("ttl", bDec(ttl)))
	if !global {
		cmd = "SET_CUSTOM_KV_BULK"
		param = bMap(bEntry("keySpace", bTextV(keySpace)), bEntry("kv", mv), bEntry("ttl", bDec(ttl)))
	}
	return s.bKvBulkResult(ctx, cmd, param, true)
}

// bKvBulkResult reads the result MapPack. A missing pack (Java NPE) is treated
// as an empty result: the Go collector sends nothing for an empty key list.
func (s *Server) bKvBulkResult(ctx context.Context, cmd string, param *protocol.MapPack, boolean bool) ([]bKeyValue, error) {
	pack, err := s.bSingle(ctx, cmd, param)
	if err != nil {
		return nil, err
	}
	out := []bKeyValue{}
	mp, ok := pack.(*protocol.MapPack)
	if pack != nil && !ok {
		return nil, bInternal("unexpected " + cmd + " response")
	}
	if mp == nil {
		return out, nil
	}
	for _, e := range mp.Entries {
		if boolean {
			out = append(out, bKeyValue{Key: e.Key, Value: e.Value.Type == protocol.ValueBoolean && e.Value.Bool})
		} else {
			out = append(out, bKeyValue{Key: e.Key, Value: bMapText(mp, e.Key)})
		}
	}
	return out, nil
}

// ---- generic handlers parameterised by store ----

// bKvStore selects the key space and key mapping of one controller.
type bKvStore struct {
	global  bool
	private bool
	// space returns the key space (custom: path {keySpace}).
	space func(r *http.Request) string
}

// bPrefix is PrivateKvStoreController.getPrivateKeyPrefix. Java dereferences a
// null token (NPE 500, W5); Go answers LOGIN_REQUIRED 401 (intended difference).
func (s *Server) bPrefix(st bKvStore, r *http.Request) (string, error) {
	if st.space != nil {
		if err := bReservedKeySpace(st.space(r)); err != nil {
			return "", err
		}
	}
	if !st.private {
		return "", nil
	}
	user, ok := s.requestUserID(r)
	if !ok {
		return "", newAPIError(http.StatusUnauthorized, http.StatusUnauthorized, "login required.")
	}
	return "]" + user + ":", nil
}

func (st bKvStore) keySpace(r *http.Request) string {
	if st.private {
		return bPrivateKeySpace
	}
	if st.space != nil {
		return st.space(r)
	}
	return ""
}

func (s *Server) bKvGetHandler(st bKvStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := bServerID(r); err != nil {
			s.writeErr(w, r, err)
			return
		}
		prefix, err := s.bPrefix(st, r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		v, err := s.bKvGet(r.Context(), st.keySpace(r), prefix+r.PathValue("key"), st.global)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.writeJSON(w, r, v)
	}
}

func (s *Server) bKvSetHandler(st bKvStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := s.bParseSetKv(r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		prefix, err := s.bPrefix(st, r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		key := q.key
		if st.private {
			k := prefix + "null"
			if key != nil {
				k = prefix + *key
			}
			key = &k
		}
		if err := s.bKvSet(r.Context(), st.keySpace(r), st.global, key, q.value, q.ttl); err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.writeJSON(w, r, true)
	}
}

func (s *Server) bKvTTLHandler(st bKvStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ttl, err := s.bParseTTL(r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		prefix, err := s.bPrefix(st, r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		if err := s.bKvTTL(r.Context(), st.keySpace(r), st.global, prefix+r.PathValue("key"), ttl); err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.writeJSON(w, r, true)
	}
}

func (s *Server) bKvBulkGetHandler(st bKvStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := bServerID(r); err != nil {
			s.writeErr(w, r, err)
			return
		}
		keys, err := bSplitParam("keys", r.PathValue("keys"))
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		prefix, err := s.bPrefix(st, r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		for i := range keys {
			keys[i] = prefix + keys[i]
		}
		out, err := s.bKvBulkGet(r.Context(), st.keySpace(r), st.global, keys)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.writeJSON(w, r, bGeneralKeys(st, prefix, out))
	}
}

func (s *Server) bKvBulkSetHandler(st bKvStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		prefix, err := s.bPrefix(st, r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		kv, ttl, err := s.bParseBulk(r, prefix)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		out, err := s.bKvBulkSet(r.Context(), st.keySpace(r), st.global, kv, ttl)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		s.writeJSON(w, r, bGeneralKeys(st, prefix, out))
	}
}

// bGeneralKeys is PrivateKvStoreController.toGeneralKey: StringUtils.substring
// by the prefix length. The original never throws ILLEGAL_KEY_ACCESS (W5), so a
// key without the prefix is cut the same way.
func bGeneralKeys(st bKvStore, prefix string, list []bKeyValue) []bKeyValue {
	if !st.private {
		return list
	}
	n := bUTF16Len(prefix)
	for i := range list {
		list[i].Key = bUTF16Substring(list[i].Key, n)
	}
	return list
}

// bGlobalReserved: Jersey selects CustomKvStoreController for any path under
// /v1/kv/space, so a global key named "space" is unreachable (404).
func (s *Server) bGlobalReserved(h http.HandlerFunc, name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue(name) == "space" {
			s.writeErr(w, r, bNotFound())
			return
		}
		h(w, r)
	}
}

// bKvPutTwo dispatches PUT /v1/kv/{a}/{b}: "space/{keySpace}" belongs to the
// custom store (class precedence), "{key}/:ttl" to the global TTL.
func (s *Server) bKvPutTwo(custom, globalTTL http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.PathValue("a") == "space":
			r.SetPathValue("keySpace", r.PathValue("b"))
			custom(w, r)
		case r.PathValue("b") == ":ttl":
			r.SetPathValue("key", r.PathValue("a"))
			globalTTL(w, r)
		default:
			s.writeErr(w, r, bNotFound())
		}
	}
}

// ---- shortener / simple view ----

// GET /v1/shortener/{key}
func (s *Server) bShortenGet(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	v, err := s.bKvGet(r.Context(), bShortenerKeySpace, r.PathValue("key"), false)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, v)
}

// POST /v1/shortener?url=
func (s *Server) bShortenMake(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	target, ok := bQueryRaw(r, "url")
	if !ok {
		// Java: HashUtil.hash(null) -> NPE 500 (intended difference: 400).
		s.writeErr(w, r, validateError("url is required"))
		return
	}
	hashed := bHexa32(int64(bJavaHash(target)))
	if err := s.bKvSet(r.Context(), bShortenerKeySpace, false, &hashed, &target, bTTLPermanent); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, bServiceBase(r)+"/s/"+hashed)
}

// bServiceBase is requestURL.substring(0, requestURL.indexOf(pathInfo)) with
// the Jersey servlet mapped at /scouter/*, i.e. "<scheme>://<host>/scouter".
func bServiceBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	full := scheme + "://" + r.Host + r.URL.EscapedPath()
	if i := strings.Index(full, "/v1/shortener"); i >= 0 {
		return full[:i]
	}
	return scheme + "://" + r.Host + "/scouter"
}

// GET /s/{key} (@NoAuth): 303 See Other to the stored URI, else 404 empty body.
func (s *Server) bSimpleView(w http.ResponseWriter, r *http.Request) {
	if err := bServerID(r); err != nil {
		s.writeErr(w, r, err)
		return
	}
	v, err := s.bKvGet(r.Context(), bShortenerKeySpace, r.PathValue("key"), false)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if v != nil && strings.TrimSpace(*v) != "" {
		if loc, ok := bJavaURI(*v); ok {
			if !loc.IsAbs() {
				// JAX-RS resolves a relative seeOther URI against the application base URI.
				base, _ := url.Parse(bServiceBase(r) + "/")
				loc = base.ResolveReference(loc)
			}
			w.Header().Set("Location", loc.String())
			w.WriteHeader(http.StatusSeeOther)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

// bJavaURI approximates new java.net.URI(s): characters illegal in RFC 2396
// URIs are rejected (Go's url.Parse alone is more permissive).
func bJavaURI(s string) (*url.URL, bool) {
	for _, c := range s {
		if c <= ' ' || c == 0x7f || strings.ContainsRune("\"<>\\^`{|}", c) {
			return nil, false
		}
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, false
	}
	return u, true
}
