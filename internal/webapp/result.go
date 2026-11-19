package webapp

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Java scouterx.webapp CommonResultView (view/CommonResultView.java:40-129).
// The webapp's Jackson mapper sets WRITE_NUMBERS_AS_STRINGS, so every JSON
// number is written as a JSON string; booleans and strings stay as they are.

// ErrorState (framework/exception/ErrorState.java:30-46).
const (
	msgInternal       = "internal server error"
	msgNotConnected   = "scouter collector not connected"
	msgLoginRequired  = "login required."
	msgLoginFail      = "id or password is incorrect."
	msgSessionExpired = "authorization token or session is expired."
	msgNotImplemented = "This API is not yet implemented."
	msgValidatePrefix = "fail to validate input parameters. : "
)

// apiError is an ErrorState-mapped failure: HTTP status, resultCode, message.
type apiError struct {
	status, code int
	message      string
	cause        error
}

func (e *apiError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%d %s: %v", e.status, e.message, e.cause)
	}
	return fmt.Sprintf("%d %s", e.status, e.message)
}
func (e *apiError) Unwrap() error { return e.cause }

func newAPIError(status, code int, message string) *apiError {
	return &apiError{status: status, code: code, message: message}
}

// validateError is ErrorState.VALIDATE_ERROR.newBizException(msg).
func validateError(msg string) error {
	return &apiError{status: http.StatusBadRequest, code: http.StatusBadRequest, message: msgValidatePrefix + msg}
}

// notImplemented is ErrorState.NOT_IMPLEMENTED. Go uses it for endpoints whose
// TCP command this server does not provide, instead of an empty success.
func notImplemented(command string) error {
	return &apiError{status: http.StatusNotImplemented, code: http.StatusNotImplemented, message: msgNotImplemented, cause: fmt.Errorf("command %s is not supported by this server", command)}
}

var (
	errLoginRequired  = newAPIError(http.StatusUnauthorized, http.StatusUnauthorized, msgLoginRequired)
	errSessionExpired = newAPIError(http.StatusUnauthorized, http.StatusUnauthorized, msgSessionExpired)
	errLoginFail      = newAPIError(http.StatusNotFound, http.StatusNotFound, msgLoginFail)
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxUserID
)

func requestID(r *http.Request) any {
	if r == nil {
		return nil
	}
	if v, ok := r.Context().Value(ctxRequestID).(string); ok {
		return v
	}
	return nil
}

// writeJSON writes CommonResultView.success(result):
// {"status":"200","requestId":..,"resultCode":"0","message":"success","result":..}.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, result any) {
	s.writeView(w, r, http.StatusOK, 0, "success", result)
}

// writeError writes CommonResultView.fail(status, resultCode, message, null).
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, resultCode int, message string) {
	s.writeView(w, r, status, resultCode, message, nil)
}

// writeErr maps an error the way GenericExceptionMapper does: ErrorState errors
// keep their status/code/message, anything else is 500/500 with its message.
func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var api *apiError
	if errors.As(err, &api) {
		if api.cause != nil {
			s.logf("webapp %s %s: %v", r.Method, r.URL.Path, err)
		}
		s.writeError(w, r, api.status, api.code, api.message)
		return
	}
	s.logf("webapp %s %s: %v", r.Method, r.URL.Path, err)
	s.writeError(w, r, http.StatusInternalServerError, http.StatusInternalServerError, err.Error())
}

func (s *Server) writeView(w http.ResponseWriter, r *http.Request, status, code int, message string, result any) {
	view := jsonObject{
		{"status", status},
		{"requestId", requestID(r)},
		{"resultCode", code},
		{"message", message},
		{"result", result},
	}
	body, err := appendJSON(make([]byte, 0, 256), view)
	if err != nil {
		s.logf("webapp %s %s: encode: %v", r.Method, r.URL.Path, err)
		status = http.StatusInternalServerError
		body, _ = appendJSON(nil, jsonObject{{"status", status}, {"requestId", requestID(r)}, {"resultCode", status}, {"message", err.Error()}, {"result", nil}})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// jsonObject is an ordered JSON object (Java LinkedHashMap / bean field order).
type jsonObject []jsonField
type jsonField struct {
	Key   string
	Value any
}

// appendJSON encodes v with the webapp mapper's rules: integers and floats as
// JSON strings (floats in Java Float/Double.toString form), structs in field
// declaration order by `json` tag ("-" skips), maps with sorted keys, nil
// slices/pointers/interfaces as null. omitempty is not supported.
func appendJSON(dst []byte, v any) ([]byte, error) {
	return appendValue(dst, reflect.ValueOf(v), 0)
}

const maxJSONDepth = 64

var jsonObjectType = reflect.TypeOf(jsonObject(nil))

func appendValue(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth > maxJSONDepth {
		return dst, errors.New("json: nesting too deep")
	}
	if !v.IsValid() {
		return append(dst, "null"...), nil
	}
	if v.Type() == jsonObjectType {
		if v.IsNil() {
			return append(dst, "null"...), nil
		}
		obj := v.Interface().(jsonObject)
		dst = append(dst, '{')
		for i, f := range obj {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendString(dst, f.Key)
			dst = append(dst, ':')
			var err error
			if dst, err = appendValue(dst, reflect.ValueOf(f.Value), depth+1); err != nil {
				return dst, err
			}
		}
		return append(dst, '}'), nil
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return append(dst, "null"...), nil
		}
		return appendValue(dst, v.Elem(), depth+1)
	case reflect.Bool:
		return strconv.AppendBool(dst, v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst = append(dst, '"')
		dst = strconv.AppendInt(dst, v.Int(), 10)
		return append(dst, '"'), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst = append(dst, '"')
		dst = strconv.AppendUint(dst, v.Uint(), 10)
		return append(dst, '"'), nil
	case reflect.Float32:
		return appendString(dst, javaFloatString(float32(v.Float()))), nil
	case reflect.Float64:
		return appendString(dst, javaDoubleString(v.Float())), nil
	case reflect.String:
		return appendString(dst, v.String()), nil
	case reflect.Slice:
		if v.IsNil() {
			return append(dst, "null"...), nil
		}
		fallthrough
	case reflect.Array:
		dst = append(dst, '[')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			if dst, err = appendValue(dst, v.Index(i), depth+1); err != nil {
				return dst, err
			}
		}
		return append(dst, ']'), nil
	case reflect.Map:
		if v.IsNil() {
			return append(dst, "null"...), nil
		}
		if v.Type().Key().Kind() != reflect.String {
			return dst, fmt.Errorf("json: unsupported map key %s", v.Type().Key())
		}
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendString(dst, k.String())
			dst = append(dst, ':')
			var err error
			if dst, err = appendValue(dst, v.MapIndex(k), depth+1); err != nil {
				return dst, err
			}
		}
		return append(dst, '}'), nil
	case reflect.Struct:
		t := v.Type()
		dst = append(dst, '{')
		first := true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag, ok := f.Tag.Lookup("json"); ok {
				tag, _, _ = strings.Cut(tag, ",")
				if tag == "-" {
					continue
				}
				if tag != "" {
					name = tag
				}
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendString(dst, name)
			dst = append(dst, ':')
			var err error
			if dst, err = appendValue(dst, v.Field(i), depth+1); err != nil {
				return dst, err
			}
		}
		return append(dst, '}'), nil
	}
	return dst, fmt.Errorf("json: unsupported type %s", v.Type())
}

const hexDigits = "0123456789abcdef"

// appendString escapes like Jackson: quote, backslash and C0 controls; other
// characters (including '/', '<', non-ASCII) are written as UTF-8.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				dst = append(dst, '\\', c)
			case c == '\n':
				dst = append(dst, '\\', 'n')
			case c == '\r':
				dst = append(dst, '\\', 'r')
			case c == '\t':
				dst = append(dst, '\\', 't')
			case c == '\b':
				dst = append(dst, '\\', 'b')
			case c == '\f':
				dst = append(dst, '\\', 'f')
			case c < 0x20:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			default:
				dst = append(dst, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, "�"...)
		} else {
			dst = append(dst, s[i:i+size]...)
		}
		i += size
	}
	return append(dst, '"')
}

// javaDoubleString is Java Double.toString (shortest round-trip digits):
// plain notation for 1e-3 <= |d| < 1e7 with at least one fraction digit,
// otherwise d.dddE<exp>.
func javaDoubleString(d float64) string { return javaFloating(d, 64) }

// javaFloatString is Java Float.toString.
func javaFloatString(f float32) string { return javaFloating(float64(f), 32) }

func javaFloating(d float64, bits int) string {
	switch {
	case math.IsNaN(d):
		return "NaN"
	case math.IsInf(d, 1):
		return "Infinity"
	case math.IsInf(d, -1):
		return "-Infinity"
	case d == 0:
		if math.Signbit(d) {
			return "-0.0"
		}
		return "0.0"
	}
	abs := math.Abs(d)
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(d, 'f', -1, bits)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	s := strconv.FormatFloat(d, 'e', -1, bits)
	mant, exp, _ := strings.Cut(s, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	n, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(n)
}
