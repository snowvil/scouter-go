package httpapi

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/telegraf"
)

var errNoRegistrar = errors.New("httpapi: no counter registrar configured")

// serveRegister is RegisterServlet/RegisterHandler: 201 when the counter
// catalog accepted the change, 500 otherwise (bad body, missing registrar,
// registrar error). Both answers have an empty body.
func (a *API) serveRegister(w http.ResponseWriter, r *http.Request) {
	body, ok := a.readBody(w, r)
	if !ok {
		return
	}
	if err := a.register(body); err != nil {
		a.registerFailed.Add(1)
		a.cfg.Logf("SC-8001 Http body parsing error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	a.registerCreated.Add(1)
	w.WriteHeader(http.StatusCreated)
}

func (a *API) register(body string) error {
	v, err := parseJSON(body)
	if err != nil {
		return err
	}
	root, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("body is %T, not an object", v)
	}
	object, ok := root["object"].(map[string]any)
	if !ok {
		return errors.New("object is missing or not an object")
	}
	counters, ok := root["counters"].([]any)
	if !ok {
		return errors.New("counters is missing or not an array")
	}
	objType, err := optString(object, "type")
	if err != nil {
		return err
	}
	if objType == nil {
		return errors.New("object.type is missing")
	}
	display, hasDisplay, err := displayOf(object)
	if err != nil {
		return err
	}
	reg := a.cfg.Registrar
	if reg == nil {
		return errNoRegistrar
	}
	existing, exists := reg.ObjectType(*objType)
	if !exists {
		family := telegraf.FamilyDef{Name: *objType}
		if reg.HasFamily(*objType) {
			family.Name = *objType + "." + javaDouble(rand.Float64()*100)
		}
		objectType := telegraf.ObjectTypeDef{Name: *objType, DisplayName: *objType, Family: family.Name}
		if hasDisplay {
			objectType.DisplayName = display
		}
		for i, rc := range counters {
			c, err := registerCounter(rc)
			if err != nil {
				return err
			}
			family.Counters = upsert(family.Counters, c)
			if i == 0 {
				family.Master = c.Name
			}
		}
		if err := reg.AddFamilyAndObjectType(family, objectType); err != nil {
			return err
		}
		reg.NotifyClients()
		return nil
	}
	edit := telegraf.ObjectTypeDef{Name: *objType, Family: existing.Family, Icon: existing.Icon, SubObject: existing.SubObject, DisplayName: existing.DisplayName}
	if hasDisplay {
		edit.DisplayName = display
	}
	for _, rc := range counters {
		c, err := registerCounter(rc)
		if err != nil {
			return err
		}
		if _, found := existing.Counter(c.Name); found {
			continue
		}
		edit.Counters = upsert(edit.Counters, c)
	}
	if err := reg.EditObjectType(edit); err != nil {
		return err
	}
	reg.NotifyClients()
	return nil
}

// displayOf is containsKey("display") + (String) get: a present null display
// is the empty string here (Java stores null).
func displayOf(m map[string]any) (string, bool, error) {
	if _, ok := m["display"]; !ok {
		return "", false, nil
	}
	s, err := optString(m, "display")
	if err != nil {
		return "", false, err
	}
	if s == nil {
		return "", true, nil
	}
	return *s, true, nil
}

// registerCounter builds a Counter{name, unit, display||name, icon "",
// total, all}; total/all default true and must be JSON booleans when present.
func registerCounter(raw any) (telegraf.CounterDef, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return telegraf.CounterDef{}, fmt.Errorf("counter is %T, not an object", raw)
	}
	name, err := optString(m, "name")
	if err != nil {
		return telegraf.CounterDef{}, err
	}
	if name == nil {
		return telegraf.CounterDef{}, errors.New("counter name is missing")
	}
	unit, err := optString(m, "unit")
	if err != nil {
		return telegraf.CounterDef{}, err
	}
	c := telegraf.CounterDef{Name: *name, DisplayName: *name, Total: true, All: true}
	if unit != nil {
		c.Unit = *unit
	}
	if display, has, err := displayOf(m); err != nil {
		return telegraf.CounterDef{}, err
	} else if has {
		c.DisplayName = display
	}
	for _, f := range []struct {
		key string
		dst *bool
	}{{"total", &c.Total}, {"all", &c.All}} {
		if v, present := m[f.key]; present {
			b, ok := v.(bool)
			if !ok {
				return telegraf.CounterDef{}, fmt.Errorf("counter %q %s is %T, not a boolean", *name, f.key, v)
			}
			*f.dst = b
		}
	}
	return c, nil
}

func upsert(list []telegraf.CounterDef, c telegraf.CounterDef) []telegraf.CounterDef {
	for i := range list {
		if list[i].Name == c.Name {
			list[i] = c
			return list
		}
	}
	return append(list, c)
}

// javaDouble approximates Double.toString for the Math.random()*100 family
// suffix: plain notation with at least one fraction digit in [1e-3, 1e7),
// otherwise d.dddE±n.
func javaDouble(f float64) string {
	if f == 0 {
		return "0.0"
	}
	if f >= 1e-3 && f < 1e7 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	s := strconv.FormatFloat(f, 'E', -1, 64)
	mant, exp, _ := strings.Cut(s, "E")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	exp = strings.TrimPrefix(exp, "+")
	n, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(n)
}
