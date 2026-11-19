package account

import (
	"bytes"
	"testing"
)

// FuzzAccountXML: any accepted file serializes to bytes that parse again,
// carry the same accounts/groups, and serialize identically from then on.
func FuzzAccountXML(f *testing.F) {
	f.Add(defaultAccountXML)
	f.Add(defaultGroupXML)
	f.Add([]byte("<?xml version=\"1.0\" standalone=\"yes\"?><!--c--><Accounts a=\"&#9;&quot;\">x&amp;<?p d?><Account id=\"\U0001F600\"/></Accounts>"))
	f.Fuzz(func(t *testing.T, data []byte) {
		doc, err := parseXML(data)
		if err != nil {
			return
		}
		groups, groupErr := parseGroups(doc, func(string, ...any) {})
		once := doc.marshal()
		again, err := parseXML(once)
		if err != nil {
			// Text Go keeps but Xerces would reject (e.g. a control
			// character from a character reference) cannot round-trip.
			if hasInvalidText(doc.root) {
				return
			}
			t.Fatalf("marshal output does not parse: %v\n%q", err, once)
		}
		a1, a2 := parseAccounts(doc), parseAccounts(again)
		if len(a1) != len(a2) {
			t.Fatal("accounts changed")
		}
		for i := range a1 {
			if a1[i] != a2[i] {
				t.Fatalf("account %d: %+v != %+v", i, a1[i], a2[i])
			}
		}
		g2, err2 := parseGroups(again, func(string, ...any) {})
		if (groupErr == nil) != (err2 == nil) || len(groups) != len(g2) {
			t.Fatal("groups changed")
		}
		twice := again.marshal()
		third, err := parseXML(twice)
		if err != nil || !bytes.Equal(third.marshal(), twice) {
			t.Fatalf("not stable after one round trip\n%q\n%q", once, twice)
		}
	})
}

func hasInvalidText(n *xmlNode) bool {
	if !validXMLText(n.text) || !validXMLText(n.name) {
		return true
	}
	for _, a := range n.attrs {
		if !validXMLText(a.Value) {
			return true
		}
	}
	for _, c := range n.children {
		if hasInvalidText(c) {
			return true
		}
	}
	return false
}
