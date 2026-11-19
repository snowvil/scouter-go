package account

import (
	"fmt"
	"strings"
)

// Element and attribute names of AccountFileHandler/GroupFileHandler.
const (
	tagAccounts = "Accounts"
	tagAccount  = "Account"
	tagEmail    = "Email"
	attrID      = "id"
	attrPass    = "pass"
	attrGroup   = "group"
	tagGroups   = "Groups"
	tagGroup    = "Group"
	tagPolicy   = "Policy"
	attrName    = "name"
)

// parseAccounts is AccountFileHandler.parse: every <Account> in document
// order; a missing <Email> is Java null, which Go keeps as "" (Account.toBytes
// would throw NullPointerException on it). StringKeyLinkedMap.put keeps the
// first position of a repeated id and the last value.
func parseAccounts(doc *xmlDocument) []Account {
	var out []Account
	index := map[string]int{}
	for _, e := range doc.elements(tagAccount) {
		a := Account{ID: e.attr(attrID), Password: e.attr(attrPass), Group: e.attr(attrGroup)}
		if emails := e.elements(tagEmail, nil); len(emails) > 0 {
			a.Email = emails[0].textContent()
		}
		if i, ok := index[a.ID]; ok {
			out[i] = a
			continue
		}
		index[a.ID] = len(out)
		out = append(out, a)
	}
	return out
}

// parseGroups is GroupFileHandler.parse. A <Group> without <Policy> makes the
// Java parse throw (NullPointerException), so the whole file is rejected. A
// policy whose text is not true/false (String.toBoolean, case-insensitive) is
// reported and skipped; a repeated policy name keeps its first position and
// the last value like Hashtable.put.
func parseGroups(doc *xmlDocument, logf func(string, ...any)) ([]Group, error) {
	var out []Group
	index := map[string]int{}
	for _, e := range doc.elements(tagGroup) {
		g := Group{Name: e.attr(attrName)}
		policies := e.elements(tagPolicy, nil)
		if len(policies) == 0 {
			return nil, fmt.Errorf("%w: group %q has no %s element", ErrXML, g.Name, tagPolicy)
		}
		seen := map[string]int{}
		for _, c := range policies[0].children {
			if c.kind != nodeElement {
				continue
			}
			text := c.textContent()
			var allow bool
			switch {
			case strings.EqualFold(text, "true"):
				allow = true
			case strings.EqualFold(text, "false"):
			default:
				logf("account: group %q policy %s=%q is not a boolean; skipped", g.Name, c.name, text)
				continue
			}
			if i, ok := seen[c.name]; ok {
				g.Policy[i].Allow = allow
				continue
			}
			seen[c.name] = len(g.Policy)
			g.Policy = append(g.Policy, Policy{Name: c.name, Allow: allow})
		}
		if i, ok := index[g.Name]; ok {
			out[i] = g
			continue
		}
		index[g.Name] = len(out)
		out = append(out, g)
	}
	return out, nil
}

func checkText(field, value string) error {
	if !validXMLText(value) {
		return fmt.Errorf("%w: %s is not valid XML 1.0 text", ErrInvalid, field)
	}
	return nil
}

func checkAccount(a Account) error {
	if err := a.checkFieldSizes(); err != nil {
		return err
	}
	for _, field := range [...]struct{ name, value string }{{"id", a.ID}, {"pass", a.Password}, {"email", a.Email}, {"group", a.Group}} {
		if err := checkText(field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

func firstElement(doc *xmlDocument, name string) (*xmlNode, error) {
	list := doc.elements(name)
	if len(list) == 0 {
		return nil, fmt.Errorf("%w: no <%s> element", ErrXML, name)
	}
	return list[0], nil
}

// addAccountDoc is AccountFileHandler.addAccount: append
// <Account id pass group><Email>email</Email></Account> to the first <Accounts>.
func addAccountDoc(doc *xmlDocument, a Account) error {
	accounts, err := firstElement(doc, tagAccounts)
	if err != nil {
		return err
	}
	e := &xmlNode{kind: nodeElement, name: tagAccount}
	e.setAttr(attrID, a.ID)
	e.setAttr(attrPass, a.Password)
	e.setAttr(attrGroup, a.Group)
	email := &xmlNode{kind: nodeElement, name: tagEmail}
	email.setTextContent(a.Email)
	e.children = append(e.children, email)
	accounts.children = append(accounts.children, e)
	return nil
}

// editAccountDoc is AccountFileHandler.editAccount: the first <Account> with
// the id gets pass/group replaced and its first <Email> text set.
func editAccountDoc(doc *xmlDocument, a Account) error {
	for _, e := range doc.elements(tagAccount) {
		if e.attr(attrID) != a.ID {
			continue
		}
		e.setAttr(attrPass, a.Password)
		e.setAttr(attrGroup, a.Group)
		emails := e.elements(tagEmail, nil)
		if len(emails) == 0 {
			// Java: getElementsByTagName(Email).item(0) is null -> NullPointerException.
			return fmt.Errorf("%w: account %q has no %s element", ErrXML, a.ID, tagEmail)
		}
		emails[0].setTextContent(a.Email)
		return nil
	}
	return fmt.Errorf("%w: cannot find account id %q", ErrXML, a.ID)
}

// setTextValue is GroupFileHandler.setTextValue: the first descendant with
// the name, created at the end when missing, gets its text replaced.
func setTextValue(parent *xmlNode, name, text string) {
	if found := parent.elements(name, nil); len(found) > 0 {
		found[0].setTextContent(text)
		return
	}
	e := &xmlNode{kind: nodeElement, name: name}
	e.setTextContent(text)
	parent.children = append(parent.children, e)
}

// addGroupDoc is GroupFileHandler.addAccountGroup writing policy in the given order.
func addGroupDoc(doc *xmlDocument, name string, policy []Policy) error {
	if err := checkText("group name", name); err != nil {
		return err
	}
	for _, p := range policy {
		if !validXMLName(p.Name) {
			return fmt.Errorf("%w: policy name %q is not an XML name", ErrInvalid, p.Name)
		}
	}
	groups, err := firstElement(doc, tagGroups)
	if err != nil {
		return err
	}
	g := &xmlNode{kind: nodeElement, name: tagGroup}
	g.setAttr(attrName, name)
	pe := &xmlNode{kind: nodeElement, name: tagPolicy}
	g.children = append(g.children, pe)
	for _, p := range policy {
		setTextValue(pe, p.Name, fmt.Sprint(p.Allow))
	}
	groups.children = append(groups.children, g)
	return nil
}

// editPolicyDoc is GroupFileHandler.editGroupPolicy/findAndSet: for each
// group key in MapPack order, the first <Group name> (re-listed per key as
// the live NodeList would be) gets each policy set under its first <Policy>;
// an unknown group is ignored.
func editPolicyDoc(doc *xmlDocument, edits []GroupPolicyEdit) error {
	// doc is a private parse of the file: a failure part way leaves nothing
	// written, like the Java exception before writeXmlFileWithIndent.
	for _, edit := range edits {
		for _, g := range doc.elements(tagGroup) {
			if g.attr(attrName) != edit.Group {
				continue
			}
			policies := g.elements(tagPolicy, nil)
			for _, p := range edit.Policy {
				if len(policies) == 0 {
					// Java: setTextValue on a null element -> NullPointerException.
					return fmt.Errorf("%w: group %q has no %s element", ErrXML, edit.Group, tagPolicy)
				}
				if !validXMLName(p.Name) {
					return fmt.Errorf("%w: policy name %q is not an XML name", ErrInvalid, p.Name)
				}
				if err := checkText("policy "+p.Name, p.Text); err != nil {
					return err
				}
				setTextValue(policies[0], p.Name, p.Text)
			}
			break
		}
	}
	return nil
}
