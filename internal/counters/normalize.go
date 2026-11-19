package counters

import "strings"

// normalize writes the Go counters.site.xml layout. It follows the outline
// of CounterManager.normalizeCountersXml (JAXB: <Types> then <Familys>,
// zws-metric./zps-metric. families removed, one entry per name with the
// last occurrence's content) with the decisions taken on the original
// defects (management-commands.md §9-3; CONTRACT.md lists every byte
// difference from the JAXB output):
//   - ObjectType child <Counter> elements and unknown attributes are kept;
//   - entries keep input order (a duplicate name takes the position of its
//     first occurrence), not HashMap order;
//   - an empty <Types/> is accepted (Java throws a NullPointerException);
//   - every <Types>/<Familys> child of the root is read, not only the last;
//   - attributes are written as given (no JAXB default values), so an
//     absent Counter total stays CounterEngine's default true.
//
// ok=false means Java's JAXB unmarshal throws JAXBException (root is not
// the no-namespace <Counters>, or a namespace error); callers then keep the
// original text as Java does.
func normalize(doc *document) (out []byte, ok bool, err error) {
	root := doc.root
	if doc.nsError || root == nil || root.ns != "" || root.local != "Counters" {
		return nil, false, nil
	}
	var types, families orderedByName
	for _, section := range root.children {
		if section.ns != "" {
			continue
		}
		switch section.local {
		case "Types":
			for _, c := range section.children {
				if c.ns == "" && c.local == "ObjectType" {
					types.put(c)
				}
			}
		case "Familys":
			for _, c := range section.children {
				if c.ns != "" || c.local != "Family" {
					continue
				}
				name, _ := plainAttr(c, "name")
				if strings.Contains(name, "zws-metric.") || strings.Contains(name, "zps-metric.") {
					continue
				}
				families.put(c)
			}
		}
	}
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Counters>\n")
	writeSection(&b, "Types", "ObjectType", types.items)
	writeSection(&b, "Familys", "Family", families.items)
	b.WriteString("</Counters>\n")
	return []byte(b.String()), true, nil
}

// orderedByName keeps one element per name attribute (absent is ""): the
// last element wins, placed where the name first appeared.
type orderedByName struct {
	index map[string]int
	items []*element
}

func (o *orderedByName) put(e *element) {
	name, _ := plainAttr(e, "name")
	if o.index == nil {
		o.index = map[string]int{}
	}
	if i, ok := o.index[name]; ok {
		o.items[i] = e
		return
	}
	o.index[name] = len(o.items)
	o.items = append(o.items, e)
}

func writeSection(b *strings.Builder, section, item string, items []*element) {
	if len(items) == 0 {
		b.WriteString("    <" + section + "/>\n")
		return
	}
	b.WriteString("    <" + section + ">\n")
	for _, e := range items {
		b.WriteString("        <" + item)
		writeAttrs(b, e)
		var counters []*element
		for _, c := range e.children {
			if c.ns == "" && c.local == "Counter" {
				counters = append(counters, c)
			}
		}
		if len(counters) == 0 {
			b.WriteString("/>\n")
			continue
		}
		b.WriteString(">\n")
		for _, c := range counters {
			b.WriteString("            <Counter")
			writeAttrs(b, c)
			b.WriteString("/>\n")
		}
		b.WriteString("        </" + item + ">\n")
	}
	b.WriteString("    </" + section + ">\n")
}

// plainAttr reads a no-namespace attribute.
func plainAttr(e *element, local string) (string, bool) {
	for _, a := range e.attrs {
		if !a.isDecl && a.ns == "" && a.local == local {
			return a.value, true
		}
	}
	return "", false
}

// writeAttrs writes the element's no-namespace attributes in input order.
// Namespace declarations and prefixed attributes are dropped: the layout
// declares no namespaces.
func writeAttrs(b *strings.Builder, e *element) {
	for _, a := range e.attrs {
		if a.isDecl || a.ns != "" || strings.Contains(a.qname, ":") {
			continue
		}
		writeAttr(b, a.qname, a.value)
	}
}

// writeAttr escapes like the JAXB RI 2.3 output observed with the oracle
// (& < > " escaped, LF/CR as decimal references) and additionally writes
// TAB as &#9; so a TAB survives attribute-value normalization on re-read
// (JAXB writes it literally and it becomes a space).
func writeAttr(b *strings.Builder, name, value string) {
	b.WriteByte(' ')
	b.WriteString(name)
	b.WriteString("=\"")
	for _, r := range value {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\n':
			b.WriteString("&#10;")
		case '\r':
			b.WriteString("&#13;")
		case '\t':
			b.WriteString("&#9;")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}
