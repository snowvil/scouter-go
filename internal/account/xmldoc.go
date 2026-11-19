// Account/group XML handling: Copyright 2015 Scouter authors, Apache-2.0.
// Independent implementation of pinned source dee47fb5 (AccountFileHandler,
// GroupFileHandler, XmlUtil.writeXmlFileWithIndent).
package account

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ErrXML marks a file that the account store cannot load or round-trip.
var ErrXML = errors.New("account: invalid XML")

type nodeKind uint8

const (
	nodeElement nodeKind = iota + 1
	nodeText
	nodeComment
	nodeProcInst
)

// xmlAttr keeps the attribute name as written (prefix included).
type xmlAttr struct{ Name, Value string }

// xmlNode is a minimal DOM: Xerces DocumentBuilder without namespace
// awareness produces exactly these node kinds after normalize().
type xmlNode struct {
	kind     nodeKind
	name     string
	attrs    []xmlAttr
	children []*xmlNode
	text     string
}

// xmlDocument holds the prolog/epilog siblings of the root element and the
// standalone flag that decides the XML declaration Xalan emits.
type xmlDocument struct {
	standalone bool
	prolog     []*xmlNode
	root       *xmlNode
	epilog     []*xmlNode
}

const maxXMLDepth = 64

func rawName(n xml.Name) string {
	if n.Space == "" {
		return n.Local
	}
	return n.Space + ":" + n.Local
}

// parseXML builds the tree with RawToken so prefixes stay as written and no
// namespace binding is required. DOCTYPE and non-UTF-8 encodings are
// rejected explicitly instead of being silently rewritten.
func parseXML(data []byte) (*xmlDocument, error) {
	// Xerces accepts a UTF-8 byte order mark; Xalan never writes one.
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	d := xml.NewDecoder(bytes.NewReader(data))
	d.Strict = true
	doc := &xmlDocument{}
	var stack []*xmlNode
	sawDecl := false
	for {
		tok, err := d.RawToken()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrXML, err)
		}
		var parent *xmlNode
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		switch t := tok.(type) {
		case xml.ProcInst:
			if t.Target == "xml" {
				if sawDecl || parent != nil || doc.root != nil || len(doc.prolog) > 0 {
					return nil, fmt.Errorf("%w: misplaced XML declaration", ErrXML)
				}
				sawDecl = true
				doc.standalone = declStandalone(string(t.Inst))
				continue
			}
			doc.append(parent, &xmlNode{kind: nodeProcInst, name: t.Target, text: string(t.Inst)})
		case xml.Comment:
			doc.append(parent, &xmlNode{kind: nodeComment, text: string(t)})
		case xml.Directive:
			return nil, fmt.Errorf("%w: DOCTYPE/directive unsupported", ErrXML)
		case xml.CharData:
			if parent == nil {
				if len(bytes.TrimSpace(t)) != 0 {
					return nil, fmt.Errorf("%w: text outside root element", ErrXML)
				}
				continue
			}
			if len(t) == 0 {
				continue
			}
			// Xerces merges adjacent text nodes (normalize()).
			if n := len(parent.children); n > 0 && parent.children[n-1].kind == nodeText {
				parent.children[n-1].text += string(t)
				continue
			}
			parent.children = append(parent.children, &xmlNode{kind: nodeText, text: string(t)})
		case xml.StartElement:
			if len(stack) >= maxXMLDepth {
				return nil, fmt.Errorf("%w: depth exceeds %d", ErrXML, maxXMLDepth)
			}
			if parent == nil && doc.root != nil {
				return nil, fmt.Errorf("%w: multiple root elements", ErrXML)
			}
			e := &xmlNode{kind: nodeElement, name: rawName(t.Name)}
			for _, a := range t.Attr {
				name := rawName(a.Name)
				for i := range e.attrs {
					if e.attrs[i].Name == name {
						return nil, fmt.Errorf("%w: duplicate attribute %s", ErrXML, name)
					}
				}
				e.attrs = append(e.attrs, xmlAttr{Name: name, Value: a.Value})
			}
			if parent == nil {
				doc.root = e
			} else {
				parent.children = append(parent.children, e)
			}
			stack = append(stack, e)
		case xml.EndElement:
			if parent == nil || parent.name != rawName(t.Name) {
				return nil, fmt.Errorf("%w: unexpected end element %s", ErrXML, rawName(t.Name))
			}
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("%w: unclosed element %s", ErrXML, stack[len(stack)-1].name)
	}
	if doc.root == nil {
		return nil, fmt.Errorf("%w: missing root element", ErrXML)
	}
	return doc, nil
}

func (doc *xmlDocument) append(parent, n *xmlNode) {
	switch {
	case parent != nil:
		parent.children = append(parent.children, n)
	case doc.root == nil:
		doc.prolog = append(doc.prolog, n)
	default:
		doc.epilog = append(doc.epilog, n)
	}
}

// declStandalone reads standalone="yes" from the declaration body; Xerces
// exposes it as Document.getXmlStandalone and DOM2TO only passes "no" on.
func declStandalone(inst string) bool {
	i := strings.Index(inst, "standalone")
	if i < 0 {
		return false
	}
	rest := strings.TrimSpace(inst[i+len("standalone"):])
	if !strings.HasPrefix(rest, "=") {
		return false
	}
	rest = strings.TrimSpace(rest[1:])
	return strings.HasPrefix(rest, `"yes"`) || strings.HasPrefix(rest, `'yes'`)
}

// elements returns every element with the given name in document order,
// like Document/Element.getElementsByTagName.
func (n *xmlNode) elements(name string, out []*xmlNode) []*xmlNode {
	for _, c := range n.children {
		if c.kind != nodeElement {
			continue
		}
		if c.name == name {
			out = append(out, c)
		}
		out = c.elements(name, out)
	}
	return out
}

func (doc *xmlDocument) elements(name string) []*xmlNode {
	var out []*xmlNode
	if doc.root.name == name {
		out = append(out, doc.root)
	}
	return doc.root.elements(name, out)
}

func (n *xmlNode) attr(name string) string {
	for _, a := range n.attrs {
		if a.Name == name {
			return a.Value
		}
	}
	return ""
}

func (n *xmlNode) setAttr(name, value string) {
	for i := range n.attrs {
		if n.attrs[i].Name == name {
			n.attrs[i].Value = value
			return
		}
	}
	n.attrs = append(n.attrs, xmlAttr{Name: name, Value: value})
}

// textContent concatenates descendant text like Node.getTextContent.
func (n *xmlNode) textContent() string {
	var b strings.Builder
	n.collectText(&b)
	return b.String()
}

func (n *xmlNode) collectText(b *strings.Builder) {
	for _, c := range n.children {
		switch c.kind {
		case nodeText:
			b.WriteString(c.text)
		case nodeElement:
			c.collectText(b)
		}
	}
}

// setTextContent replaces all children with one text node, or none for "".
func (n *xmlNode) setTextContent(text string) {
	if text == "" {
		n.children = nil
		return
	}
	n.children = []*xmlNode{{kind: nodeText, text: text}}
}

// validXMLName follows XML 1.0 Name (what Xerces createElement enforces
// through DOMException INVALID_CHARACTER_ERR when violated).
func validXMLName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	for i, r := range name {
		if !nameChar(r, i == 0) {
			return false
		}
	}
	return true
}

func nameChar(r rune, start bool) bool {
	switch {
	case r == ':' || r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
		return true
	case r >= 0xC0 && r <= 0xD6, r >= 0xD8 && r <= 0xF6, r >= 0xF8 && r <= 0x2FF, r >= 0x370 && r <= 0x37D, r >= 0x37F && r <= 0x1FFF,
		r >= 0x200C && r <= 0x200D, r >= 0x2070 && r <= 0x218F, r >= 0x2C00 && r <= 0x2FEF, r >= 0x3001 && r <= 0xD7FF,
		r >= 0xF900 && r <= 0xFDCF, r >= 0xFDF0 && r <= 0xFFFD, r >= 0x10000 && r <= 0xEFFFF:
		return true
	}
	if start {
		return false
	}
	return r == '-' || r == '.' || (r >= '0' && r <= '9') || r == 0xB7 || (r >= 0x300 && r <= 0x36F) || (r >= 0x203F && r <= 0x2040)
}

// validXMLText reports whether every rune is a legal XML 1.0 Char; Xalan
// would serialize others as unparseable numeric references.
func validXMLText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
		if r == 0xFFFE || r == 0xFFFF {
			return false
		}
	}
	return true
}

// xalanWriter reproduces JDK 8 com.sun.org.apache.xml.internal.serializer
// ToStream output for indent="yes", indent-amount=2: existing whitespace
// text is written verbatim, a start tag directly after text gets no
// indentation, and indentation is only inserted where no text preceded.
type xalanWriter struct {
	b        bytes.Buffer
	prevText bool
}

// marshal is the byte-for-byte target of XmlUtil.writeXmlFileWithIndent(doc, file, 2)
// on a JDK 8 runtime (JDK 9+ Xalan indents whitespace text differently).
func (doc *xmlDocument) marshal() []byte {
	w := &xalanWriter{}
	w.b.WriteString(`<?xml version="1.0" encoding="UTF-8"`)
	if doc.standalone {
		// DOM2TO only forwards standalone when the document is not standalone,
		// and ToStream then omits the line break after the declaration.
		w.b.WriteString("?>")
	} else {
		w.b.WriteString(` standalone="no"?>` + "\n")
	}
	for _, n := range doc.prolog {
		w.misc(n, 0)
	}
	w.element(doc.root, 0)
	for _, n := range doc.epilog {
		w.misc(n, 0)
	}
	if !w.prevText {
		w.b.WriteByte('\n')
	}
	return w.b.Bytes()
}

func (w *xalanWriter) indent(depth int) {
	w.b.WriteByte('\n')
	for range depth * 2 {
		w.b.WriteByte(' ')
	}
}

// misc writes a comment or processing instruction; shouldIndent needs an
// open element (depth > 0) and no preceding text.
func (w *xalanWriter) misc(n *xmlNode, depth int) {
	if !w.prevText && depth > 0 {
		w.indent(depth)
	}
	switch n.kind {
	case nodeComment:
		w.b.WriteString("<!--")
		w.b.WriteString(n.text)
		w.b.WriteString("-->")
	case nodeProcInst:
		w.b.WriteString("<?")
		w.b.WriteString(n.name)
		if n.text != "" {
			w.b.WriteByte(' ')
			w.b.WriteString(n.text)
		}
		w.b.WriteString("?>")
	}
}

func (w *xalanWriter) element(n *xmlNode, parentDepth int) {
	if !w.prevText && parentDepth > 0 {
		w.indent(parentDepth)
	}
	w.b.WriteByte('<')
	w.b.WriteString(n.name)
	attrs := append([]xmlAttr(nil), n.attrs...)
	// Xerces NamedNodeMapImpl keeps attributes sorted by name.
	sort.SliceStable(attrs, func(i, j int) bool { return javaLess(attrs[i].Name, attrs[j].Name) })
	for _, a := range attrs {
		w.b.WriteByte(' ')
		w.b.WriteString(a.Name)
		w.b.WriteString(`="`)
		w.attrValue(a.Value)
		w.b.WriteByte('"')
	}
	if len(n.children) == 0 {
		w.b.WriteString("/>")
		w.prevText = false
		return
	}
	w.b.WriteByte('>')
	w.prevText = false
	depth := parentDepth + 1
	for _, c := range n.children {
		switch c.kind {
		case nodeText:
			w.text(c.text)
			w.prevText = true
		case nodeElement:
			w.element(c, depth)
		default:
			w.misc(c, depth)
		}
	}
	if !w.prevText {
		w.indent(depth - 1)
	}
	w.b.WriteString("</")
	w.b.WriteString(n.name)
	w.b.WriteByte('>')
	w.prevText = false
}

// javaLess orders by UTF-16 code units like String.compareTo.
func javaLess(a, b string) bool {
	ra, rb := []rune(a), []rune(b)
	for i := 0; i < len(ra) && i < len(rb); i++ {
		if ra[i] != rb[i] {
			ua, ub := utf16Units(ra[i]), utf16Units(rb[i])
			for j := 0; j < len(ua) && j < len(ub); j++ {
				if ua[j] != ub[j] {
					return ua[j] < ub[j]
				}
			}
			return len(ua) < len(ub)
		}
	}
	return len(ra) < len(rb)
}

func utf16Units(r rune) []uint16 {
	if r < 0x10000 {
		return []uint16{uint16(r)}
	}
	r -= 0x10000
	return []uint16{uint16(0xD800 + (r >> 10)), uint16(0xDC00 + (r & 0x3FF))}
}

func (w *xalanWriter) numeric(r rune) {
	w.b.WriteString("&#")
	w.b.WriteString(strconv.Itoa(int(r)))
	w.b.WriteByte(';')
}

// text matches ToStream.characters: &, <, > and CR are escaped, C1 controls
// and DEL and non-BMP characters become decimal references, "]]>" becomes
// "]]&gt;" through the plain '>' rule.
func (w *xalanWriter) text(s string) {
	for _, r := range s {
		switch {
		case r == '&':
			w.b.WriteString("&amp;")
		case r == '<':
			w.b.WriteString("&lt;")
		case r == '>':
			w.b.WriteString("&gt;")
		case r == '\r', r >= 0x7F && r <= 0x9F, r > 0xFFFF:
			w.numeric(r)
		default:
			w.b.WriteRune(r)
		}
	}
}

// attrValue matches ToStream.writeAttrString: quotes and the three
// whitespace controls are escaped, C1/DEL stay raw, non-BMP is numeric.
func (w *xalanWriter) attrValue(s string) {
	for _, r := range s {
		switch {
		case r == '&':
			w.b.WriteString("&amp;")
		case r == '<':
			w.b.WriteString("&lt;")
		case r == '>':
			w.b.WriteString("&gt;")
		case r == '"':
			w.b.WriteString("&quot;")
		case r == '\t', r == '\n', r == '\r', r > 0xFFFF:
			w.numeric(r)
		default:
			w.b.WriteRune(r)
		}
	}
}
