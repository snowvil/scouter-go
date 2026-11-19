// Package counters models Scouter counters.xml and the custom
// counters.site.xml file (CounterManager.java, CounterEngine.java at the
// pinned source dee47fb5). See CONTRACT.md for the byte and error contract.
package counters

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// ErrMalformed marks a document that Java DocumentBuilder.parse would reject
// (CounterEngine.parse returns false for it).
var ErrMalformed = errors.New("counters: malformed XML")

// ErrLimit marks a document or file beyond the configured bounds.
var ErrLimit = errors.New("counters: limit exceeded")

const (
	maxDepth    = 256
	maxElements = 200000
)

// element is a minimal non-namespace-aware DOM element, as DocumentBuilder
// (DocumentBuilderFactory default, namespaceAware=false) builds it. Text,
// comments and processing instructions are not retained: neither
// CounterEngine.parse nor the normalizer reads them.
type element struct {
	qname    string
	attrs    []attr
	children []*element
	// Namespace view used only by the normalizer, which keeps Java's
	// namespace-aware (JAXB) element matching.
	ns, local string
}

type attr struct {
	qname, value string
	ns, local    string
	isDecl       bool // xmlns or xmlns:*
}

type document struct {
	root *element
	// nsError is set when a namespace-aware SAX parser (JAXB unmarshal)
	// would fail although the non-namespace DOM parse succeeds.
	nsError bool
}

func (e *element) attr(qname string) (string, bool) {
	for _, a := range e.attrs {
		if a.qname == qname {
			return a.value, true
		}
	}
	return "", false
}

func (e *element) setAttr(qname, value string) {
	for i := range e.attrs {
		if e.attrs[i].qname == qname {
			e.attrs[i].value = value
			return
		}
	}
	e.attrs = append(e.attrs, attr{qname: qname, value: value, local: qname})
}

// walk visits e and its descendants in document (pre)order, like
// Element.getElementsByTagName.
func (e *element) walk(visit func(*element)) {
	visit(e)
	for _, c := range e.children {
		c.walk(visit)
	}
}

func (d *document) elementsByTagName(qname string) []*element {
	var out []*element
	if d.root == nil {
		return nil
	}
	d.root.walk(func(e *element) {
		if e.qname == qname {
			out = append(out, e)
		}
	})
	return out
}

func descendantsByTagName(e *element, qname string) []*element {
	var out []*element
	for _, c := range e.children {
		c.walk(func(x *element) {
			if x.qname == qname {
				out = append(out, x)
			}
		})
	}
	return out
}

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// parseDocument applies the XML 1.0 well-formedness rules that Java's default
// DocumentBuilder enforces and that the Go decoder leaves to the caller
// (matching end tags, single root, duplicate attributes, XML declaration
// position, attribute-value whitespace normalization). DOCTYPE is refused:
// see CONTRACT.md (intended difference).
func parseDocument(content []byte, maxBytes int) (*document, error) {
	if len(content) > maxBytes {
		return nil, ErrLimit
	}
	content = bytes.TrimPrefix(content, utf8BOM)
	if bytes.Contains(content, []byte(placeholderTab)) || bytes.Contains(content, []byte(placeholderLF)) || bytes.Contains(content, []byte(placeholderCR)) {
		return nil, ErrUnsupportedDocument
	}
	content = protectCharRefs(content)
	dec := xml.NewDecoder(bytes.NewReader(content))
	dec.Strict = true
	dec.CharsetReader = charsetReader
	doc := &document{}
	var stack []*element
	var scopes []map[string]string // prefix -> namespace ("" key = default)
	first := true
	count := 0
	for {
		tok, err := dec.RawToken()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		switch t := tok.(type) {
		case xml.ProcInst:
			if strings.EqualFold(t.Target, "xml") && !(first && t.Target == "xml") {
				return nil, fmt.Errorf("%w: misplaced XML declaration", ErrMalformed)
			}
		case xml.Directive:
			return nil, fmt.Errorf("%w: DOCTYPE/directive not accepted", ErrMalformed)
		case xml.CharData:
			if len(stack) == 0 && len(bytes.TrimLeft(t, " \t\r\n")) != 0 {
				return nil, fmt.Errorf("%w: content outside root element", ErrMalformed)
			}
		case xml.StartElement:
			if len(stack) == 0 && doc.root != nil {
				return nil, fmt.Errorf("%w: multiple root elements", ErrMalformed)
			}
			if len(stack) >= maxDepth {
				return nil, ErrLimit
			}
			if count++; count > maxElements {
				return nil, ErrLimit
			}
			e := &element{qname: qualified(t.Name)}
			seen := make(map[string]bool, len(t.Attr))
			scope := map[string]string{}
			for _, a := range t.Attr {
				q := qualified(a.Name)
				if seen[q] {
					return nil, fmt.Errorf("%w: duplicate attribute %s", ErrMalformed, q)
				}
				seen[q] = true
				// Attribute-value normalization for CDATA attributes
				// (XML 1.0 §3.3.3). The decoder already folded CR/CRLF to LF.
				v := strings.Map(func(r rune) rune {
					switch r {
					case '\t', '\n', '\r':
						return ' '
					case runeTab:
						return '\t'
					case runeLF:
						return '\n'
					case runeCR:
						return '\r'
					}
					return r
				}, a.Value)
				na := attr{qname: q, value: v}
				switch {
				case a.Name.Space == "" && a.Name.Local == "xmlns":
					na.isDecl = true
					scope[""] = v
				case a.Name.Space == "xmlns":
					na.isDecl = true
					scope[a.Name.Local] = v
				}
				e.attrs = append(e.attrs, na)
			}
			scopes = append(scopes, scope)
			e.ns, e.local = resolve(t.Name, scopes, true, &doc.nsError)
			for i := range e.attrs {
				a := &e.attrs[i]
				if a.isDecl {
					continue
				}
				prefix, local := splitQName(a.qname)
				a.ns, a.local = resolve(xml.Name{Space: prefix, Local: local}, scopes, false, &doc.nsError)
			}
			if len(stack) == 0 {
				doc.root = e
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, e)
			}
			stack = append(stack, e)
		case xml.EndElement:
			if len(stack) == 0 || stack[len(stack)-1].qname != qualified(t.Name) {
				return nil, fmt.Errorf("%w: mismatched end tag %s", ErrMalformed, qualified(t.Name))
			}
			stack = stack[:len(stack)-1]
			scopes = scopes[:len(scopes)-1]
		}
		first = false
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("%w: unclosed element", ErrMalformed)
	}
	if doc.root == nil {
		return nil, fmt.Errorf("%w: no root element", ErrMalformed)
	}
	return doc, nil
}

func qualified(n xml.Name) string {
	if n.Space == "" {
		return n.Local
	}
	return n.Space + ":" + n.Local
}

func splitQName(q string) (string, string) {
	if i := strings.IndexByte(q, ':'); i >= 0 {
		return q[:i], q[i+1:]
	}
	return "", q
}

// resolve maps a raw (prefix, local) name to its namespace. Unprefixed
// attributes are never in a namespace. An undeclared prefix or a malformed
// QName is a namespace-aware parse error (JAXB unmarshal fails).
func resolve(n xml.Name, scopes []map[string]string, isElement bool, nsError *bool) (string, string) {
	if strings.Contains(n.Local, ":") || (n.Space != "" && n.Local == "") {
		*nsError = true
		return "", qualified(n)
	}
	if n.Space == "" && !isElement {
		return "", n.Local
	}
	if n.Space == "xml" {
		return "http://www.w3.org/XML/1998/namespace", n.Local
	}
	for i := len(scopes) - 1; i >= 0; i-- {
		if ns, ok := scopes[i][n.Space]; ok {
			if ns == "" && n.Space != "" {
				*nsError = true
			}
			return ns, n.Local
		}
	}
	if n.Space != "" {
		*nsError = true
	}
	return "", n.Local
}

// charsetReader accepts the single-byte encodings whose bytes map 1:1 to
// code points. Other declared encodings are refused (CONTRACT.md).
func charsetReader(label string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(label) {
	case "iso-8859-1", "latin1", "iso8859-1", "us-ascii", "ascii":
		raw, err := io.ReadAll(input)
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		for _, b := range raw {
			out.WriteRune(rune(b))
		}
		if bytes.Contains(out.Bytes(), []byte(placeholderTab)) || bytes.Contains(out.Bytes(), []byte(placeholderLF)) || bytes.Contains(out.Bytes(), []byte(placeholderCR)) {
			return nil, ErrUnsupportedDocument
		}
		return bytes.NewReader(protectCharRefs(out.Bytes())), nil
	}
	return nil, fmt.Errorf("unsupported encoding %q", label)
}

// Character references to TAB/LF/CR survive attribute-value normalization
// (XML 1.0 3.3.3) while literal whitespace becomes a space. encoding/xml
// resolves references before the caller sees the value, so they are swapped
// for noncharacter placeholders first and restored after normalization.
// Documents already containing a placeholder are refused.
const (
	runeTab        = '\uFDD0'
	runeLF         = '\uFDD1'
	runeCR         = '\uFDD2'
	placeholderTab = string(runeTab)
	placeholderLF  = string(runeLF)
	placeholderCR  = string(runeCR)
)

var charRef = regexp.MustCompile(`&#(?:0*(9|10|13)|x0*([9aAdD]));`)

func protectCharRefs(content []byte) []byte {
	if !bytes.Contains(content, []byte("&#")) {
		return content
	}
	return charRef.ReplaceAllFunc(content, func(m []byte) []byte {
		switch strings.TrimLeft(strings.TrimSuffix(strings.TrimPrefix(string(m), "&#"), ";"), "x0") {
		case "9":
			return []byte(placeholderTab)
		case "10", "a", "A":
			return []byte(placeholderLF)
		default:
			return []byte(placeholderCR)
		}
	})
}
