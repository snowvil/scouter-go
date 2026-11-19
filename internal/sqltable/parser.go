// Package sqltable ports Scouter's SqlTables (S/core/SqlTables.scala) and the
// SQLSimpleParser.getCrudInfo heuristic it calls (scouter.common
// scouter/util/SQLSimpleParser.java). The contract is in CONTRACT.md.
package sqltable

import (
	"errors"
	"fmt"
	"strings"
)

// MaxSQLBytes is a Go-only input bound. Java parses any length; a longer SQL
// text is treated as a parse failure (failSet + S112) instead of being parsed.
const MaxSQLBytes = 1 << 20

// MaxWorkSteps bounds the token scans and node-list walks of one parse. The
// Java heuristic is O(tokens^2) and worse for nested unbalanced subqueries and
// keeps running; exceeding the budget is a parse failure (failSet + S112), the
// Go analogue of the Java Throwable path (StackOverflowError/OutOfMemoryError).
const MaxWorkSteps = 20_000_000

// MaxNodes bounds the SQL nodes (one per SELECT/DELETE/INSERT/UPDATE/MERGE
// INTO/JOIN, plus one per FROM re-scanned inside an open parenthesis) and so
// the parse memory. Exceeding it is reported as ErrBudget.
const MaxNodes = 100_000

var (
	// ErrTooLong is returned by Parse when the SQL text exceeds MaxSQLBytes.
	ErrTooLong = errors.New("sqltable: sql text exceeds the parse size limit")
	// ErrBudget is returned by Parse when parsing exceeds MaxWorkSteps.
	ErrBudget = errors.New("sqltable: sql parse exceeded the work budget")
)

// CrudInfo returns exactly what Java new SQLSimpleParser().getCrudInfo(sql)
// returns: "table(R),other(U)" in node order without duplicates, or "" when the
// Java heuristic raised an Exception (index out of range, "last index",
// "Can't find node"). Inputs that hit the Go-only bounds also yield "".
func CrudInfo(sql string) string {
	s, _ := Parse(sql)
	return s
}

// Parse is CrudInfo with the Go-only bound failures reported as ErrTooLong or
// ErrBudget. A Java Exception inside getCrudInfo is not an error here: Java
// catches it and returns "", and SqlTables stores that "".
func Parse(sql string) (string, error) {
	if len(sql) > MaxSQLBytes {
		return "", ErrTooLong
	}
	p := &parser{budget: MaxWorkSteps}
	return p.getCrudInfo(sql)
}

// sqlType mirrors SQLSimpleParser.SQLTypeEnum and its toString.
type sqlType uint8

const (
	typeSelect sqlType = iota
	typeDelete
	typeUpdate
	typeInsert
	typeMerge
)

func (t sqlType) String() string {
	switch t {
	case typeDelete:
		return "D"
	case typeUpdate:
		return "U"
	case typeInsert, typeMerge:
		return "C"
	default:
		return "R"
	}
}

type node struct {
	typ    sqlType
	tables []string
	depth  int
	next   *node
}

// javaException stands for any java.lang.Exception thrown inside
// getCrudInfo's try block; getCrudInfo returns "" for it.
type javaException struct{ msg string }

// budgetExceeded aborts a parse that ran past MaxWorkSteps.
type budgetExceeded struct{}

type parser struct {
	head, tail    *node // tail only shortcuts appendNode's walk to the end
	inParenthesis bool
	parenCount    int
	depth         int
	tokens        []string
	budget        int
	nodes         int
	// byDepth indexes the non-head nodes by their creation depth in list
	// order. A node's depth only ever changes to -1, so stale entries are
	// skipped lazily and findNode stays amortized O(1) instead of Java's walk.
	byDepth map[int][]*node
}

func (p *parser) step() {
	p.budget--
	if p.budget < 0 {
		panic(budgetExceeded{})
	}
}

// tok is tokens[i] with Java's ArrayIndexOutOfBoundsException.
func (p *parser) tok(i int) string {
	if i < 0 || i >= len(p.tokens) {
		panic(javaException{fmt.Sprintf("index %d out of bounds for length %d", i, len(p.tokens))})
	}
	return p.tokens[i]
}

func (p *parser) getCrudInfo(value string) (result string, err error) {
	ok := func() (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				switch r.(type) {
				case javaException:
					ok = false
				case budgetExceeded:
					err = ErrBudget
					ok = false
				default:
					panic(r)
				}
			}
		}()
		p.tokens = p.tokenize(value)
		p.crudInfo()
		return true
	}()
	if !ok {
		return "", err
	}
	var sb strings.Builder
	seen := make(map[string]struct{})
	for n := p.head; n != nil; n = n.next {
		for _, t := range n.tables {
			info := t + "(" + n.typ.String() + ")"
			if _, dup := seen[info]; dup {
				continue
			}
			if sb.Len() > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(info)
			seen[info] = struct{}{}
		}
	}
	return sb.String(), nil
}

func (p *parser) createOrAppendNode(t sqlType) {
	p.step()
	p.nodes++
	if p.nodes > MaxNodes {
		panic(budgetExceeded{})
	}
	p.depth++
	n := &node{typ: t, depth: p.depth}
	if p.head == nil {
		p.head = n
	} else {
		p.tail.next = n
		if p.byDepth == nil {
			p.byDepth = make(map[int][]*node)
		}
		p.byDepth[n.depth] = append(p.byDepth[n.depth], n)
	}
	p.tail = n
}

// findNode keeps Java's quirk: with more than one node the head is never
// matched, and the last node is returned when no node after the head has the
// depth. The result equals Java's walk from the head.
func (p *parser) findNode(depth int) *node {
	if p.head.next == nil {
		return p.head
	}
	q := p.byDepth[depth]
	for len(q) > 0 && q[0].depth != depth {
		p.step()
		q = q[1:]
	}
	p.byDepth[depth] = q
	if len(q) > 0 {
		return q[0]
	}
	return p.tail
}

func (p *parser) crudInfo() {
	tokens := p.tokens
	for i := 0; i < len(tokens); i++ {
		p.step()
		token := tokens[i]
		switch {
		case token == "(":
			p.parenCount++
			p.inParenthesis = true
		case token == ")":
			p.parenCount--
			if p.parenCount == 0 {
				p.inParenthesis = false
			}
		case upperIs(token, "SELECT"):
			if p.inParenthesis {
				for j := i + 1; j < len(tokens); j++ {
					p.step()
					if lowerIs(tokens[j], "from") {
						p.createOrAppendNode(typeSelect)
						j = p.applyNode(j)
					}
					if tokens[j] == "(" {
						p.parenCount++
						p.inParenthesis = true
					}
					if tokens[j] == ")" {
						p.parenCount--
						if p.parenCount == 0 {
							p.inParenthesis = false
							i = j
							break
						}
					}
				}
			} else {
				p.createOrAppendNode(typeSelect)
			}
		case upperIs(token, "DELETE"):
			p.createOrAppendNode(typeDelete)
		case upperIs(token, "INSERT"):
			p.createOrAppendNode(typeInsert)
		case upperIs(token, "UPDATE"):
			p.createOrAppendNode(typeUpdate)
			i = p.applyNode(i)
		case upperIs(token, "MERGE"):
			if upperIs(p.tok(i+1), "INTO") {
				p.createOrAppendNode(typeMerge)
			}
		case upperIs(token, "FROM"), upperIs(token, "INTO"):
			i = p.applyNode(i)
		case upperIs(token, "JOIN"):
			p.createOrAppendNode(typeSelect)
			i = p.applyNode(i)
		}
	}
}

func (p *parser) applyNode(index int) int {
	tokens := p.tokens
	returnIndex := index
	if index == len(tokens)-1 {
		panic(javaException{"last index of tokens"})
	}
	if p.depth == 0 {
		return index
	}
	n := p.findNode(p.depth)
	if n.typ == typeSelect {
		if p.tok(index+1) != "(" {
			n.tables = append(n.tables, p.tok(index+1))
			returnIndex = index + 1
			step := 0
			if len(tokens) > index+2 && tokens[index+2] == "," {
				step = 2
			}
			if len(tokens) > index+3 && tokens[index+3] == "," {
				step = 3
			}
			if step > 0 {
				for i := index + 1; i < len(tokens); i += step {
					p.step()
					if len(tokens) < i+step {
						break
					}
					if p.tok(i+step-1) != "," {
						break
					}
					if t := p.tok(i + step); t != "(" {
						n.tables = append(n.tables, t)
						returnIndex = i + step
					}
				}
			}
		}
	} else {
		n.tables = append(n.tables, p.tok(index+1))
		returnIndex = index + 1
	}
	n.depth = -1
	p.depth--
	return returnIndex
}

func isSpliter(c byte) bool {
	switch c {
	case '=', '<', '>', '!', ',', ' ', '(', ')', '\r', '\n':
		return true
	}
	return false
}

// tokenize ports SQLSimpleParser.tokenize(value, false). Java walks UTF-16
// code units; every character the tokenizer inspects is ASCII and UTF-8 never
// uses ASCII bytes inside a multi-byte sequence, so walking bytes of a valid
// UTF-8 string cuts the same tokens.
func (p *parser) tokenize(value string) []string {
	var tokens []string
	hasQuotation, hasSingle, hasMulti := false, false, false
	start := 0
	n := len(value)
	for i := 0; i < n; i++ {
		p.step()
		c := value[i]
		if i == n-1 {
			if isSpliter(c) {
				tokens = append(tokens, copyOfRange(value, start, n-1))
				if c != ' ' {
					tokens = append(tokens, string(c))
				}
			} else {
				tokens = append(tokens, copyOfRange(value, start, n))
			}
			break
		}
		if c == '\'' {
			hasQuotation = !hasQuotation
			continue
		}
		if c == '-' {
			if !hasQuotation && value[i+1] == '-' {
				hasSingle = true
			}
			continue
		}
		if c == '#' {
			if !hasQuotation {
				hasSingle = true
			}
			continue
		}
		if c == '\n' && hasSingle {
			hasSingle = false
		}
		if c == '/' {
			if value[i+1] == '*' {
				hasMulti = true
			}
			if i > 0 && value[i-1] == '*' && hasMulti {
				hasMulti = false
				start = i + 1
			}
			continue
		}
		if i > 0 && isSpliter(c) {
			if hasQuotation || hasSingle || hasMulti {
				continue
			}
			if i >= start {
				if i > start {
					tokens = append(tokens, value[start:i])
				}
				switch c {
				case ' ':
					start = i + 1
					continue
				case '\r':
					if value[i+1] == '\n' {
						start = i + 2
						i++
						continue
					}
					start = i + 1
					continue
				case '\n':
					start = i + 1
					continue
				}
				if next := value[i+1]; isSpliter(next) {
					temp := value[i : i+2]
					if temp == "<>" || temp == "!=" || temp == "<=" || temp == ">=" {
						tokens = append(tokens, temp)
						i++
					} else {
						tokens = append(tokens, string(c))
					}
				} else {
					tokens = append(tokens, string(c))
				}
			}
			start = i + 1
			continue
		}
	}
	return tokens
}

// copyOfRange mirrors Arrays.copyOfRange, which throws when from > to.
func copyOfRange(s string, from, to int) string {
	if from > to || from < 0 || to > len(s) {
		panic(javaException{"copyOfRange"})
	}
	return s[from:to]
}

// upperIs reports whether Java token.toUpperCase().equals(kw) for an ASCII
// upper-case keyword. Non-ASCII tokens go through strings.ToUpper, whose
// simple mapping agrees with Java's for every character that can upper-case
// into a keyword letter (for example U+017F long s and U+0131 dotless i).
// Java's default locale is assumed not to be Turkish (see CONTRACT.md).
func upperIs(token, kw string) bool {
	if len(token) == len(kw) && asciiFoldEqual(token, kw) {
		return true
	}
	if isASCII(token) || len(token) < len(kw) {
		return false
	}
	return strings.ToUpper(token) == kw
}

// lowerIs reports whether Java token.toLowerCase().equals(kw) for an ASCII
// lower-case keyword. No non-ASCII character lower-cases to an ASCII letter
// other than U+212A KELVIN SIGN ("k") and U+0130 (Go "i", Java "i̇"),
// and the only caller's keyword "from" contains neither.
func lowerIs(token, kw string) bool {
	if len(token) == len(kw) && asciiFoldEqual(token, kw) {
		return true
	}
	if isASCII(token) || len(token) < len(kw) {
		return false
	}
	return strings.ToLower(token) == kw
}

func asciiFoldEqual(a, b string) bool {
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'a' <= x && x <= 'z' {
			x -= 'a' - 'A'
		}
		if 'a' <= y && y <= 'z' {
			y -= 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
