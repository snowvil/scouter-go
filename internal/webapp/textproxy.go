package webapp

import (
	"container/list"
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// TextProxy/TextModel (framework/client/model/TextProxy.java, TextModel.java):
// one LRU per text type keyed by hash (not by date), filled by GET_TEXT_100
// {date, type, hash[]} -> MapPack {hexa32(hash): text} and GET_TEXT_ANY_TYPE
// {type[], hash[]} -> TextPack stream. Unresolved hashes become
// "**unlabeled**:<type>:<hash>", hash 0 is "".

var textLimits = map[string]int{
	"service": 20000, "sql": 20000, "method": 40000, "error": 20000, "apicall": 40000,
	"object": 2000, "referer": 20000, "ua": 40000, "group": 2000, "table": 4000,
	"city": 20000, "maria": 2000, "web": 20000, "login": 40000, "desc": 40000,
	"hmsg": 20000, "stackelem": 20000,
}

const (
	defaultTextLimit = 20000
	maxTextTypes     = 64
	// maxTextBatch bounds one GET_TEXT_ANY_TYPE request below the codec's
	// 4096-element collection limit (type[] and hash[] lists).
	maxTextBatch = 2000
)

type textLRU struct {
	limit int
	byKey map[int32]*list.Element
	order *list.List
}

type textEntry struct {
	hash int32
	text string
}

type textProxy struct {
	mu    sync.Mutex
	types map[string]*textLRU
}

func newTextProxy() *textProxy { return &textProxy{types: map[string]*textLRU{}} }

func (p *textProxy) lru(textType string) *textLRU {
	l := p.types[textType]
	if l == nil {
		if len(p.types) >= maxTextTypes {
			return nil
		}
		limit, ok := textLimits[textType]
		if !ok {
			limit = defaultTextLimit
		}
		l = &textLRU{limit: limit, byKey: map[int32]*list.Element{}, order: list.New()}
		p.types[textType] = l
	}
	return l
}

func (p *textProxy) get(textType string, hash int32) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l := p.types[textType]
	if l == nil {
		return "", false
	}
	e, ok := l.byKey[hash]
	if !ok {
		return "", false
	}
	return e.Value.(textEntry).text, true
}

// put is TextModel.cache: LinkedMap put, dropping the oldest entry over limit.
func (p *textProxy) put(textType string, hash int32, text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l := p.lru(textType)
	if l == nil {
		return
	}
	if e, ok := l.byKey[hash]; ok {
		e.Value = textEntry{hash, text}
		return
	}
	l.byKey[hash] = l.order.PushBack(textEntry{hash, text})
	for l.order.Len() > l.limit {
		front := l.order.Front()
		l.order.Remove(front)
		delete(l.byKey, front.Value.(textEntry).hash)
	}
}

func unlabeled(textType string, hash int32) string {
	return "**unlabeled**:" + textType + ":" + strconv.FormatInt(int64(hash), 10)
}

// text is TextModel.getTextIfNullDefault(date, hash). A failed lookup is
// reported as the unlabeled marker; the error is logged, not returned, as the
// Java model swallows it after logging inside TextLoader.
func (s *Server) text(ctx context.Context, date, textType string, hash int32) string {
	if hash == 0 {
		return ""
	}
	if t, ok := s.texts.get(textType, hash); ok && strings.TrimSpace(t) != "" {
		return t
	}
	param := newMap(entry("date", textValue(date)), entry("type", textValue(textType)),
		entry("hash", listValue([]protocol.Value{decimalValue(int64(hash))})))
	err := s.call(ctx, "GET_TEXT_100", param, func(it Item) error {
		m, ok := it.Pack.(*protocol.MapPack)
		if !ok {
			return nil
		}
		for _, e := range m.Entries {
			if e.Value.Type != protocol.ValueText || e.Value.Text == "" {
				continue
			}
			key, err := parseHexa32(e.Key)
			if err != nil {
				continue
			}
			s.texts.put(textType, int32(key), e.Value.Text)
		}
		return nil
	})
	if err != nil {
		s.logf("webapp GET_TEXT_100 %s %d: %v", textType, hash, err)
	}
	if t, ok := s.texts.get(textType, hash); ok && strings.TrimSpace(t) != "" {
		return t
	}
	return unlabeled(textType, hash)
}

// TextKey names one text dictionary entry.
type textKey struct {
	Type string
	Hash int32
}

// loadTexts is TextLoader.loadAll: one GET_TEXT_ANY_TYPE for uncached keys
// (hash 0 skipped), filling the per-type caches. Callers then use text().
func (s *Server) loadTexts(ctx context.Context, keys []textKey) error {
	var types, hashes []protocol.Value
	seen := map[textKey]bool{}
	for _, k := range keys {
		if k.Hash == 0 || seen[k] {
			continue
		}
		seen[k] = true
		if _, ok := s.texts.get(k.Type, k.Hash); ok {
			continue
		}
		types = append(types, textValue(k.Type))
		hashes = append(hashes, decimalValue(int64(k.Hash)))
		if len(hashes) == maxTextBatch {
			if err := s.loadTextBatch(ctx, types, hashes); err != nil {
				return err
			}
			types, hashes = nil, nil
		}
	}
	if len(hashes) == 0 {
		return nil
	}
	return s.loadTextBatch(ctx, types, hashes)
}

func (s *Server) loadTextBatch(ctx context.Context, types, hashes []protocol.Value) error {
	param := newMap(entry("type", listValue(types)), entry("hash", listValue(hashes)))
	return s.call(ctx, "GET_TEXT_ANY_TYPE", param, func(it Item) error {
		if t, ok := it.Pack.(*protocol.TextPack); ok && t.Text != "" {
			s.texts.put(t.XType, t.Hash, t.Text)
		}
		return nil
	})
}
