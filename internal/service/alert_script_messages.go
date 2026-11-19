// AlertScriptLoadMessageLoopCache: Copyright 2015 Scouter authors,
// Apache-2.0. Independent implementation of pinned source dee47fb5.
package service

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/snowvil/scouter-go/internal/transport"
)

const (
	alertRuleSuffix = ".alert"
	alertConfSuffix = ".conf"
	// AlertScriptLoadMessageCache = AlertScriptLoadMessageLoopCache(50).
	alertScriptMessageCapacity = 50
	// alertScriptUnsupported prefixes every Go message: rules are stored, but
	// the Java AlertRuleLoader compiler (javassist) and RealCounter runtime
	// are not implemented.
	alertScriptUnsupported = "Go server: Java alert script compile/execute unsupported"
)

// AlertScriptLoadMessages is AlertScriptLoadMessageLoopCache.get's CacheOut:
// the current cursor and the messages after the caller's cursor.
type AlertScriptLoadMessages struct {
	Loop     int64
	Index    int32
	Messages []string
}

type alertRuleStamp struct {
	modified int64
	size     int64
}

// alertScriptMessages is the 50-slot ring with Java loop/index cursor
// semantics plus the rule-file snapshot used for change messages.
type alertScriptMessages struct {
	mu      sync.Mutex
	queue   [alertScriptMessageCapacity]string
	present [alertScriptMessageCapacity]bool
	index   int32
	loop    int64
	rules   map[string]alertRuleStamp
}

func newAlertScriptMessages() *alertScriptMessages {
	c := &alertScriptMessages{rules: map[string]alertRuleStamp{}}
	c.putLocked(alertScriptUnsupported + "; .alert/.conf files are saved and listed only")
	return c
}

func (c *alertScriptMessages) putLocked(message string) {
	c.queue[c.index] = message
	c.present[c.index] = true
	c.index++
	if c.index >= alertScriptMessageCapacity {
		c.loop++
		c.index = 0
	}
}

// get is AlertScriptLoadMessageLoopCache.get. A negative index with the
// current loop makes Java index the array out of bounds (no response); it
// is an explicit error here.
func (c *alertScriptMessages) get(lastLoop int64, lastIndex int32) (AlertScriptLoadMessages, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	end, endLoop := int(c.index), c.loop
	start := int(lastIndex)
	out := AlertScriptLoadMessages{Loop: endLoop, Index: c.index, Messages: []string{}}
	copyRange := func(from, to int) {
		for i := from; i < to; i++ {
			if c.present[i] {
				out.Messages = append(out.Messages, c.queue[i])
			}
		}
	}
	switch endLoop - lastLoop {
	case 0:
		if start < end {
			if start < 0 {
				return AlertScriptLoadMessages{}, badParam("index")
			}
			copyRange(start, end)
		}
	case 1:
		if start <= end {
			copyRange(end, alertScriptMessageCapacity)
			copyRange(0, end)
		} else {
			copyRange(start, alertScriptMessageCapacity)
			copyRange(0, end)
		}
	default:
		copyRange(end, alertScriptMessageCapacity)
		copyRange(0, end)
	}
	return out, nil
}

// saved records a successful save and refreshes the snapshot entry of a
// rule file so the next scan does not report it again.
func (c *alertScriptMessages) saved(dir, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasSuffix(name, alertRuleSuffix) {
		c.putLocked(alertScriptUnsupported + "; " + name + " saved, not compiled")
		if stamp, ok := alertRuleFileStamp(dir, name); ok {
			c.rules[name] = stamp
		}
		return
	}
	c.putLocked(alertScriptUnsupported + "; " + name + " saved, not applied")
}

// scan replaces AlertRuleLoader.checkModified/checkNewRule: it reports rule
// files that appeared, changed or disappeared since the previous scan.
// Symlinks and non-regular entries are ignored. The bounded directory read
// runs under the lock so a concurrent save cannot be reported twice.
func (c *alertScriptMessages) scan(dir string, maxEntries int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, err := alertRuleFiles(dir, maxEntries)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(current))
	for name := range current {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		old, known := c.rules[name]
		switch {
		case !known:
			c.putLocked(alertScriptUnsupported + "; " + name + " detected, not compiled")
		case old != current[name]:
			c.putLocked(alertScriptUnsupported + "; " + name + " modified, not compiled")
		}
	}
	removed := make([]string, 0)
	for name := range c.rules {
		if _, ok := current[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)
	for _, name := range removed {
		c.putLocked(alertScriptUnsupported + "; " + name + " removed")
	}
	c.rules = current
	return nil
}

func alertRuleFiles(dir string, maxEntries int) (map[string]alertRuleStamp, error) {
	out := map[string]alertRuleStamp{}
	root, err := os.OpenRoot(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(maxEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > maxEntries {
		return nil, transport.ErrLimit
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), alertRuleSuffix) || !e.Type().IsRegular() {
			continue
		}
		info, err := root.Lstat(e.Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.Mode().IsRegular() {
			out[e.Name()] = alertRuleStamp{info.ModTime().UnixNano(), info.Size()}
		}
	}
	return out, nil
}

func alertRuleFileStamp(dir, name string) (alertRuleStamp, bool) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return alertRuleStamp{}, false
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return alertRuleStamp{}, false
	}
	return alertRuleStamp{info.ModTime().UnixNano(), info.Size()}, true
}
