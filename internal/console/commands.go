package console

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// helpLines is Help.help verbatim (original spelling, trailing space kept).
var helpLines = []string{
	"\thelp = Help",
	"\tquit = Quit",
	"\tobjtype = ObjType List",
	"\tobject = Object List",
	"\tcounter [objType] = counter list for the objType",
	"\tvisitor [objType] = today visit users for the objType",
	"\tdashboard [objType]  = javaee's dashboard",
	"\tdashboard [objType]  [Counter] = host's dashdoard",
	"\trealtime [objType]  TPS [SUM/AVG] = realtime counter view",
	"\txlog [objType]",
	"\txlist [objType] [Time=10:12:23.123] [Count=20000] [minElapsed=0]",
	"\ttagcnt group",
	"\ttagcnt tag [group]",
	"\ttagcnt top100 [objType] [group] [tag] ",
	"\ttagcnt data [objType] [group] [tag] [inx]",
	"\tdebug queue",
}

// Words is Help.words(): the Tab completion candidates.
var Words = []string{"help", "quit", "objtype", "object", "counter", "realtime", "tagcnt", "dashboard"}

// tagGroups/tagNames are TagCountConfig.getTagGroups/getTagNames: the
// StringKeyLinkedMap registration order and the pinned StringSet order
// (the same lists internal/service answers TAGCNT_DIV/TAG_NAMES with).
var tagGroups = []string{"service", "alert"}

func tagNames(group string) []string {
	switch group {
	case "service":
		return []string{"apitime-sum", "service-kbyte-sum", "@total", "city", "sqlcount-sum", "service-time-sum", "service", "nation", "service-error-sum", "sqltime-sum", "referer", "error", "login", "desc", "apicount-sum", "group", "object", "user-agent"}
	case "alert":
		return []string{"@total", "title", "level", "object"}
	}
	return nil
}

func (c *Console) help() {
	for _, l := range helpLines {
		c.println(l)
	}
}

// objType is ProcessMain.objType: distinct objTypes of every in-memory
// object (dead included), String order.
func (c *Console) objType(ctx context.Context) {
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		c.queryFailed(ctx, "objtype", err)
		return
	}
	set := map[string]bool{}
	var types []string
	for _, o := range objs {
		if !set[o.Type] {
			set[o.Type] = true
			types = append(types, o.Type)
		}
	}
	sort.Slice(types, func(i, j int) bool { return javaLess(types[i], types[j]) })
	for _, t := range types {
		c.println("\t" + c.green(t))
	}
}

// objectList is ProcessMain.objectList: alive objects; the second token
// filters on the printed line.
func (c *Console) objectList(ctx context.Context, cmd string) {
	tokens, _ := tokenizer(cmd, " ,")
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		c.queryFailed(ctx, "object", err)
		return
	}
	for _, o := range objs {
		if !o.Alive {
			continue
		}
		text := "\t" + rpad(o.Type, 20) + "\t" + rpad(o.Name, 30) + "\t" + "alive"
		if len(tokens) < 2 || strings.Contains(text, tokens[1]) {
			c.println(text)
		}
	}
}

// counterList is ProcessMain.counterList (getAllCounterList, sorted).
func (c *Console) counterList(cmd string) {
	tokens, _ := tokenizer(cmd, " ,")
	if len(tokens) < 2 {
		return
	}
	t, ok := c.cfg.Source.ObjectType(tokens[1])
	if !ok {
		return
	}
	if !t.HasFamily {
		// Java: ObjectType.listCounters() returns null and the loop NPEs.
		c.errorf("counter: object type %s has no family", tokens[1])
		return
	}
	names := append([]string(nil), t.Counters...)
	sort.SliceStable(names, func(i, j int) bool { return javaLess(names[i], names[j]) })
	for _, n := range names {
		c.println("\t" + c.green(n))
	}
}

// dashboard is Dashboard.process.
func (c *Console) dashboard(ctx context.Context, arg string) {
	name := firstWord(javaTrim(arg), " ")
	t, ok := c.cfg.Source.ObjectType(name)
	if !ok {
		c.println("no object for " + javaTrim(arg))
		return
	}
	counter := lastWord(arg, " ")
	c.repeat(ctx, func(ctx context.Context) bool {
		if !t.HasFamily {
			c.errorf("dashboard: object type %s has no family", name)
			return false
		}
		hashes, names, err := c.liveOfType(ctx, name)
		if err != nil {
			c.queryFailed(ctx, "dashboard", err)
			return true
		}
		switch t.Family {
		case "javaee":
			c.dashJavaee(ctx, hashes)
		case "host":
			c.dashHost(ctx, hashes, names, counter)
		default:
			// Java's `return` here is a swallowed non-local return that would
			// repeat the line every 2s (D-C3); Go prints it once and stops.
			c.println("not supported : " + name)
			return false
		}
		return true
	})
}

// liveOfType is AgentManager.getLiveObjHashList(objType) with objNames.
func (c *Console) liveOfType(ctx context.Context, objType string) ([]int32, map[int32]string, error) {
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		return nil, nil, err
	}
	var hashes []int32
	names := map[int32]string{}
	for _, o := range objs {
		if o.Type == objType && o.Alive {
			hashes = append(hashes, o.Hash)
		}
		names[o.Hash] = o.Name
	}
	return hashes, names, nil
}

// counterStat is Dashboard.getCnt before f: the sum and count of the
// objects that have the realtime value. ok=false reports the error line.
func (c *Console) counterStat(ctx context.Context, hashes []int32, counter string) (sum float64, cnt int, ok bool) {
	for _, h := range hashes {
		v, found, err := c.cfg.Source.RealtimeCounter(ctx, h, counter)
		if err != nil {
			c.queryFailed(ctx, "dashboard", err)
			return 0, 0, false
		}
		if !found {
			continue
		}
		n, isNumber := valueNumber(v)
		if !isNumber {
			c.errorf("dashboard: counter %s value is not a number (%s)", counter, valueString(v))
			return 0, 0, false
		}
		sum += n
		cnt++
	}
	return sum, cnt, true
}

func (c *Console) dashJavaee(ctx context.Context, hashes []int32) {
	cell := func(counter string, f func(sum float64, cnt int) float64) (string, bool) {
		sum, cnt, ok := c.counterStat(ctx, hashes, counter)
		if !ok {
			return "", false
		}
		v := 0.0
		if cnt != 0 {
			v = f(sum, cnt)
		}
		return leftPad(decimalFormat(v, 0), 5), true
	}
	count := func(_ float64, cnt int) float64 { return float64(cnt) }
	total := func(sum float64, _ int) float64 { return sum }
	avg := func(sum float64, cnt int) float64 { return sum / float64(cnt) }
	var cells [5]string
	for i, spec := range []struct {
		counter string
		f       func(float64, int) float64
	}{{"ActiveService", count}, {"ActiveService", total}, {"TPS", total}, {"ElapsedTime", avg}, {"GcCount", total}} {
		s, ok := cell(spec.counter, spec.f)
		if !ok {
			return
		}
		cells[i] = s
	}
	tm := logTime(c.nowMillis(), c.cfg.Location)
	if c.lines == 0 {
		c.println("            " + "  INST" + "   ACT" + "   TPS" + " RTIME" + " GCCNT")
	}
	c.nextLine()
	c.println(tm + " " + strings.Join(cells[:], " "))
}

func (c *Console) nextLine() {
	if c.lines == 10 {
		c.lines = 0
	} else {
		c.lines++
	}
}

func (c *Console) dashHost(ctx context.Context, hashes []int32, names map[int32]string, counter string) {
	if counter == "" {
		return
	}
	values := map[string]float64{}
	maxHead := 5
	for _, h := range hashes {
		v, found, err := c.cfg.Source.RealtimeCounter(ctx, h, counter)
		if err != nil {
			c.queryFailed(ctx, "dashboard", err)
			return
		}
		if !found {
			continue
		}
		name, known := names[h]
		if !known || name == "" {
			// Java: getAgentName(...).substring(1) on null or "".
			c.errorf("dashboard: object %d has no name", h)
			return
		}
		_, size := firstRune(name)
		name = name[size:]
		n, isNumber := valueNumber(v)
		if !isNumber {
			c.errorf("dashboard: counter %s value is not a number (%s)", counter, valueString(v))
			return
		}
		values[name] = n
		if l := javaLen(name); l > maxHead {
			maxHead = l
		}
	}
	head := make([]string, 0, len(values))
	for k := range values {
		head = append(head, k)
	}
	sort.Slice(head, func(i, j int) bool { return javaLess(head[i], head[j]) })
	if c.lines == 0 || len(head) != c.hostColumns {
		c.hostColumns = len(head)
		var b strings.Builder
		b.WriteString("            ")
		for _, k := range head {
			b.WriteString(" " + leftPad(k, maxHead))
		}
		c.println(b.String())
	}
	c.nextLine()
	var b strings.Builder
	b.WriteString(logTime(c.nowMillis(), c.cfg.Location))
	for _, k := range head {
		b.WriteString(" " + leftPad(decimalFormat(values[k], 0), maxHead))
	}
	c.println(b.String())
}

func firstRune(s string) (rune, int) {
	for i, r := range s {
		if i == 0 {
			return r, len(string(r))
		}
	}
	return 0, 0
}

// filter is AgentManager.filter: "*" is every alive object, a known objType
// is every object of the type (dead included), otherwise objName contains.
func (c *Console) filter(ctx context.Context, word string) ([]int32, map[int32]string, error) {
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		return nil, nil, err
	}
	names := map[int32]string{}
	isType := false
	for _, o := range objs {
		names[o.Hash] = o.Name
		if o.Type == word {
			isType = true
		}
	}
	var out []int32
	for _, o := range objs {
		switch {
		case word == "*":
			if o.Alive {
				out = append(out, o.Hash)
			}
		case isType:
			if o.Type == word {
				out = append(out, o.Hash)
			}
		case strings.Contains(o.Name, word):
			out = append(out, o.Hash)
		}
	}
	return out, names, nil
}

// agentName is AgentManager.getAgentName: "null" for an unknown object.
func agentName(names map[int32]string, h int32) string {
	if n, ok := names[h]; ok {
		return n
	}
	return "null"
}

// realtime is REALTIME.process.
func (c *Console) realtime(ctx context.Context, arg string) {
	tokens, ok := tokenizer(arg, " ")
	if !ok {
		c.errorf("realtime: usage: realtime <objType|name|*> <counter> [SUM|AVG]")
		return
	}
	if len(tokens) < 2 {
		return
	}
	counter := tokens[1]
	mode := ""
	if len(tokens) > 2 {
		mode = tokens[2]
	}
	c.repeat(ctx, func(ctx context.Context) bool {
		hashes, names, err := c.filter(ctx, tokens[0])
		if err != nil {
			c.queryFailed(ctx, "realtime", err)
			return true
		}
		tm := logTime(c.nowMillis(), c.cfg.Location)
		if len(hashes) == 0 {
			c.println(tm + " ...")
			return true
		}
		if mode == "" {
			for _, h := range hashes {
				v, found, err := c.cfg.Source.RealtimeCounter(ctx, h, counter)
				if err != nil {
					c.queryFailed(ctx, "realtime", err)
					return true
				}
				text := "null"
				if found {
					text = valueFormat(v)
				}
				c.println(tm + " " + c.blue(agentName(names, h)) + " " + text)
			}
			return true
		}
		sum := 0.0
		for _, h := range hashes {
			v, found, err := c.cfg.Source.RealtimeCounter(ctx, h, counter)
			if err != nil {
				c.queryFailed(ctx, "realtime", err)
				return true
			}
			if !found {
				// D-C8: Java NPEs and skips this cycle.
				c.errorf("realtime: %s has no %s value", agentName(names, h), counter)
				return true
			}
			n, isNumber := valueNumber(v)
			if !isNumber {
				c.errorf("realtime: counter %s value is not a number (%s)", counter, valueString(v))
				return true
			}
			sum += n
		}
		if strings.ToUpper(mode) == "AVG" {
			sum /= float64(len(hashes))
		}
		c.println(tm + " " + counter + " " + decimalFormat(sum, 0))
		return true
	})
}

// xlog is XLOG.process. The loop cursor persists across runs like the Java
// object fields.
func (c *Console) xlog(ctx context.Context, arg string) {
	tokens, ok := tokenizer(arg, " ")
	if !ok || len(tokens) == 0 {
		c.errorf("xlog: usage: xlog <objType|name|*> [maxSec]")
		return
	}
	maxTime := int32(10)
	if len(tokens) > 1 {
		maxTime = javaInt(tokens[1])
	}
	c.repeat(ctx, func(ctx context.Context) bool {
		hashes, _, err := c.filter(ctx, tokens[0])
		if err != nil {
			c.queryFailed(ctx, "xlog", err)
			return true
		}
		if len(hashes) == 0 {
			c.println(logTime(c.nowMillis(), c.cfg.Location) + " ...")
			return true
		}
		loop, index, records, err := c.cfg.Source.XLogsSince(ctx, c.xlogLoop, c.xlogIndex, hashes)
		if err != nil {
			c.queryFailed(ctx, "xlog", err)
			return true
		}
		c.xlogLoop, c.xlogIndex = loop, index
		var table [60]byte
		for i := range table {
			table[i] = '_'
		}
		tm := ""
		divisor := maxTime * 1000 // Java int arithmetic, wraps like the original
		for _, p := range records {
			if divisor == 0 {
				c.errorf("xlog: / by zero (maxSec %d)", maxTime)
				return true
			}
			bk := p.Elapsed * 60 / divisor
			if bk >= 60 {
				bk = 59
			}
			if bk < 0 {
				c.errorf("xlog: elapsed %d is outside the 60 buckets", p.Elapsed)
				return true
			}
			if table[bk] != 'E' {
				if p.Error != 0 {
					table[bk] = 'E'
				} else {
					table[bk] = '#'
				}
			}
			if tm == "" {
				tm = logTime(p.EndTime, c.cfg.Location)
			}
		}
		if tm == "" {
			tm = logTime(c.nowMillis(), c.cfg.Location)
		}
		c.println(tm + " " + c.green(string(table[:])) + " " + strconv.Itoa(int(maxTime)) + "sec " + longFormat(int64(len(records))))
		return true
	})
}

// xlist is XLIST.process. Like Java it ignores the objType filter and
// minElapsed (D-C5); only the count limit applies.
func (c *Console) xlist(ctx context.Context, arg string) {
	tokens, ok := tokenizer(arg, " ")
	if !ok || len(tokens) < 2 {
		c.errorf("xlist: usage: xlist <objType> <HH:mm:ss.SSS> [count] [minElapsed]")
		return
	}
	now := c.nowMillis()
	today := scoutertime.Date(now, c.cfg.Location)
	from, ok := parseLogTime(today, tokens[1], c.cfg.Location)
	if !ok {
		// Java SimpleDateFormat fails, getTime returns 0 and nothing prints.
		c.errorf("xlist: invalid time %q (HH:mm:ss.SSS)", tokens[1])
		return
	}
	maxCount := int32(10000)
	if len(tokens) > 2 {
		maxCount = javaInt(tokens[2])
	}
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		c.queryFailed(ctx, "xlist", err)
		return
	}
	names := map[int32]string{}
	for _, o := range objs {
		names[o.Hash] = o.Name
	}
	records, err := c.cfg.Source.XLogsByTime(ctx, today, from, from+2000)
	if err != nil {
		c.queryFailed(ctx, "xlist", err)
		return
	}
	text := func(date, div string, hash int32) (string, bool) {
		s, found, err := c.cfg.Source.Text(ctx, date, div, hash)
		if err != nil {
			c.queryFailed(ctx, "xlist", err)
			return "", false
		}
		if !found {
			return "null", true
		}
		return s, true
	}
	for i, x := range records {
		if int32(i) >= maxCount {
			break
		}
		date := scoutertime.Date(x.EndTime, c.cfg.Location)
		service, ok := text(date, "service", x.Service)
		if !ok {
			return
		}
		var b strings.Builder
		b.WriteString(leftPad(longFormat(int64(i+1)), 5) + " ")
		b.WriteString(logTime(x.EndTime, c.cfg.Location) + " ")
		b.WriteString(hexa32(x.TxID) + " ")
		b.WriteString(agentName(names, x.ObjHash) + " ")
		b.WriteString(ipString(x.IPAddress) + " ")
		b.WriteString(service + " ")
		b.WriteString(longFormat(int64(x.Elapsed)) + "ms ")
		if x.SQLCount > 0 {
			b.WriteString("sql:" + longFormat(int64(x.SQLCount)) + "/" + longFormat(int64(x.SQLTime)) + "ms ")
		}
		if x.APICallCount > 0 {
			b.WriteString("api:" + longFormat(int64(x.APICallCount)) + "/" + longFormat(int64(x.APICallTime)) + "ms ")
		}
		if x.UserAgent != 0 {
			ua, ok := text(date, "ua", x.UserAgent)
			if !ok {
				return
			}
			b.WriteString(ua + " ")
		}
		c.println(b.String())
	}
}

// parseLogTime is DateUtil.getTime(yyyyMMdd + s, "yyyyMMddHH:mm:ss.SSS")
// with the lenient SimpleDateFormat: HH is two digits (it abuts dd), the
// other fields take any digit run, out-of-range fields roll over and text
// after the milliseconds is ignored.
func parseLogTime(date, s string, loc *time.Location) (int64, bool) {
	day, err := time.ParseInLocation("20060102", date, loc)
	if err != nil {
		return 0, false
	}
	if len(s) < 2 || !isDigit(s[0]) || !isDigit(s[1]) {
		return 0, false
	}
	hour := int(s[0]-'0')*10 + int(s[1]-'0')
	rest := s[2:]
	var fields [3]int
	for i, sep := range []byte{':', ':', '.'} {
		if rest == "" || rest[0] != sep {
			return 0, false
		}
		rest = rest[1:]
		n := 0
		for n < len(rest) && isDigit(rest[n]) {
			n++
		}
		if n == 0 || n > 9 {
			return 0, false
		}
		fields[i], _ = strconv.Atoi(rest[:n])
		rest = rest[n:]
	}
	t := time.Date(day.Year(), day.Month(), day.Day(), hour, fields[0], fields[1], 0, loc)
	return t.UnixMilli() + int64(fields[2]), true
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// tagcnt is TAGCNT.process.
func (c *Console) tagcnt(ctx context.Context, arg string) {
	tokens, ok := tokenizer(arg, " ")
	if !ok {
		c.errorf("tagcnt: usage: tagcnt group | tag <group> | top100 <objType> <group> <tag> | data <objType> <group> <tag> [inx]")
		return
	}
	if len(tokens) < 1 {
		return
	}
	switch {
	case tokens[0] == "group":
		for _, g := range tagGroups {
			c.println("\t" + g)
		}
	case len(tokens) > 1 && tokens[0] == "tag":
		for _, n := range tagNames(tokens[1]) {
			c.println("\t" + n)
		}
	case len(tokens) > 3 && tokens[0] == "top100":
		c.top100(ctx, tokens[1], tokens[2], tokens[3])
	case len(tokens) > 3 && tokens[0] == "data":
		x := -1
		if len(tokens) > 4 {
			v, err := strconv.ParseInt(tokens[4], 10, 32)
			if err != nil {
				c.errorf("tagcnt: invalid index %q", tokens[4])
				return
			}
			x = int(v)
		}
		c.tagData(ctx, tokens[1], tokens[2], tokens[3], x)
	}
}

func (c *Console) top100(ctx context.Context, objType, group, tag string) {
	date := scoutertime.Date(c.nowMillis(), c.cfg.Location)
	values, err := c.cfg.Source.TagValues(ctx, date, objType, group, tag)
	if err != nil {
		c.queryFailed(ctx, "tagcnt", err)
		return
	}
	for i, vc := range values {
		inx := strconv.Itoa(i + 1)
		if vc.Value.Type == protocol.ValueTextHash {
			// D-C6: Java resolves every TEXT_HASH tag value in the error dictionary.
			text, found, err := c.cfg.Source.Text(ctx, date, "error", int32(vc.Value.Int))
			if err != nil {
				c.queryFailed(ctx, "tagcnt", err)
				return
			}
			if !found {
				text = "null"
			}
			c.println("\t" + inx + " " + text + "(" + valueString(vc.Value) + ") \t= " + javaDoubleString(vc.Count))
		} else {
			c.println("\t" + inx + " " + valueString(vc.Value) + " \t= " + javaDoubleString(vc.Count))
		}
	}
}

func (c *Console) tagData(ctx context.Context, objType, group, tag string, x int) {
	if x == 0 {
		return
	}
	date := scoutertime.Date(c.nowMillis(), c.cfg.Location)
	values, err := c.cfg.Source.TagValues(ctx, date, objType, group, tag)
	if err != nil {
		c.queryFailed(ctx, "tagcnt", err)
		return
	}
	for i, vc := range values {
		if i+1 != x && x >= 0 {
			continue
		}
		series, err := c.cfg.Source.TagValueSeries(ctx, date, objType, group, tag, vc.Value)
		if err != nil {
			c.queryFailed(ctx, "tagcnt", err)
			return
		}
		if len(series) < 1440 {
			// Java: getTagValueCountData returns null and printTable NPEs.
			c.errorf("tagcnt: no minute data for %s", valueString(vc.Value))
			return
		}
		for h := 0; h < 24; h++ {
			var b strings.Builder
			b.WriteString(leftPad("H"+strconv.Itoa(h), 5) + " ")
			for m := 0; m < 60; m++ {
				if m > 0 {
					b.WriteString(", ")
				}
				b.WriteString(decimalFormat(float64(series[h*60+m]), 1))
			}
			c.println(b.String())
		}
		return
	}
}

// visitor is VISITOR.process.
func (c *Console) visitor(ctx context.Context, arg string) {
	word := firstWord(arg, " ")
	objs, err := c.cfg.Source.Objects(ctx)
	if err != nil {
		c.queryFailed(ctx, "visitor", err)
		return
	}
	for _, o := range objs {
		if o.Type == word {
			v, err := c.cfg.Source.VisitorObjType(ctx, word)
			if err != nil {
				c.queryFailed(ctx, "visitor", err)
				return
			}
			c.println("\t" + longFormat(v))
			return
		}
	}
	hashes, names, err := c.filter(ctx, word)
	if err != nil {
		c.queryFailed(ctx, "visitor", err)
		return
	}
	for _, h := range hashes {
		v, err := c.cfg.Source.VisitorObject(ctx, h)
		if err != nil {
			c.queryFailed(ctx, "visitor", err)
			return
		}
		if v > 0 {
			c.println("\t" + agentName(names, h) + " = " + longFormat(v))
		}
	}
}

// debug is DEBUG.process: `debug queue` prints the server statistics as
// sorted key=value lines plus the empty line Java's println adds.
func (c *Console) debug(arg string) {
	switch arg {
	case "queue":
		stats := c.cfg.Source.Stats()
		sort.Slice(stats, func(i, j int) bool { return javaLess(stats[i].Key, stats[j].Key) })
		var b strings.Builder
		for _, s := range stats {
			b.WriteString(s.Key + "=" + strconv.FormatInt(s.Value, 10) + "\n")
		}
		c.println(b.String())
	case "_":
	default:
		// D-C4: Java throws scala.MatchError.
		c.errorf("debug: unknown option %q (debug queue)", arg)
	}
}
