package telegraf

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

// File is the scouter-telegraf.xml model (TgConfig/TgmConfig/TgCounterMapping)
// as JAXB unmarshals it. Absent elements keep the Java field defaults; an
// empty list wrapper (<objTypePrependTags/>) replaces the default with an empty
// list. Nullable Java String fields that have no default are *string.
type File struct {
	Enabled                             bool
	DebugEnabled                        bool
	DeltaCounterNormalizeDefault        bool
	DeltaCounterNormalizeDefaultSeconds int
	ObjectDeadtimeMs                    int
	Measurements                        []MeasurementFile
}

// MeasurementFile is TgmConfig.
type MeasurementFile struct {
	MeasurementName     *string
	Enabled             bool
	DebugEnabled        bool
	ObjFamilyBase       string
	ObjFamilyAppendTags []string
	ObjTypeBase         string
	ObjTypePrependTags  []string
	ObjTypeAppendTags   []string
	ObjTypeIcon         string
	ObjNameBase         string
	ObjNameAppendTags   []string
	HostTag             string
	HostMappings        []HostMapping
	TagFilters          []TagFilter
	CounterMappings     []CounterMapping
}

// HostMapping is TgmConfig.HostMapping.
type HostMapping struct{ Telegraf, Scouter *string }

// TagFilter is TgmConfig.TagFilter. Match is nil when no <match> element was
// present (Java null).
type TagFilter struct {
	Tag   *string
	Match []string
}

// CounterMapping is TgCounterMapping. DeltaType "" is the Java null that JAXB
// leaves for an unknown enum literal; it behaves like NONE.
type CounterMapping struct {
	TgFieldName, CounterName, DisplayName *string
	Unit                                  string
	Totalizable                           bool
	NormalizeSeconds                      int
	DeltaType                             DeltaType
}

// DefaultTypePrependTag is TgmConfig.DEFAULT_OBJ_TYPE_PREPEND_TAG.
const DefaultTypePrependTag = "scouter_obj_type_prefix"

// DefaultFile is new TgConfig(): asDefault() with no measurements.
func DefaultFile() *File {
	return &File{Enabled: true, DeltaCounterNormalizeDefault: true, DeltaCounterNormalizeDefaultSeconds: 30, ObjectDeadtimeMs: 35000}
}

// NewMeasurementFile is new TgmConfig().
func NewMeasurementFile() MeasurementFile {
	return MeasurementFile{Enabled: true, ObjFamilyAppendTags: []string{}, ObjTypePrependTags: []string{DefaultTypePrependTag}, ObjTypeAppendTags: []string{},
		ObjNameAppendTags: []string{}, HostTag: "host", HostMappings: []HostMapping{}, TagFilters: []TagFilter{}, CounterMappings: []CounterMapping{}}
}

// NewCounterMapping is new TgCounterMapping().
func NewCounterMapping() CounterMapping {
	return CounterMapping{NormalizeSeconds: 30, DeltaType: DeltaNone}
}

// MaxFileBytes bounds the XML accepted by ParseFile.
const MaxFileBytes = 4 << 20

// The xml* list types are JAXB @XmlElementWrapper lists: every occurrence of
// the wrapper element starts a new list (JAXB RI clears the collection when
// the wrapper starts), so a repeated wrapper replaces the earlier one and an
// empty wrapper yields an empty, non-nil list.
type xmlTags struct{ items []string }

func (l *xmlTags) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	l.items, err = decodeList[string](d, "tag")
	return err
}

type xmlHostMapping struct {
	Telegraf *string `xml:"telegraf"`
	Scouter  *string `xml:"scouter"`
}

type xmlHostMappings struct{ items []xmlHostMapping }

func (l *xmlHostMappings) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	l.items, err = decodeList[xmlHostMapping](d, "hostMapping")
	return err
}

type xmlTagFilter struct {
	Tag   *string  `xml:"tag"`
	Match []string `xml:"match"`
}

type xmlTagFilters struct{ items []xmlTagFilter }

func (l *xmlTagFilters) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	l.items, err = decodeList[xmlTagFilter](d, "tagFilter")
	return err
}

type xmlCounterMapping struct {
	TgFieldName      *string `xml:"tgFieldName"`
	CounterName      *string `xml:"counterName"`
	DisplayName      *string `xml:"displayName"`
	Unit             *string `xml:"unit"`
	Totalizable      *string `xml:"totalizable"`
	NormalizeSeconds *string `xml:"normalizeSeconds"`
	DeltaType        *string `xml:"deltaType"`
}

type xmlCounterMappings struct{ items []xmlCounterMapping }

func (l *xmlCounterMappings) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	l.items, err = decodeList[xmlCounterMapping](d, "counterMapping")
	return err
}

type xmlMeasurement struct {
	MeasurementName     *string             `xml:"measurementName"`
	Enabled             *string             `xml:"enabled"`
	DebugEnabled        *string             `xml:"debugEnabled"`
	ObjFamilyBase       *string             `xml:"objFamilyBase"`
	ObjFamilyAppendTags *xmlTags            `xml:"objFamilyAppendTags"`
	ObjTypeBase         *string             `xml:"objTypeBase"`
	ObjTypePrependTags  *xmlTags            `xml:"objTypePrependTags"`
	ObjTypeAppendTags   *xmlTags            `xml:"objTypeAppendTags"`
	ObjTypeIcon         *string             `xml:"objTypeIcon"`
	ObjNameBase         *string             `xml:"objNameBase"`
	ObjNameAppendTags   *xmlTags            `xml:"objNameAppendTags"`
	HostTag             *string             `xml:"hostTag"`
	HostMappings        *xmlHostMappings    `xml:"hostMappings"`
	TagFilters          *xmlTagFilters      `xml:"tagFilters"`
	CounterMappings     *xmlCounterMappings `xml:"counterMappings"`
}

type xmlMeasurements struct{ items []xmlMeasurement }

func (l *xmlMeasurements) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	l.items, err = decodeList[xmlMeasurement](d, "measurement")
	return err
}

type xmlFile struct {
	XMLName                             xml.Name         `xml:"TelegrafConfig"`
	Enabled                             *string          `xml:"enabled"`
	DebugEnabled                        *string          `xml:"debugEnabled"`
	DeltaCounterNormalizeDefault        *string          `xml:"deltaCounterNormalizeDefault"`
	DeltaCounterNormalizeDefaultSeconds *string          `xml:"deltaCounterNormalizeDefaultSeconds"`
	ObjectDeadtimeMs                    *string          `xml:"objectDeadtimeMs"`
	Measurements                        *xmlMeasurements `xml:"measurements"`
}

// decodeList decodes the children named item of the current element and
// skips anything else.
func decodeList[T any](d *xml.Decoder, item string) ([]T, error) {
	out := []T{}
	for {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local != item {
				if err := d.Skip(); err != nil {
					return nil, err
				}
				continue
			}
			var v T
			if err := d.DecodeElement(&v, &t); err != nil {
				return nil, err
			}
			out = append(out, v)
		case xml.EndElement:
			return out, nil
		}
	}
}

// ParseFile is JAXB unmarshal of TgConfig. A document that is not well formed
// or whose root is not <TelegrafConfig> is an error (Java JAXBException).
// Scalar values follow the JAXB RI converters checked against JDK 8
// (DatatypeConverterImpl): see jaxbBool, jaxbInt and the exact enum match.
// Non-canonical values are converted the same way and reported in warnings.
func ParseFile(data []byte) (*File, []string, error) {
	if len(data) > MaxFileBytes {
		return nil, nil, fmt.Errorf("telegraf config: %d bytes exceeds %d", len(data), MaxFileBytes)
	}
	var doc xmlFile
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, nil, fmt.Errorf("telegraf config: %w", err)
	}
	var warnings []string
	f := DefaultFile()
	setBool(&f.Enabled, doc.Enabled, "enabled", &warnings)
	setBool(&f.DebugEnabled, doc.DebugEnabled, "debugEnabled", &warnings)
	setBool(&f.DeltaCounterNormalizeDefault, doc.DeltaCounterNormalizeDefault, "deltaCounterNormalizeDefault", &warnings)
	setInt(&f.DeltaCounterNormalizeDefaultSeconds, doc.DeltaCounterNormalizeDefaultSeconds, "deltaCounterNormalizeDefaultSeconds", &warnings)
	setInt(&f.ObjectDeadtimeMs, doc.ObjectDeadtimeMs, "objectDeadtimeMs", &warnings)
	if doc.Measurements != nil {
		f.Measurements = make([]MeasurementFile, 0, len(doc.Measurements.items))
		for _, x := range doc.Measurements.items {
			f.Measurements = append(f.Measurements, x.decode(&warnings))
		}
	}
	return f, warnings, nil
}

func (x xmlMeasurement) decode(warnings *[]string) MeasurementFile {
	m := NewMeasurementFile()
	m.MeasurementName = x.MeasurementName
	setBool(&m.Enabled, x.Enabled, "enabled", warnings)
	setBool(&m.DebugEnabled, x.DebugEnabled, "debugEnabled", warnings)
	setString(&m.ObjFamilyBase, x.ObjFamilyBase)
	setTags(&m.ObjFamilyAppendTags, x.ObjFamilyAppendTags)
	setString(&m.ObjTypeBase, x.ObjTypeBase)
	setTags(&m.ObjTypePrependTags, x.ObjTypePrependTags)
	setTags(&m.ObjTypeAppendTags, x.ObjTypeAppendTags)
	setString(&m.ObjTypeIcon, x.ObjTypeIcon)
	setString(&m.ObjNameBase, x.ObjNameBase)
	setTags(&m.ObjNameAppendTags, x.ObjNameAppendTags)
	setString(&m.HostTag, x.HostTag)
	if x.HostMappings != nil {
		m.HostMappings = []HostMapping{}
		for _, h := range x.HostMappings.items {
			m.HostMappings = append(m.HostMappings, HostMapping{Telegraf: h.Telegraf, Scouter: h.Scouter})
		}
	}
	if x.TagFilters != nil {
		m.TagFilters = []TagFilter{}
		for _, t := range x.TagFilters.items {
			m.TagFilters = append(m.TagFilters, TagFilter{Tag: t.Tag, Match: t.Match})
		}
	}
	if x.CounterMappings != nil {
		m.CounterMappings = []CounterMapping{}
		for _, c := range x.CounterMappings.items {
			cm := NewCounterMapping()
			cm.TgFieldName, cm.CounterName, cm.DisplayName = c.TgFieldName, c.CounterName, c.DisplayName
			setString(&cm.Unit, c.Unit)
			setBool(&cm.Totalizable, c.Totalizable, "totalizable", warnings)
			setInt(&cm.NormalizeSeconds, c.NormalizeSeconds, "normalizeSeconds", warnings)
			if c.DeltaType != nil {
				// JAXB matches the enum literal exactly (no trim, case sensitive).
				switch v := DeltaType(*c.DeltaType); v {
				case DeltaNone, DeltaDelta, DeltaBoth:
					cm.DeltaType = v
				default:
					cm.DeltaType = ""
					*warnings = append(*warnings, fmt.Sprintf("deltaType %q is not NONE/DELTA/BOTH; treated as null", *c.DeltaType))
				}
			}
			m.CounterMappings = append(m.CounterMappings, cm)
		}
	}
	return m
}

func setString(dst *string, v *string) {
	if v != nil {
		*dst = *v
	}
}

func setTags(dst *[]string, v *xmlTags) {
	if v != nil {
		*dst = append([]string{}, v.items...)
	}
}

func setBool(dst *bool, v *string, name string, warnings *[]string) {
	if v == nil {
		return
	}
	*dst = jaxbBool(*v)
	if t := strings.Trim(*v, xmlSpace); t != "true" && t != "false" && t != "1" && t != "0" {
		*warnings = append(*warnings, fmt.Sprintf("%s %q is not a canonical boolean; read as %v", name, *v, *dst))
	}
}

func setInt(dst *int, v *string, name string, warnings *[]string) {
	if v == nil {
		return
	}
	n, ok := jaxbInt(*v)
	*dst = n
	if _, err := strconv.ParseInt(strings.Trim(*v, xmlSpace), 10, 32); err != nil || !ok {
		*warnings = append(*warnings, fmt.Sprintf("%s %q is not a canonical int; read as %d", name, *v, n))
	}
}

const xmlSpace = " \t\r\n"

// jaxbBool is DatatypeConverterImpl._parseBoolean as observed on JDK 8:
// after leading XML whitespace, "1..." and "true..." are true; everything else
// (including "TRUE", "yes", "") is false.
func jaxbBool(s string) bool {
	s = strings.TrimLeft(s, xmlSpace)
	return strings.HasPrefix(s, "1") || strings.HasPrefix(s, "true")
}

// jaxbInt is DatatypeConverterImpl._parseInt as observed on JDK 8: XML
// whitespace anywhere is ignored, '+' is ignored, any '-' makes the result
// negative, digits accumulate with int32 overflow; any other character is a
// conversion error that leaves 0 (ok=false).
func jaxbInt(s string) (int, bool) {
	var r int32
	sign := int32(1)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case strings.IndexByte(xmlSpace, c) >= 0, c == '+':
		case c >= '0' && c <= '9':
			r = r*10 + int32(c-'0')
		case c == '-':
			sign = -1
		default:
			return 0, false
		}
	}
	return int(r * sign), true
}

// Marshal writes the document the way the pinned JAXB marshaller formats it
// (JAXB_FORMATTED_OUTPUT, 4-space indent, standalone header, empty String as
// <x></x>, empty list wrapper as <x/>, null fields omitted). Parsing the
// pinned TgConfig.getSampleContents() and marshalling it reproduces the same
// bytes.
func (f *File) Marshal() []byte {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<TelegrafConfig>\n")
	leaf(&b, 1, "enabled", strconv.FormatBool(f.Enabled))
	leaf(&b, 1, "debugEnabled", strconv.FormatBool(f.DebugEnabled))
	leaf(&b, 1, "deltaCounterNormalizeDefault", strconv.FormatBool(f.DeltaCounterNormalizeDefault))
	leaf(&b, 1, "deltaCounterNormalizeDefaultSeconds", strconv.Itoa(f.DeltaCounterNormalizeDefaultSeconds))
	leaf(&b, 1, "objectDeadtimeMs", strconv.Itoa(f.ObjectDeadtimeMs))
	if len(f.Measurements) == 0 {
		indent(&b, 1)
		b.WriteString("<measurements/>\n")
	} else {
		open(&b, 1, "measurements")
		for _, m := range f.Measurements {
			open(&b, 2, "measurement")
			leafPtr(&b, 3, "measurementName", m.MeasurementName)
			leaf(&b, 3, "enabled", strconv.FormatBool(m.Enabled))
			leaf(&b, 3, "debugEnabled", strconv.FormatBool(m.DebugEnabled))
			leaf(&b, 3, "objFamilyBase", m.ObjFamilyBase)
			tagList(&b, 3, "objFamilyAppendTags", m.ObjFamilyAppendTags)
			leaf(&b, 3, "objTypeBase", m.ObjTypeBase)
			tagList(&b, 3, "objTypePrependTags", m.ObjTypePrependTags)
			tagList(&b, 3, "objTypeAppendTags", m.ObjTypeAppendTags)
			leaf(&b, 3, "objTypeIcon", m.ObjTypeIcon)
			leaf(&b, 3, "objNameBase", m.ObjNameBase)
			tagList(&b, 3, "objNameAppendTags", m.ObjNameAppendTags)
			leaf(&b, 3, "hostTag", m.HostTag)
			if len(m.HostMappings) == 0 {
				empty(&b, 3, "hostMappings")
			} else {
				open(&b, 3, "hostMappings")
				for _, h := range m.HostMappings {
					open(&b, 4, "hostMapping")
					leafPtr(&b, 5, "telegraf", h.Telegraf)
					leafPtr(&b, 5, "scouter", h.Scouter)
					closeTag(&b, 4, "hostMapping")
				}
				closeTag(&b, 3, "hostMappings")
			}
			if len(m.TagFilters) == 0 {
				empty(&b, 3, "tagFilters")
			} else {
				open(&b, 3, "tagFilters")
				for _, t := range m.TagFilters {
					open(&b, 4, "tagFilter")
					leafPtr(&b, 5, "tag", t.Tag)
					for _, v := range t.Match {
						leaf(&b, 5, "match", v)
					}
					closeTag(&b, 4, "tagFilter")
				}
				closeTag(&b, 3, "tagFilters")
			}
			if len(m.CounterMappings) == 0 {
				empty(&b, 3, "counterMappings")
			} else {
				open(&b, 3, "counterMappings")
				for _, c := range m.CounterMappings {
					open(&b, 4, "counterMapping")
					leafPtr(&b, 5, "tgFieldName", c.TgFieldName)
					leafPtr(&b, 5, "counterName", c.CounterName)
					leafPtr(&b, 5, "displayName", c.DisplayName)
					leaf(&b, 5, "unit", c.Unit)
					leaf(&b, 5, "totalizable", strconv.FormatBool(c.Totalizable))
					leaf(&b, 5, "normalizeSeconds", strconv.Itoa(c.NormalizeSeconds))
					if c.DeltaType != "" {
						leaf(&b, 5, "deltaType", string(c.DeltaType))
					}
					closeTag(&b, 4, "counterMapping")
				}
				closeTag(&b, 3, "counterMappings")
			}
			closeTag(&b, 2, "measurement")
		}
		closeTag(&b, 1, "measurements")
	}
	b.WriteString("</TelegrafConfig>\n")
	return []byte(b.String())
}

func indent(b *strings.Builder, depth int) {
	for i := 0; i < depth; i++ {
		b.WriteString("    ")
	}
}

func open(b *strings.Builder, depth int, name string) {
	indent(b, depth)
	b.WriteString("<" + name + ">\n")
}

func closeTag(b *strings.Builder, depth int, name string) {
	indent(b, depth)
	b.WriteString("</" + name + ">\n")
}

func empty(b *strings.Builder, depth int, name string) {
	indent(b, depth)
	b.WriteString("<" + name + "/>\n")
}

func leaf(b *strings.Builder, depth int, name, value string) {
	indent(b, depth)
	b.WriteString("<" + name + ">")
	escapeText(b, value)
	b.WriteString("</" + name + ">\n")
}

func leafPtr(b *strings.Builder, depth int, name string, value *string) {
	if value != nil {
		leaf(b, depth, name, *value)
	}
}

func tagList(b *strings.Builder, depth int, name string, tags []string) {
	if len(tags) == 0 {
		empty(b, depth, name)
		return
	}
	open(b, depth, name)
	for _, t := range tags {
		leaf(b, depth+1, "tag", t)
	}
	closeTag(b, depth, name)
}

// escapeText follows the JAXB RI MinimumEscapeHandler for element text.
func escapeText(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '\r':
			b.WriteString("&#13;")
		default:
			b.WriteByte(c)
		}
	}
}
