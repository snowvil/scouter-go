package pulse

import (
	"strconv"
	"strings"
	"unicode"
)

// javaWhitespace is java.lang.Character.isWhitespace: Unicode space separators
// except the non-breaking ones, plus the listed ASCII controls.
func javaWhitespace(r rune) bool {
	switch r {
	case '\t', '\n', '\u000B', '\f', '\r', '\u001C', '\u001D', '\u001E', '\u001F':
		return true
	case ' ', ' ', ' ':
		return false
	}
	return unicode.Is(unicode.Zs, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r)
}

// javaIsBlank is org.apache.commons.lang3.StringUtils.isBlank.
func javaIsBlank(s string) bool {
	for _, r := range s {
		if !javaWhitespace(r) {
			return false
		}
	}
	return true
}

// javaSplitChar is scouter.util.StringUtil.split(String, char): empty tokens
// are dropped.
func javaSplitChar(s string, c rune) []string {
	var out []string
	for _, part := range strings.Split(s, string(c)) {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// javaFormat reproduces the subset of java.lang.String.format(format, String...)
// that can succeed with String arguments: %s %S %b %B %h %H %n %% with optional
// argument index (n$ or <), the '-' flag, width and precision. Any specifier
// that Java would reject (other conversions with a String argument, bad flags,
// missing arguments, dangling '%') returns ok=false, which mirrors
// ParameterizedMessageStep.buildMessasge returning the raw format on exception.
func javaFormat(format string, args []string) (string, bool) {
	var out strings.Builder
	last := -1    // index of the previous ordinary/explicit argument
	ordinary := 0 // next ordinary argument index
	i := 0
	for i < len(format) {
		c := format[i]
		if c != '%' {
			out.WriteByte(c)
			i++
			continue
		}
		i++
		// %[argument_index$][flags][width][.precision]conversion
		j := i
		for j < len(format) && format[j] >= '0' && format[j] <= '9' {
			j++
		}
		explicit := -1
		if j < len(format) && format[j] == '$' && j > i {
			n, err := strconv.Atoi(format[i:j])
			if err != nil {
				return "", false
			}
			explicit = n
			i = j + 1
		}
		leftJustify, previous := false, false
		for i < len(format) && strings.IndexByte("-#+ 0,(<", format[i]) >= 0 {
			switch format[i] {
			case '-':
				if leftJustify {
					return "", false // DuplicateFormatFlagsException
				}
				leftJustify = true
			case '<':
				if previous {
					return "", false
				}
				previous = true
			default:
				return "", false // flag not valid for a String argument
			}
			i++
		}
		width := -1
		j = i
		for j < len(format) && format[j] >= '0' && format[j] <= '9' {
			j++
		}
		if j > i {
			n, err := strconv.Atoi(format[i:j])
			if err != nil {
				return "", false
			}
			width = n
			i = j
		}
		precision := -1
		if i < len(format) && format[i] == '.' {
			j = i + 1
			for j < len(format) && format[j] >= '0' && format[j] <= '9' {
				j++
			}
			if j == i+1 {
				return "", false
			}
			n, err := strconv.Atoi(format[i+1 : j])
			if err != nil {
				return "", false
			}
			precision = n
			i = j
		}
		if i >= len(format) {
			return "", false // UnknownFormatConversionException
		}
		conv := format[i]
		i++
		if leftJustify && width == -1 {
			return "", false // MissingFormatWidthException
		}
		switch conv {
		case '%':
			// Fixed text ignores an argument index; precision is illegal.
			if precision != -1 {
				return "", false
			}
			out.WriteString(pad("%", width, leftJustify))
			continue
		case 'n':
			if precision != -1 || width != -1 || leftJustify {
				return "", false
			}
			out.WriteString("\n")
			continue
		case 's', 'S', 'b', 'B', 'h', 'H':
		default:
			return "", false // not applicable to a String argument
		}
		var arg string
		switch {
		case previous:
			if last < 0 || last >= len(args) {
				return "", false
			}
			arg = args[last]
		case explicit >= 1:
			if explicit > len(args) {
				return "", false
			}
			last = explicit - 1
			arg = args[last]
		default:
			if ordinary >= len(args) {
				return "", false
			}
			last = ordinary
			arg = args[ordinary]
			ordinary++
		}
		var text string
		switch conv {
		case 's', 'S':
			text = arg
		case 'b', 'B':
			text = "true"
		case 'h', 'H':
			text = strconv.FormatUint(uint64(uint32(JavaStringHashCode(arg))), 16)
		}
		if precision != -1 {
			runes := []rune(text)
			if precision < len(runes) {
				text = string(runes[:precision])
			}
		}
		if conv == 'S' || conv == 'B' || conv == 'H' {
			text = strings.ToUpper(text)
		}
		out.WriteString(pad(text, width, leftJustify))
	}
	return out.String(), true
}

func pad(s string, width int, left bool) string {
	n := len([]rune(s))
	if width <= n {
		return s
	}
	fill := strings.Repeat(" ", width-n)
	if left {
		return s + fill
	}
	return fill + s
}
