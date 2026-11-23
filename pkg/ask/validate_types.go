package ask

import (
	"fmt"
	"math"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

// maxCachedPatterns bounds the compiled-pattern cache: patterns come from
// asks (incl. caller-supplied inputs), so past the bound they are compiled
// per use and not kept.
const maxCachedPatterns = 256

var reCache = struct {
	sync.Mutex
	m map[string]*regexp.Regexp
}{m: map[string]*regexp.Regexp{}}

// compiled caches compiled patterns (up to maxCachedPatterns); invalid
// ones are not cached.
func compiled(p string) (*regexp.Regexp, error) {
	reCache.Lock()
	re, ok := reCache.m[p]
	reCache.Unlock()
	if ok {
		return re, nil
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, err
	}
	reCache.Lock()
	if len(reCache.m) < maxCachedPatterns {
		reCache.m[p] = re
	}
	reCache.Unlock()
	return re, nil
}

// cachedPatterns is the cache size (tests).
func cachedPatterns() int {
	reCache.Lock()
	defer reCache.Unlock()
	return len(reCache.m)
}

var phoneRE = regexp.MustCompile(`^\+?[0-9 ()-]{7,20}$`)

func checkText(f Field, v any) string {
	l := f.label()
	s, ok := v.(string)
	if !ok {
		return l + " must be text"
	}
	n := utf8.RuneCountInString(s)
	if f.MinLength > 0 && n < f.MinLength {
		return fmt.Sprintf("%s must be at least %d characters", l, f.MinLength)
	}
	if f.MaxLength > 0 && n > f.MaxLength {
		return fmt.Sprintf("%s must be at most %d characters", l, f.MaxLength)
	}
	switch f.Format {
	case FormatEmail:
		if addr, err := mail.ParseAddress(s); err != nil || addr.Address != s {
			return l + " must be a valid email address"
		}
	case FormatURL:
		if u, err := url.ParseRequestURI(s); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return l + " must be a valid URL"
		}
	case FormatPhone:
		if !phoneRE.MatchString(s) {
			return l + " must be a valid phone number"
		}
	}
	if f.Pattern != "" {
		if re, err := compiled(f.Pattern); err == nil && !re.MatchString(s) {
			if f.PatternMessage != "" {
				return f.PatternMessage
			}
			return l + " has an invalid format"
		}
	}
	return ""
}

func num(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }

func checkNumber(f Field, v any) string {
	l := f.label()
	x, ok := toFloat(v)
	if !ok || math.IsNaN(x) || math.IsInf(x, 0) {
		return l + " must be a number"
	}
	lo, hasLo := toFloat(f.Min)
	if hasLo && x < lo {
		return fmt.Sprintf("%s must be at least %s", l, num(lo))
	}
	if hi, ok := toFloat(f.Max); ok && x > hi {
		return fmt.Sprintf("%s must be at most %s", l, num(hi))
	}
	if f.Step > 0 {
		base := 0.0
		if hasLo {
			base = lo
		}
		q := (x - base) / f.Step
		if math.Abs(q-math.Round(q)) > 1e-9 {
			return fmt.Sprintf("%s must be a multiple of %s", l, num(f.Step))
		}
	}
	return ""
}

func choiceOK(f Field, v any) bool {
	switch t := v.(type) {
	case string:
		for _, o := range f.Options {
			if o.Value == t {
				return true
			}
		}
	case map[string]any:
		s, ok := t["other"].(string)
		return ok && s != "" && len(t) == 1 && f.allowOther()
	}
	return false
}

func checkChoiceValue(f Field, v any) string {
	bad := f.label() + " has an invalid choice"
	if !f.Multiple {
		if !choiceOK(f, v) {
			return bad
		}
		return ""
	}
	arr, ok := v.([]any)
	if !ok {
		return bad
	}
	for _, x := range arr {
		if !choiceOK(f, x) {
			return bad
		}
	}
	return items(len(arr), f)
}

func parseDate(s string) (time.Time, bool) {
	if len(s) != 10 {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02", s)
	return t, err == nil
}

func parseTime(s string) (time.Time, bool) {
	if len(s) != 5 {
		return time.Time{}, false
	}
	t, err := time.Parse("15:04", s)
	return t, err == nil
}

func parseDateTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

// bounds checks t against the field's min/max using parse; the words go in
// the messages ("on or after" for dates, "at or after" otherwise).
func bounds(f Field, t time.Time, parse func(string) (time.Time, bool), after, before string) string {
	if s, ok := f.Min.(string); ok {
		if lo, ok := parse(s); ok && t.Before(lo) {
			return fmt.Sprintf("%s must be %s %s", f.label(), after, s)
		}
	}
	if s, ok := f.Max.(string); ok {
		if hi, ok := parse(s); ok && t.After(hi) {
			return fmt.Sprintf("%s must be %s %s", f.label(), before, s)
		}
	}
	return ""
}

func checkDate(f Field, v any) string {
	s, _ := v.(string)
	t, ok := parseDate(s)
	if !ok {
		return f.label() + " must be a date (YYYY-MM-DD)"
	}
	return bounds(f, t, parseDate, "on or after", "on or before")
}

func checkTime(f Field, v any) string {
	s, _ := v.(string)
	t, ok := parseTime(s)
	if !ok {
		return f.label() + " must be a time (HH:MM)"
	}
	return bounds(f, t, parseTime, "at or after", "at or before")
}

func checkDateTime(f Field, v any) string {
	s, _ := v.(string)
	t, ok := parseDateTime(s)
	if !ok {
		return f.label() + " must be a date and time with a UTC offset"
	}
	return bounds(f, t, parseDateTime, "at or after", "at or before")
}

func checkDateRange(f Field, v any) string {
	l := f.label()
	m, ok := v.(map[string]any)
	if !ok {
		return l + " must be a date range"
	}
	fromV, toV := m["from"], m["to"]
	if isEmpty(fromV) || isEmpty(toV) {
		return l + " needs a start and end date"
	}
	fs, _ := fromV.(string)
	ts, _ := toV.(string)
	from, ok1 := parseDate(fs)
	to, ok2 := parseDate(ts)
	if !ok1 || !ok2 {
		return l + " must be a date range"
	}
	if p, has := m["preset"]; has && p != nil {
		ps, _ := p.(string)
		if !validPreset(ps) || (len(f.Presets) > 0 && !contains(f.Presets, ps)) {
			return l + " has an invalid preset"
		}
	}
	if from.After(to) {
		return l + ": start must be on or before end"
	}
	if msg := bounds(f, from, parseDate, "on or after", "on or before"); msg != "" {
		return msg
	}
	return bounds(f, to, parseDate, "on or after", "on or before")
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func checkTags(f Field, v any) string {
	arr, ok := v.([]any)
	if !ok {
		return f.label() + " must be a list of text"
	}
	for _, x := range arr {
		if _, ok := x.(string); !ok {
			return f.label() + " must be a list of text"
		}
	}
	return items(len(arr), f)
}

func checkTable(f Field, v any) string {
	bad := f.label() + " has an invalid selection"
	var rows []any
	switch t := v.(type) {
	case map[string]any:
		rows = []any{t}
	case []any:
		rows = t
	default:
		return bad
	}
	for _, r := range rows {
		m, ok := r.(map[string]any)
		if !ok {
			return bad
		}
		if len(f.Rows) > 0 {
			found := false
			for _, want := range f.Rows {
				if same(want, m) {
					found = true
					break
				}
			}
			if !found {
				return bad
			}
		}
	}
	return items(len(rows), f)
}

// compare orders two values of a field type for the after/notBefore rules.
func compare(typ string, x, y any) (int, bool) {
	switch typ {
	case TypeNumber:
		a, ok1 := toFloat(x)
		b, ok2 := toFloat(y)
		if !ok1 || !ok2 {
			return 0, false
		}
		return cmpFloat(a, b), true
	case TypeDate, TypeTime, TypeDateTime:
		parse := map[string]func(string) (time.Time, bool){TypeDate: parseDate, TypeTime: parseTime, TypeDateTime: parseDateTime}[typ]
		xs, _ := x.(string)
		ys, _ := y.(string)
		a, ok1 := parse(xs)
		b, ok2 := parse(ys)
		if !ok1 || !ok2 {
			return 0, false
		}
		switch {
		case a.Before(b):
			return -1, true
		case a.After(b):
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
