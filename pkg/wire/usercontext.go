package wire

import (
	"regexp"
	"strings"
	"time"
	_ "time/tzdata" // zones must load in minimal images (alpine has no zoneinfo)
	"unicode"
)

// UserContext (v1.3) is display context about the person behind a turn: who
// they are called and what their clock says, so an agent can greet them and
// read "today" in their time zone. It is DISPLAY ONLY — never use it for
// authorization; identity comes from the X-TRX-IDT token (§5.1).
//
// Callers (UIs, relays, orchestrators) set it; agents read it through
// Normalize, which drops anything invalid instead of failing the turn.
type UserContext struct {
	TimeZone string `json:"timeZone,omitempty"` // IANA name, e.g. "America/New_York"
	UserName string `json:"userName,omitempty"` // display name, e.g. "Jane Doe"
	Locale   string `json:"locale,omitempty"`   // BCP 47 tag, e.g. "en-US"
}

const (
	maxUserNameRunes = 80
	maxTimeZoneLen   = 64
	maxLocaleLen     = 35
)

var (
	timeZoneRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$`)
	localeRe   = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$`)
)

// Normalize returns a cleaned copy: an unknown time zone, a malformed locale,
// and every character of the name outside letters, digits, spaces and
// . ' - , are dropped; whitespace is collapsed and the name capped at 80
// characters. Returns nil when nothing usable is left. Safe on nil.
func (u *UserContext) Normalize() *UserContext {
	if u == nil {
		return nil
	}
	out := UserContext{
		TimeZone: cleanTimeZone(u.TimeZone),
		UserName: cleanUserName(u.UserName),
		Locale:   cleanLocale(u.Locale),
	}
	if out == (UserContext{}) {
		return nil
	}
	return &out
}

// Location is the user's time zone, or UTC when unknown or invalid.
func (u *UserContext) Location() *time.Location {
	if u == nil {
		return time.UTC
	}
	if tz := cleanTimeZone(u.TimeZone); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc
		}
	}
	return time.UTC
}

// Line is the standard sentence every agent puts in its system prompt so the
// model knows who is asking and what "today" means for them, e.g.:
//
//	The user asking is Jane Doe (time zone America/New_York). Their current
//	local time is Monday, October 19, 2026 9:30 PM EDT; UTC is 2026-10-20 01:30 UTC.
//
// Safe on nil (states the time zone is unknown and gives UTC).
func (u *UserContext) Line(now time.Time) string {
	c := u.Normalize()
	utc := now.UTC().Format("2006-01-02 15:04") + " UTC"
	if c == nil || c.TimeZone == "" {
		who := ""
		if c != nil && c.UserName != "" {
			who = "The user asking is " + c.UserName + ". "
		}
		return who + "The user's time zone is unknown; the current time is " + utc + "."
	}
	local := now.In(c.Location()).Format("Monday, January 2, 2006 3:04 PM MST")
	who := "The user's time zone is " + c.TimeZone + "."
	if c.UserName != "" {
		who = "The user asking is " + c.UserName + " (time zone " + c.TimeZone + ")."
	}
	return who + " Their current local time is " + local + "; UTC is " + utc + "."
}

func cleanTimeZone(tz string) string {
	tz = strings.TrimSpace(tz)
	// "Local" would silently mean the agent's own zone; "" means UTC to Go.
	if tz == "" || tz == "Local" || len(tz) > maxTimeZoneLen || !timeZoneRe.MatchString(tz) {
		return ""
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return ""
	}
	return tz
}

func cleanUserName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case unicode.IsLetter(r), unicode.IsMark(r), unicode.IsDigit(r), r == '.', r == '\'', r == '-', r == ',':
			b.WriteRune(r)
		case unicode.IsSpace(r):
			b.WriteRune(' ')
		}
	}
	out := strings.Join(strings.Fields(b.String()), " ")
	if r := []rune(out); len(r) > maxUserNameRunes {
		out = strings.TrimSpace(string(r[:maxUserNameRunes]))
	}
	return out
}

func cleanLocale(l string) string {
	l = strings.TrimSpace(l)
	if len(l) > maxLocaleLen || !localeRe.MatchString(l) {
		return ""
	}
	return l
}
