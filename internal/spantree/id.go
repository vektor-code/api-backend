package spantree

import "strings"

// Normalize makes span IDs comparable across ingest encodings.
// Empty, all-zero, and 0x00.. IDs are roots ("").
// Shorter hex IDs are left-padded to 16 characters.
func Normalize(id string) string {
	s := strings.TrimSpace(strings.ToLower(id))
	if strings.HasPrefix(s, "0x") {
		s = s[2:]
	}
	if s == "" || isAllZero(s) {
		return ""
	}
	if len(s) < 16 && isHex(s) {
		s = strings.Repeat("0", 16-len(s)) + s
	}
	return s
}

func IsRoot(id string) bool {
	return Normalize(id) == ""
}

func isAllZero(s string) bool {
	for _, c := range s {
		if c != '0' {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
