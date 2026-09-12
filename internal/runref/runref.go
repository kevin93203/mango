// Package runref contains the shared formatting and matching rules for
// user-facing execution references.
package runref

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// MinPrefixLength is the minimum number of significant characters required
	// for a non-exact execution reference.
	MinPrefixLength = 8
	// DisplayLength is the default number of hexadecimal characters shown for
	// generated UUID execution IDs.
	DisplayLength = 12
)

// Candidate identifies one unambiguous reference for an execution.
type Candidate struct {
	Ref   string
	RunID string
}

// Display returns the operator-facing representation of an execution ID.
// Generated UUIDs are shown as compact hexadecimal prefixes. Opaque legacy
// IDs are deliberately left unchanged so existing history remains readable.
func Display(runID string, noTrunc bool) string {
	if runID == "" {
		return "-"
	}
	if noTrunc {
		return runID
	}
	if compact, ok := compactUUID(runID); ok {
		return compact[:DisplayLength]
	}
	return runID
}

// SignificantLength returns the number of ID characters in a reference. UUID
// separators are not significant, which makes both dashed and compact UUID
// prefixes obey the same minimum length rule.
func SignificantLength(ref string) int {
	if compact, ok := uuidPrefix(ref); ok {
		return len(compact)
	}
	return len(ref)
}

// IsUUIDPrefix reports whether ref is a valid compact or dashed UUID prefix.
// A UUID prefix may contain between eight and 32 hexadecimal characters.
func IsUUIDPrefix(ref string) bool {
	_, ok := uuidPrefix(ref)
	return ok
}

// StoragePrefix converts a UUID reference into the dashed form used by the
// persistence layer. Opaque IDs are returned unchanged.
func StoragePrefix(ref string) string {
	if compact, ok := uuidPrefix(ref); ok {
		return dashedPrefix(compact)
	}
	return ref
}

// Matches reports whether ref identifies storedID as an exact or prefix
// reference. UUID references are case-insensitive and ignore separators;
// opaque IDs use literal, case-sensitive matching.
func Matches(storedID, ref string) bool {
	if compact, ok := uuidPrefix(ref); ok {
		storedCompact, storedOK := compactUUID(storedID)
		if storedOK {
			return strings.HasPrefix(storedCompact, compact)
		}
		// A legacy opaque ID may consist only of hexadecimal characters. Keep
		// those IDs addressable even when the supplied reference resembles a
		// compact UUID prefix.
		return strings.HasPrefix(storedID, ref)
	}
	return strings.HasPrefix(storedID, ref)
}

// UniqueCandidates returns a deterministic, directly usable reference for
// each supplied ID. It chooses the shortest unique prefix at or above the
// minimum length and falls back to the full ID for short opaque IDs.
func UniqueCandidates(ids []string) []Candidate {
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	sort.Strings(unique)

	result := make([]Candidate, 0, len(unique))
	for _, id := range unique {
		token := id
		if compact, ok := compactUUID(id); ok {
			token = compact
		}
		candidate := token
		start := MinPrefixLength
		if len(token) < start {
			start = len(token)
		}
		for length := start; length <= len(token); length++ {
			ref := token[:length]
			matches := 0
			for _, other := range unique {
				if Matches(other, ref) {
					matches++
					if matches > 1 {
						break
					}
				}
			}
			if matches == 1 {
				candidate = ref
				break
			}
		}
		result = append(result, Candidate{Ref: candidate, RunID: id})
	}
	return result
}

func compactUUID(value string) (string, bool) {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", false
	}
	compact := strings.Builder{}
	compact.Grow(32)
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !isHex(char) {
			return "", false
		}
		compact.WriteByte(toLowerHex(byte(char)))
	}
	return compact.String(), compact.Len() == 32
}

func uuidPrefix(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	compact := strings.Builder{}
	compact.Grow(len(value))
	hexCount := 0
	for _, char := range value {
		switch {
		case char == '-':
			if hexCount != 8 && hexCount != 12 && hexCount != 16 && hexCount != 20 {
				return "", false
			}
		case isHex(char):
			if hexCount >= 32 {
				return "", false
			}
			compact.WriteByte(toLowerHex(byte(char)))
			hexCount++
		default:
			return "", false
		}
	}
	if hexCount < MinPrefixLength || hexCount > 32 {
		return "", false
	}
	return compact.String(), true
}

func dashedPrefix(compact string) string {
	if len(compact) <= 8 {
		return compact
	}
	var result strings.Builder
	result.Grow(len(compact) + 4)
	for index, char := range compact {
		if index == 8 || index == 12 || index == 16 || index == 20 {
			result.WriteByte('-')
		}
		result.WriteByte(byte(char))
	}
	return result.String()
}

func isHex(value rune) bool {
	return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f') || (value >= 'A' && value <= 'F')
}

func toLowerHex(value byte) byte {
	if value >= 'A' && value <= 'F' {
		return value + ('a' - 'A')
	}
	return value
}

// Validate returns a user-facing error for a non-exact reference that is too
// short. Exact lookup is intentionally performed by the scheduler before
// this check so short legacy IDs remain compatible.
func Validate(ref string) error {
	if SignificantLength(ref) < MinPrefixLength {
		return fmt.Errorf("run reference must be an exact ID or at least %d characters", MinPrefixLength)
	}
	return nil
}
