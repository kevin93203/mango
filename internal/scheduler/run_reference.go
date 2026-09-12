package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/kevin93203/mango/internal/runref"
)

const (
	RunReferenceAmbiguousCode = "RUN_ID_AMBIGUOUS"
	RunReferenceTooShortCode  = "RUN_ID_PREFIX_TOO_SHORT"
)

// RunReferenceError describes a reference failure that is safe to return to
// an API client. Candidates contain both an immediately usable short ref and
// the canonical ID for clients that need an immutable value.
type RunReferenceError struct {
	Code       string
	Reference  string
	Candidates []runref.Candidate
}

func (e *RunReferenceError) Error() string {
	switch e.Code {
	case RunReferenceAmbiguousCode:
		return fmt.Sprintf("run reference %q is ambiguous", e.Reference)
	case RunReferenceTooShortCode:
		return fmt.Sprintf("run reference %q must be an exact ID or at least %d characters", e.Reference, runref.MinPrefixLength)
	default:
		return fmt.Sprintf("invalid run reference %q", e.Reference)
	}
}

// ResolveExecutionRunID resolves an operator-supplied reference to the
// canonical ID of a root execution. Exact lookup is deliberately attempted
// before the minimum prefix-length check to preserve short legacy IDs.
func (s *Scheduler) ResolveExecutionRunID(ctx context.Context, reference string) (string, error) {
	store := s.executionStore()
	if store == nil {
		return "", ErrExecutionNotFound
	}

	if execution, err := store.GetExecution(ctx, reference); err == nil {
		return execution.Record.RunID, nil
	} else if !errors.Is(err, ErrExecutionNotFound) {
		return "", err
	}

	// A full or compact UUID may be supplied with different casing or without
	// separators. Try its canonical storage form before doing prefix lookup.
	storagePrefix := runref.StoragePrefix(reference)
	if storagePrefix != reference {
		if execution, err := store.GetExecution(ctx, storagePrefix); err == nil {
			return execution.Record.RunID, nil
		} else if !errors.Is(err, ErrExecutionNotFound) {
			return "", err
		}
	}

	if err := runref.Validate(reference); err != nil {
		return "", &RunReferenceError{Code: RunReferenceTooShortCode, Reference: reference}
	}

	ids, err := store.FindExecutionRunIDsByPrefix(ctx, storagePrefix)
	if err != nil {
		return "", err
	}
	// Compact UUID-looking references can also be valid opaque legacy IDs.
	// Query the literal form as well when it differs from the storage form.
	if storagePrefix != reference {
		literalIDs, literalErr := store.FindExecutionRunIDsByPrefix(ctx, reference)
		if literalErr != nil {
			return "", literalErr
		}
		ids = append(ids, literalIDs...)
	}

	matched := uniqueMatchingIDs(ids, reference)
	switch len(matched) {
	case 0:
		return "", ErrExecutionNotFound
	case 1:
		return matched[0], nil
	default:
		return "", &RunReferenceError{
			Code:       RunReferenceAmbiguousCode,
			Reference:  reference,
			Candidates: runref.UniqueCandidates(matched),
		}
	}
}

func uniqueMatchingIDs(ids []string, reference string) []string {
	seen := make(map[string]struct{}, len(ids))
	matched := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" || !runref.Matches(id, reference) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		matched = append(matched, id)
	}
	sort.Strings(matched)
	return matched
}

// IsRunReferenceError reports whether err is a structured reference error.
func IsRunReferenceError(err error) bool {
	var target *RunReferenceError
	return errors.As(err, &target)
}

// RunReferenceCandidates returns a copy of the structured candidates. It is
// useful to API adapters that must not expose the scheduler's slice directly.
func RunReferenceCandidates(err error) []runref.Candidate {
	var target *RunReferenceError
	if !errors.As(err, &target) {
		return nil
	}
	return append([]runref.Candidate(nil), target.Candidates...)
}

// RunReferenceCode returns the wire-level code for a structured reference
// error, or an empty string for unrelated errors.
func RunReferenceCode(err error) string {
	var target *RunReferenceError
	if !errors.As(err, &target) {
		return ""
	}
	return target.Code
}

// RunReferenceMessage is kept separate from Error so adapters can preserve
// the original input in their own response format without parsing strings.
func RunReferenceMessage(err error) string {
	var target *RunReferenceError
	if !errors.As(err, &target) {
		return ""
	}
	return target.Error()
}

// RunReferenceHint formats the short candidates for a human-facing error.
func RunReferenceHint(err error) string {
	var target *RunReferenceError
	if !errors.As(err, &target) || len(target.Candidates) == 0 {
		return ""
	}
	refs := make([]string, 0, len(target.Candidates))
	for _, candidate := range target.Candidates {
		refs = append(refs, candidate.Ref)
	}
	return strings.Join(refs, ", ")
}
