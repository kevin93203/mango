// Package reconcile contains deterministic desired-state planning primitives.
package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"

	"github.com/kevin93203/mango/internal/config"
)

// ServiceChange describes one process-affecting or no-op service decision.
// Fingerprints deliberately avoid exposing environment values or other
// configuration details that may contain secrets.
type ServiceChange struct {
	Service           string `json:"service"`
	Action            string `json:"action"`
	Restart           bool   `json:"restart,omitempty"`
	BeforeFingerprint string `json:"before_fingerprint,omitempty"`
	AfterFingerprint  string `json:"after_fingerprint,omitempty"`
}

// Plan is the stable, JSON-friendly representation of an apply preview.
type Plan struct {
	Project            string          `json:"project"`
	CurrentGeneration  uint64          `json:"current_generation,omitempty"`
	ProposedGeneration uint64          `json:"proposed_generation,omitempty"`
	Changes            []ServiceChange `json:"changes"`
}

func (p Plan) HasChanges() bool {
	for _, change := range p.Changes {
		if change.Action != "unchanged" {
			return true
		}
	}
	return false
}

// Build compares two effective service snapshots. The order is always
// project/service lexical order, independent of map iteration order.
func Build(project string, current, desired []config.EffectiveService, active map[string]bool, currentGeneration, proposedGeneration uint64) Plan {
	currentByName := make(map[string]config.EffectiveService, len(current))
	desiredByName := make(map[string]config.EffectiveService, len(desired))
	names := make(map[string]bool, len(current)+len(desired))
	for _, spec := range current {
		currentByName[spec.Name] = spec
		names[spec.Name] = true
	}
	for _, spec := range desired {
		desiredByName[spec.Name] = spec
		names[spec.Name] = true
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	changes := make([]ServiceChange, 0, len(ordered))
	for _, name := range ordered {
		before, beforeOK := currentByName[name]
		after, afterOK := desiredByName[name]
		change := ServiceChange{Service: name}
		switch {
		case !beforeOK:
			change.Action = "added"
			change.AfterFingerprint = fingerprint(after)
		case !afterOK:
			change.Action = "removed"
			change.BeforeFingerprint = fingerprint(before)
		case reflect.DeepEqual(before, after):
			change.Action = "unchanged"
			change.BeforeFingerprint = fingerprint(before)
			change.AfterFingerprint = change.BeforeFingerprint
		default:
			change.BeforeFingerprint = fingerprint(before)
			change.AfterFingerprint = fingerprint(after)
			if active[name] {
				change.Action = "restarted"
				change.Restart = true
			} else {
				change.Action = "changed"
			}
		}
		changes = append(changes, change)
	}
	return Plan{Project: project, CurrentGeneration: currentGeneration, ProposedGeneration: proposedGeneration, Changes: changes}
}

func fingerprint(value config.EffectiveService) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
