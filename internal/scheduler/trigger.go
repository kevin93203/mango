package scheduler

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// TriggerRef identifies the source that caused a logical execution. Type is
// intentionally open-ended so future trigger implementations can be added
// without changing the execution record shape.
type TriggerRef struct {
	Type    string
	Name    string
	Mode    string
	EventID string
}

const (
	TriggerSchedule = "schedule"
	TriggerWebhook  = "webhook"
	TriggerManual   = "manual"

	TriggerModeAutomatic = "automatic"
	TriggerModeManual    = "manual"
	TriggerModeRetry     = "retry"
	// TriggerAutomatic is retained as a readable alias for callers that used
	// the original mode constant name during the trigger migration.
	TriggerAutomatic = TriggerModeAutomatic
)

func ManualTrigger() TriggerRef {
	return TriggerRef{Type: TriggerManual, Mode: TriggerModeManual}
}

func ScheduleTrigger(name string) TriggerRef {
	return TriggerRef{Type: TriggerSchedule, Name: name, Mode: TriggerModeAutomatic}
}

func (t TriggerRef) IsZero() bool {
	return t.Type == "" && t.Name == "" && t.Mode == "" && t.EventID == ""
}

func (t TriggerRef) Display() string {
	if t.Name != "" {
		return t.Type + "/" + t.Name
	}
	if t.Type != "" {
		return t.Type
	}
	return "-"
}

// UnmarshalJSON keeps version 1 history readable. Version 1 stored Trigger
// as a string; version 2 stores the structured TriggerRef object.
func (r *Record) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var triggerData json.RawMessage
	for key, value := range fields {
		if strings.EqualFold(key, "trigger") {
			triggerData = value
			delete(fields, key)
		}
	}
	type recordAlias Record
	var decoded recordAlias
	withoutTrigger, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(withoutTrigger, &decoded); err != nil {
		return err
	}
	*r = Record(decoded)
	if len(triggerData) == 0 || string(triggerData) == "null" {
		return nil
	}
	triggerData = bytes.TrimSpace(triggerData)
	if len(triggerData) == 0 {
		return nil
	}
	if triggerData[0] == '"' {
		var legacy string
		if err := json.Unmarshal(triggerData, &legacy); err != nil {
			return err
		}
		if legacy == TriggerManual {
			r.Trigger = ManualTrigger()
		} else if legacy != "" {
			r.Trigger = ScheduleTrigger(legacy)
		}
		return nil
	}
	return json.Unmarshal(triggerData, &r.Trigger)
}

// NewRunID returns an opaque, persistent identifier for a logical run or
// nested task invocation. It deliberately has no ordering semantics.
func NewRunID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return formatRunID(raw)
	}
	// crypto/rand failures are exceptionally unlikely. Keep the fallback
	// unique as well; RunID is used to distinguish concurrent executions.
	binary.BigEndian.PutUint64(raw[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(raw[8:], fallbackRunID.Add(1))
	return formatRunID(raw)
}

var fallbackRunID atomic.Uint64

func formatRunID(raw [16]byte) string {
	// UUID v4 layout makes the identifier easy to recognize while Mango still
	// treats it as opaque and assigns no ordering semantics.
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
}
