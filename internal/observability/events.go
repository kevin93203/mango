// Package observability contains the safe, cross-subsystem event and audit
// contracts used by Mango's IPC, HTTP, CLI, and durable metadata layers.
package observability

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type Event struct {
	ID                      int64             `json:"id,omitempty"`
	Timestamp               time.Time         `json:"timestamp"`
	Type                    string            `json:"type"`
	Actor                   string            `json:"actor,omitempty"`
	Project                 string            `json:"project,omitempty"`
	Target                  string            `json:"target,omitempty"`
	RunID                   string            `json:"run_id,omitempty"`
	OperationID             string            `json:"operation_id,omitempty"`
	ConfigurationGeneration uint64            `json:"configuration_generation,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
}

type AuditEntry struct {
	ID          int64     `json:"id,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Actor       string    `json:"actor,omitempty"`
	Action      string    `json:"action"`
	Target      string    `json:"target,omitempty"`
	OperationID string    `json:"operation_id,omitempty"`
	Result      string    `json:"result"`
	Error       string    `json:"error,omitempty"`
}

type EventQuery struct {
	AfterID int64
	Limit   int
	Type    string
	Project string
	RunID   string
}

type Store interface {
	AppendEvent(context.Context, Event) (Event, error)
	ListEvents(context.Context, EventQuery) ([]Event, error)
	AppendAudit(context.Context, AuditEntry) (AuditEntry, error)
	ListAudit(context.Context, int) ([]AuditEntry, error)
}

type EventPruner interface {
	PruneEvents(context.Context, int) error
}

type SecretReferenceMetadata struct {
	Project   string
	Target    string
	Name      string
	Provider  string
	Reference string
}

type ResourcePolicyMetadata struct {
	Project      string
	Target       string
	ProcessLimit int
	Memory       string
	CPUPercent   int
	Status       string
}

type MetadataStore interface {
	ReplaceSecurityMetadata(context.Context, string, []SecretReferenceMetadata, []ResourcePolicyMetadata) error
}

type Bus struct {
	mu          sync.RWMutex
	subscribers map[int]chan Event
	nextID      int
}

func NewBus() *Bus { return &Bus{subscribers: map[int]chan Event{}} }

func (b *Bus) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 32
	}
	b.mu.Lock()
	if b.subscribers == nil {
		b.subscribers = map[int]chan Event{}
	}
	b.nextID++
	id := b.nextID
	channel := make(chan Event, buffer)
	b.subscribers[id] = channel
	b.mu.Unlock()
	return channel, func() {
		b.mu.Lock()
		if current := b.subscribers[id]; current != nil {
			delete(b.subscribers, id)
			close(current)
		}
		b.mu.Unlock()
	}
}

func (b *Bus) Publish(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, subscriber := range b.subscribers {
		select {
		case subscriber <- event:
		default:
			// A slow observer must not block service execution or IPC.
		}
	}
}

func MarshalMetadata(values map[string]interface{}) string {
	data, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(data)
}

type actorKey struct{}

func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

func Actor(ctx context.Context) string {
	if ctx != nil {
		if value, ok := ctx.Value(actorKey{}).(string); ok && value != "" {
			return value
		}
	}
	return "local"
}
