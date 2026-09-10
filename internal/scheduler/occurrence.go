package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

const maxScheduleOccurrenceScan = 10000

// ScheduleOccurrenceID is stable for a schedule slot. The UTC instant makes
// the two sides of a DST fall-back distinct while keeping retries and daemon
// restarts idempotent.
func ScheduleOccurrenceID(project, schedule string, scheduledAt time.Time) string {
	payload := project + "\x00" + schedule + "\x00" + scheduledAt.UTC().Format(time.RFC3339Nano)
	digest := sha256.Sum256([]byte(payload))
	return "occ-" + hex.EncodeToString(digest[:16])
}

func ScheduleOccurrenceIdempotencyKey(id string) string {
	return "schedule:" + id
}

func (s *Scheduler) setClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clock == nil {
		s.clock = time.Now
		return
	}
	s.clock = clock
}

func (s *Scheduler) now() time.Time {
	s.mu.Lock()
	clock := s.clock
	s.mu.Unlock()
	if clock == nil {
		return time.Now()
	}
	return clock()
}

func (s *Scheduler) occurrenceStoreSnapshot() HistoryRepository {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.historyRepo
}

func (s *Scheduler) scheduleDueTimes(ctx context.Context, schedule config.EffectiveSchedule, now time.Time) ([]time.Time, error) {
	return s.scheduleDueTimesMode(ctx, schedule, now, false)
}

func (s *Scheduler) scheduleDueTimesMode(ctx context.Context, schedule config.EffectiveSchedule, now time.Time, includeCurrent bool) ([]time.Time, error) {
	s.mu.Lock()
	parser := s.parser
	reader := s.historyRepo
	s.mu.Unlock()
	if reader == nil {
		return []time.Time{now}, nil
	}
	location := schedule.Timezone
	if location == nil {
		location = time.Local
	}
	spec, err := parser.Parse("CRON_TZ=" + location.String() + " " + schedule.Cron)
	if err != nil {
		return nil, fmt.Errorf("parse schedule %s/%s: %w", schedule.Project, schedule.Name, err)
	}
	latest, err := reader.LatestScheduleOccurrence(ctx, schedule.Project, schedule.Name)
	after := now
	if includeCurrent {
		after = now.Add(-time.Minute)
	}
	if err == nil {
		after = latest.ScheduledAt
	} else if err != ErrScheduleOccurrenceNotFound {
		return nil, err
	}
	result := make([]time.Time, 0, 1)
	for len(result) < maxScheduleOccurrenceScan {
		next := spec.Next(after)
		if next.IsZero() || next.After(now) {
			break
		}
		result = append(result, next.UTC())
		after = next
	}
	return result, nil
}

func (s *Scheduler) executeDue(ctx context.Context, schedule config.EffectiveSchedule, now time.Time) {
	s.executeDueMode(ctx, schedule, now, false)
}

func (s *Scheduler) executeDueMode(ctx context.Context, schedule config.EffectiveSchedule, now time.Time, startup bool) {
	due, err := s.scheduleDueTimesMode(ctx, schedule, now, !startup)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: calculate schedule occurrences: %v\n", err)
		return
	}
	if len(due) == 0 {
		return
	}
	misfire := schedule.Misfire
	if misfire == "" {
		misfire = config.DefaultScheduleMisfire
	}
	maxCatchUp := schedule.MaxCatchUp
	if maxCatchUp <= 0 {
		maxCatchUp = config.DefaultScheduleMaxCatchUp
	}
	start := 0
	switch misfire {
	case "skip":
		start = len(due)
		if !startup && len(due) > 0 && sameScheduleMinute(schedule, due[len(due)-1], now) {
			start = len(due) - 1
		}
	case "run_once":
		start = len(due) - 1
	case "catch_up":
		if len(due) > maxCatchUp {
			start = len(due) - maxCatchUp
		}
	default:
		start = len(due) - 1
	}
	for _, scheduledAt := range due[:start] {
		s.recordSkippedOccurrence(ctx, schedule, scheduledAt)
	}
	for _, scheduledAt := range due[start:] {
		s.executeOccurrence(ctx, schedule, scheduledAt)
	}
}

func sameScheduleMinute(schedule config.EffectiveSchedule, scheduledAt, now time.Time) bool {
	location := schedule.Timezone
	if location == nil {
		location = time.Local
	}
	scheduledLocal := scheduledAt.In(location)
	nowLocal := now.In(location)
	return scheduledLocal.Year() == nowLocal.Year() && scheduledLocal.YearDay() == nowLocal.YearDay() &&
		scheduledLocal.Hour() == nowLocal.Hour() && scheduledLocal.Minute() == nowLocal.Minute()
}

func (s *Scheduler) recordSkippedOccurrence(ctx context.Context, schedule config.EffectiveSchedule, scheduledAt time.Time) {
	store := s.occurrenceStoreSnapshot()
	if store == nil {
		return
	}
	occurrence := ScheduleOccurrence{
		ID: ScheduleOccurrenceID(schedule.Project, schedule.Name, scheduledAt), Project: schedule.Project,
		Schedule: schedule.Name, ScheduledAt: scheduledAt.UTC(), Status: StatusSkipped, Misfire: schedule.Misfire,
	}
	claimed, created, err := store.ClaimScheduleOccurrence(ctx, occurrence)
	if err != nil || !created && claimed.Status != OccurrencePending {
		return
	}
	claimed.Status = StatusSkipped
	_ = store.UpdateScheduleOccurrence(ctx, claimed)
}
