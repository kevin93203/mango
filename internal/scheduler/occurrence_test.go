package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

func TestScheduleOccurrenceIsStableAcrossRepeatedDispatchAndRestart(t *testing.T) {
	var runs atomic.Int32
	repo := newMemoryHistoryRepository()
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "minute", Cron: "* * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "skip",
	}
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	newScheduler := func() *Scheduler {
		s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
			runs.Add(1)
			return ExecutionResult{}
		})
		if err := s.SetHistoryRepository(repo); err != nil {
			t.Fatal(err)
		}
		s.setClock(func() time.Time { return now })
		if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
			t.Fatal(err)
		}
		return s
	}

	first := newScheduler()
	previous := ScheduleOccurrence{
		ID: ScheduleOccurrenceID("demo", "minute", now.Add(-time.Minute)), Project: "demo", Schedule: "minute",
		ScheduledAt: now.Add(-time.Minute), Status: StatusSuccess,
	}
	if _, _, err := repo.ClaimScheduleOccurrence(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	first.executeDue(context.Background(), schedule, now)
	if runs.Load() != 1 {
		t.Fatalf("first dispatch runs = %d, want one", runs.Load())
	}

	second := newScheduler()
	second.executeDue(context.Background(), schedule, now)
	if runs.Load() != 1 {
		t.Fatalf("restart dispatch runs = %d, want no duplicate", runs.Load())
	}
	occurrenceID := ScheduleOccurrenceID("demo", "minute", time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC))
	occurrence, err := repo.GetScheduleOccurrence(context.Background(), occurrenceID)
	if err != nil {
		t.Fatal(err)
	}
	if occurrence.Status != StatusSuccess || occurrence.RunID == "" {
		t.Fatalf("occurrence = %+v, want completed occurrence with run id", occurrence)
	}
	events, err := repo.ListExecutionEvents(context.Background(), occurrence.RunID)
	if err != nil {
		t.Fatal(err)
	}
	foundOccurrence := false
	for _, event := range events {
		if event.Type == "schedule_occurrence" && event.Status == OccurrenceDispatched {
			foundOccurrence = true
			break
		}
	}
	if !foundOccurrence {
		t.Fatalf("events = %+v, want schedule occurrence event", events)
	}
}

func TestScheduleCatchUpIsBounded(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		return ExecutionResult{}
	})
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "minute", Cron: "* * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "catch_up", MaxCatchUp: 2,
	}
	base := time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC)
	store := s.occurrenceStoreSnapshot()
	last := ScheduleOccurrence{ID: ScheduleOccurrenceID("demo", "minute", base), Project: "demo", Schedule: "minute", ScheduledAt: base, Status: StatusSuccess}
	if _, _, err := store.ClaimScheduleOccurrence(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateScheduleOccurrence(context.Background(), last); err != nil {
		t.Fatal(err)
	}

	now := base.Add(3*time.Minute + 5*time.Second)
	s.executeDue(context.Background(), schedule, now)
	if runs.Load() != 2 {
		t.Fatalf("catch-up runs = %d, want max_catch_up=2", runs.Load())
	}
	if latest, err := store.LatestScheduleOccurrence(context.Background(), "demo", "minute"); err != nil || latest.ScheduledAt != base.Add(3*time.Minute) {
		t.Fatalf("latest occurrence = %+v, err=%v, want final catch-up slot", latest, err)
	}
}

func TestScheduleMisfirePoliciesSkipOrRunOnce(t *testing.T) {
	base := time.Date(2026, time.January, 2, 2, 0, 0, 0, time.UTC)
	newScheduler := func(policy string) (*Scheduler, HistoryRepository, *atomic.Int32) {
		runs := &atomic.Int32{}
		s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
			runs.Add(1)
			return ExecutionResult{}
		})
		store := s.occurrenceStoreSnapshot()
		previous := ScheduleOccurrence{
			ID: ScheduleOccurrenceID("demo", policy, base), Project: "demo", Schedule: policy,
			ScheduledAt: base, Status: StatusSuccess,
		}
		if _, _, err := store.ClaimScheduleOccurrence(context.Background(), previous); err != nil {
			t.Fatal(err)
		}
		return s, store, runs
	}

	skipScheduler, skipStore, skipRuns := newScheduler("skip")
	skipSchedule := config.EffectiveSchedule{
		Project: "demo", Name: "skip", Cron: "0 * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "skip",
	}
	skipScheduler.executeDue(context.Background(), skipSchedule, base.Add(3*time.Hour+5*time.Minute))
	if skipRuns.Load() != 0 {
		t.Fatalf("skip policy runs = %d, want no backfill", skipRuns.Load())
	}
	if latest, err := skipStore.LatestScheduleOccurrence(context.Background(), "demo", "skip"); err != nil || latest.Status != StatusSkipped {
		t.Fatalf("skip latest occurrence = %+v, err=%v, want skipped", latest, err)
	}

	onceScheduler, _, onceRuns := newScheduler("run_once")
	onceSchedule := config.EffectiveSchedule{
		Project: "demo", Name: "run_once", Cron: "0 * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "run_once",
	}
	onceScheduler.executeDue(context.Background(), onceSchedule, base.Add(3*time.Hour+5*time.Minute))
	if onceRuns.Load() != 1 {
		t.Fatalf("run_once policy runs = %d, want one", onceRuns.Load())
	}
}

func TestScheduleStartupSkipDoesNotBackfillCurrentSlot(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		return ExecutionResult{}
	})
	base := time.Date(2026, time.January, 2, 2, 0, 0, 0, time.UTC)
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "startup-skip", Cron: "0 * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "skip",
	}
	store := s.occurrenceStoreSnapshot()
	previous := ScheduleOccurrence{
		ID: ScheduleOccurrenceID("demo", "startup-skip", base), Project: "demo", Schedule: "startup-skip",
		ScheduledAt: base, Status: StatusSuccess,
	}
	if _, _, err := store.ClaimScheduleOccurrence(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	s.executeDueMode(context.Background(), schedule, base.Add(3*time.Hour+5*time.Second), true)
	if runs.Load() != 0 {
		t.Fatalf("startup skip runs = %d, want no backfill", runs.Load())
	}
	if latest, err := store.LatestScheduleOccurrence(context.Background(), "demo", "startup-skip"); err != nil || latest.Status != StatusSkipped {
		t.Fatalf("startup skip latest occurrence = %+v, err=%v, want skipped", latest, err)
	}
}

func TestFirstCronActivationIncludesCurrentSlot(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		return ExecutionResult{}
	})
	base := time.Date(2026, time.January, 2, 2, 0, 0, 0, time.UTC)
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "first", Cron: "0 * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job", Misfire: "skip",
	}
	s.executeDue(context.Background(), schedule, base.Add(5*time.Second))
	if runs.Load() != 1 {
		t.Fatalf("first cron activation runs = %d, want one", runs.Load())
	}
	occurrence, err := s.occurrenceStoreSnapshot().LatestScheduleOccurrence(context.Background(), "demo", "first")
	if err != nil || !occurrence.ScheduledAt.Equal(base) {
		t.Fatalf("first occurrence = %+v, err=%v, want scheduled at %v", occurrence, err, base)
	}
}

func TestScheduleOccurrenceIDsDistinguishInstants(t *testing.T) {
	first := time.Date(2026, time.November, 1, 1, 30, 0, 0, time.FixedZone("DST", -4*60*60))
	second := time.Date(2026, time.November, 1, 1, 30, 0, 0, time.FixedZone("STD", -5*60*60))
	if first.Equal(second) {
		t.Fatal("test instants unexpectedly equal")
	}
	if ScheduleOccurrenceID("demo", "fold", first) == ScheduleOccurrenceID("demo", "fold", second) {
		t.Fatal("different UTC instants share an occurrence id")
	}
}

func TestScheduleDueTimesUseDeterministicDSTRules(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult { return ExecutionResult{} })

	// robfig/cron's documented rule is to skip a wall-clock time that does not
	// exist during the spring-forward transition.
	spring := config.EffectiveSchedule{
		Project: "demo", Name: "spring", Cron: "30 2 * * *", Timezone: location,
	}
	springNow := time.Date(2026, time.March, 8, 4, 0, 0, 0, location)
	due, err := s.scheduleDueTimes(context.Background(), spring, springNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("spring-forward due times = %v, want skipped nonexistent 02:30", due)
	}

	// The fall-back repeated hour is represented by both deterministic UTC
	// instants; UTC is retained in the occurrence identity so both activations
	// remain independently claimable.
	fall := config.EffectiveSchedule{
		Project: "demo", Name: "fall", Cron: "30 1 * * *", Timezone: location,
	}
	previous := time.Date(2026, time.November, 1, 1, 30, 0, 0, location).Add(-24 * time.Hour)
	store := s.occurrenceStoreSnapshot()
	if _, _, err := store.ClaimScheduleOccurrence(context.Background(), ScheduleOccurrence{
		ID: ScheduleOccurrenceID("demo", "fall", previous), Project: "demo", Schedule: "fall",
		ScheduledAt: previous, Status: StatusSuccess,
	}); err != nil {
		t.Fatal(err)
	}
	fallNow := time.Date(2026, time.November, 1, 3, 0, 0, 0, location)
	due, err = s.scheduleDueTimes(context.Background(), fall, fallNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 2 || due[0].In(location).Hour() != 1 || due[0].In(location).Minute() != 30 ||
		due[1].In(location).Hour() != 1 || due[1].In(location).Minute() != 30 || due[0].Equal(due[1]) {
		t.Fatalf("fall-back due times = %v, want two distinct 01:30 instants", due)
	}
}
