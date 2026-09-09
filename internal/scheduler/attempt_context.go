package scheduler

import "context"

type attemptContextKey struct{}

// WithAttemptNumber attaches the scheduler-level attempt number to an
// execution context. It is an internal coordination detail used by runners
// that need to isolate per-attempt output files.
func WithAttemptNumber(ctx context.Context, number int) context.Context {
	if number < 1 {
		number = 1
	}
	return context.WithValue(ctx, attemptContextKey{}, number)
}

// AttemptNumber returns the scheduler-level attempt number in ctx. Direct
// compatibility callers that do not provide one represent their first
// attempt.
func AttemptNumber(ctx context.Context) int {
	number, ok := ctx.Value(attemptContextKey{}).(int)
	if !ok || number < 1 {
		return 1
	}
	return number
}
