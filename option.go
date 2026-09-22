package cron

import (
	"time"
)

// Option represents a modification to the default behavior of a Cron.
type Option func(*Cron)

// WithLocation overrides the timezone of the cron instance.
func WithLocation(loc *time.Location) Option {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds overrides the parser used for interpreting job schedules to
// include a seconds field as the first one.
func WithSeconds() Option {
	return WithParser(NewParser(
		Second | Minute | Hour | Dom | Month | Dow | Descriptor,
	))
}

// WithParser overrides the parser used for interpreting job schedules.
func WithParser(p ScheduleParser) Option {
	return func(c *Cron) {
		c.parser = p
	}
}

// WithChain specifies Job wrappers to apply to all jobs added to this cron.
// Refer to the Chain* functions in this package for provided wrappers.
func WithChain(wrappers ...JobWrapper) Option {
	return func(c *Cron) {
		c.chain = NewChain(wrappers...)
	}
}

// WithLogger uses the provided logger.
func WithLogger(logger Logger) Option {
	return func(c *Cron) {
		c.logger = logger
	}
}

// WithClock provides the source of "now" used by the scheduler. Every
// scheduling decision reads time from this clock, so passing a FakeClock makes
// the scheduler replay deterministically from any chosen starting point.
func WithClock(clock Clock) Option {
	return func(c *Cron) {
		c.clock = clock
	}
}

// WithCatchUp enables making up occurrences that were missed while the
// scheduler was stopped (or the process was down). On restart the missed
// occurrences are run in chronological order, at most maxCatchUp of them;
// older occurrences stay in the ledger as RunMissed so the debt is visible.
//
// A non-positive maxCatchUp disables catch-up (the default): after a restart
// the entry simply schedules from the current time.
func WithCatchUp(maxCatchUp int, ledger Ledger) Option {
	return func(c *Cron) {
		if maxCatchUp != 0 && ledger != nil {
			c.catchUp = maxCatchUp
			c.ledger = ledger
		}
	}
}

// WithOverlapPolicy sets what happens when an entry is due again while its
// previous run is still in flight. The default is OverlapAllow, matching the
// historical behavior of running each occurrence in its own goroutine.
func WithOverlapPolicy(policy OverlapPolicy) Option {
	return func(c *Cron) {
		c.overlap = policy
	}
}
