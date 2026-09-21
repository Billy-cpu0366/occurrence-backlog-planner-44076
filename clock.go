package cron

import "time"

// Clock is the source of wall-clock time used by the scheduler. Production use
// relies on SystemClock; tests substitute a controllable clock to replay
// schedules deterministically from an arbitrary starting instant.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the operating system clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time {
	return time.Now()
}

// FakeClock is a manually advanced Clock. Every scheduling decision is derived
// from the instant last supplied via a call to Now (see Cron.Now), so a replay
// started from the same instant always produces the same activation list.
type FakeClock struct {
	now time.Time
}

// NewFakeClock returns a FakeClock positioned at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the current simulated time.
func (c *FakeClock) Now() time.Time {
	return c.now
}

// Set moves the simulated clock to t (which must not be earlier than the
// current simulated time) and returns the new instant.
func (c *FakeClock) Set(t time.Time) time.Time {
	c.now = t
	return c.now
}

// Advance moves the simulated clock forward by d and returns the new instant.
func (c *FakeClock) Advance(d time.Duration) time.Time {
	c.now = c.now.Add(d)
	return c.now
}
