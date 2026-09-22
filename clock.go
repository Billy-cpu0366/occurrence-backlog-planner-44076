package cron

import (
	"sync"
	"time"
)

// Clock is a source of the current time. All scheduling decisions read the
// time through the Cron's Clock, so scheduling can be replayed from any fixed
// starting point in tests by supplying a controllable clock instead of reading
// the system clock in several places.
type Clock interface {
	Now() time.Time
}

// clock is the internal extension of Clock used by the run loop. A clock that
// can be controlled (FakeClock) exposes a channel that is closed each time the
// clock is advanced; a wall clock has no such channel.
type wakefulClock interface {
	Clock
	changed() <-chan struct{}
}

// RealClock reports the current wall-clock time, interpreted in the given
// location.
type RealClock struct {
	Location *time.Location
}

// Now implements Clock.
func (c RealClock) Now() time.Time {
	loc := c.Location
	if loc == nil {
		loc = time.Local
	}
	return time.Now().In(loc)
}

func (c RealClock) changed() <-chan struct{} { return nil }

// FakeClock is a manually controlled Clock for deterministic tests. Time only
// moves when Set or Advance is called, and a Cron driven by the same FakeClock
// wakes up immediately when that happens.
//
// Replaying the same sequence of Advance calls from the same starting time
// produces the same schedule of occurrences.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
	ch  chan struct{}
}

// NewFakeClock returns a FakeClock initialized at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{
		now: t,
		ch:  make(chan struct{}),
	}
}

// Now returns the current simulated time. The returned time is never in the
// future relative to the most recent Set/Advance call.
func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Set moves the clock forward to t. It must not move backwards: setting an
// earlier time panics, because schedules are only defined for monotonic
// replay. All crons watching this clock are woken exactly once.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Before(c.now) {
		panic("cron: FakeClock cannot move backwards: " + t.String() + " < " + c.now.String())
	}
	if t.Equal(c.now) {
		return
	}
	c.now = t
	close(c.ch)
	c.ch = make(chan struct{})
}

// Advance moves the clock forward by d (d must be non-negative).
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d < 0 {
		panic("cron: FakeClock cannot advance by a negative duration")
	}
	if d == 0 {
		return
	}
	c.now = c.now.Add(d)
	close(c.ch)
	c.ch = make(chan struct{})
}

// changed returns the current wake channel. The channel is replaced (and the
// old one closed) on every Set/Advance.
func (c *FakeClock) changed() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ch
}
