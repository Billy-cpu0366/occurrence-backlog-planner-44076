package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor blocks until cond is true or the timeout elapses. FakeClock-driven
// runs still execute jobs in goroutines, so assertions on counts wait briefly.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func replayCron(t *testing.T, start time.Time, opts ...Option) (*Cron, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(start)
	all := append([]Option{WithParser(secondParser), WithChain(), WithClock(clock)}, opts...)
	c := New(all...)
	return c, clock
}

// The same start replayed twice produces exactly the same schedule of firings.
func TestReplayDeterministic(t *testing.T) {
	spec := "0 30 2 * * *"
	start := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	run := func() []time.Time {
		c, clock := replayCron(t, start)
		var mu sync.Mutex
		var fired []time.Time
		c.AddFunc(spec, func() {
			mu.Lock()
			fired = append(fired, clock.Now())
			mu.Unlock()
		})
		c.Start()
		defer c.Stop()
		for i := 0; i < 10; i++ {
			clock.Advance(24 * time.Hour)
		}
		waitFor(2*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(fired) >= 10
		})
		mu.Lock()
		defer mu.Unlock()
		out := make([]time.Time, len(fired))
		copy(out, fired)
		return out
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("replay length differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			t.Fatalf("replay differs at %d: %v vs %v", i, a[i], b[i])
		}
	}
}

// No scheduling path may read a different clock than the injected one: while
// driven by a FakeClock fixed in 2025, wall-clock time must never leak in.
func TestFakeClockIsSingleSourceOfTime(t *testing.T) {
	fixed := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	c, clock := replayCron(t, fixed)
	var calls int64
	var mu sync.Mutex
	c.AddFunc("0 0 * * * *", func() {
		mu.Lock()
		calls++
		mu.Unlock()
	})
	c.Start()
	defer c.Stop()
	realSleep(250 * time.Millisecond)
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 0 {
		t.Fatalf("job fired %d times without the clock advancing", got)
	}
	clock.Advance(time.Hour)
	if !waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 1
	}) {
		t.Fatalf("expected 1 firing after advancing the clock, got %d", calls)
	}
}

func realSleep(d time.Duration) { time.Sleep(d) }

// Enumerating occurrences over the 2025 US DST transitions:
//   - the skipped 02:30 on 2025-03-09 is made up at 03:00 EDT (gap start),
//     the first executable instant of that day, not on 03-10;
//   - the doubled 01:30 on 2025-11-02 is counted exactly once.
func TestOccurrencesDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("timezone data unavailable:", err)
	}
	sched, err := secondParser.Parse("CRON_TZ=America/New_York 0 30 2 * * ?")
	if err != nil {
		t.Fatal(err)
	}
	spring := time.Date(2025, 3, 8, 0, 0, 0, 0, loc)
	occ := Occurrences(sched, spring, time.Date(2025, 3, 12, 0, 0, 0, 0, loc))
	var got time.Time
	for _, o := range occ {
		if o.In(loc).Month() == time.March && o.In(loc).Day() == 9 {
			got = o.In(loc)
		}
	}
	if got.IsZero() {
		t.Fatal("no occurrence for the spring-forward day")
	}
	if got.Hour() != 3 || got.Minute() != 0 {
		t.Errorf("skipped slot should be made up at the gap start 03:00, got %v", got)
	}
	if got.Day() != 9 {
		t.Errorf("make-up must happen on the same day, got %v", got)
	}

	fall := time.Date(2025, 11, 1, 0, 0, 0, 0, loc)
	occ = Occurrences(sched, fall, time.Date(2025, 11, 3, 0, 0, 0, 0, loc))
	var count int
	for _, o := range occ {
		if o.In(loc).Month() == time.November && o.In(loc).Day() == 2 {
			count++
		}
	}
	if count != 1 {
		t.Errorf("repeated fall-back slot counted %d times, want 1", count)
	}
}

// The runner itself catches the spring-forward slot on the same simulated day.
func TestRunnerSpringForwardSameDay(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("timezone data unavailable:", err)
	}
	start := time.Date(2025, 3, 9, 0, 0, 0, 0, loc)
	ledger := NewMemoryLedger()
	c, clock := replayCron(t, start, WithLocation(loc),
		WithCatchUp(-1, ledger))
	var fired int64
	var mu sync.Mutex
	c.AddFunc("0 30 2 * * ?", func() {
		mu.Lock()
		atomic.AddInt64(&fired, 1)
		mu.Unlock()
	})
	c.Start()
	defer c.Stop()

	clock.Set(time.Date(2025, 3, 9, 4, 0, 0, 0, loc))
	if !waitFor(2*time.Second, func() bool {
		hist := ledger.History(EntryID(1))
		return len(hist) == 1
	}) {
		t.Fatalf("expected same-day make-up, got %d fired", fired)
	}
	// The scheduled (planned) instant is the gap start, 03:00, even though
	// the job goroutine actually executes at the replayed time (04:00).
	got := ledger.History(EntryID(1))[0].Scheduled.In(loc)
	if got.Day() != 9 || got.Hour() != 3 || got.Minute() != 0 {
		t.Fatalf("expected make-up scheduled at 03:00 on Mar 9, got %v", got)
	}

	// Advancing to the next day must not produce a second firing for the 9th.
	clock.Set(time.Date(2025, 3, 10, 4, 0, 0, 0, loc))
	if !waitFor(2*time.Second, func() bool {
		return len(ledger.History(EntryID(1))) == 2
	}) {
		t.Fatalf("expected the regular Mar 10 run, got %v", ledger.History(EntryID(1)))
	}
	next := ledger.History(EntryID(1))[1].Scheduled.In(loc)
	if next.Day() != 10 || next.Hour() != 2 {
		t.Fatalf("expected regular Mar 10 02:30 run, got %v", next)
	}
}

// The doubled fall-back slot fires only once.
func TestRunnerFallBackOnce(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("timezone data unavailable:", err)
	}
	start := time.Date(2025, 11, 2, 0, 0, 0, 0, loc)
	c, clock := replayCron(t, start, WithLocation(loc))
	var mu sync.Mutex
	var fired []time.Time
	c.AddFunc("0 30 1 * * ?", func() {
		mu.Lock()
		fired = append(fired, clock.Now().In(loc))
		mu.Unlock()
	})
	c.Start()
	defer c.Stop()
	clock.Set(time.Date(2025, 11, 2, 4, 0, 0, 0, loc))
	waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fired) >= 1
	})
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 1 {
		t.Fatalf("fall-back slot fired %d times: %v", len(fired), fired)
	}
}

// Cron expressions, descriptors and CRON_TZ specs all enumerate through the
// same code path, including month-end and leap day.
func TestOccurrenceAlignment(t *testing.T) {
	utc := time.UTC
	// Leap day: 2024 has Feb 29, 2025 does not; enumeration skips 2025
	// cleanly without drifting to Mar 1.
	sched, err := secondParser.Parse("0 0 0 29 Feb ?")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2024, 2, 1, 0, 0, 0, 0, utc)
	occ := Occurrences(sched, from, time.Date(2029, 1, 1, 0, 0, 0, 0, utc))
	want := []time.Time{
		time.Date(2024, 2, 29, 0, 0, 0, 0, utc),
		time.Date(2028, 2, 29, 0, 0, 0, 0, utc),
	}
	if len(occ) != len(want) {
		t.Fatalf("leap-day occurrences = %v, want %v", occ, want)
	}
	for i := range want {
		if !occ[i].Equal(want[i]) {
			t.Fatalf("leap-day occurrence %d = %v, want %v", i, occ[i], want[i])
		}
	}

	// Month-end style schedule never slips into the next month.
	sched, err = secondParser.Parse("0 0 0 31 * ?")
	if err != nil {
		t.Fatal(err)
	}
	occ = Occurrences(sched, time.Date(2025, 1, 1, 0, 0, 0, 0, utc),
		time.Date(2025, 6, 1, 0, 0, 0, 0, utc))
	for _, o := range occ {
		if o.Day() == 1 {
			t.Fatalf("month-end occurrence drifted into the next month: %v", o)
		}
	}

	// Descriptor.
	sched, err = standardParser.Parse("@daily")
	if err != nil {
		t.Fatal(err)
	}
	occ = Occurrences(sched, time.Date(2025, 1, 1, 0, 0, 0, 0, utc),
		time.Date(2025, 1, 4, 0, 0, 0, 0, utc))
	if len(occ) != 3 {
		t.Fatalf("@daily over 3 days = %d occurrences", len(occ))
	}
}

// @every descriptors enumerate through the same path as cron expressions and
// advance with the simulated clock, not wall time.
func TestEveryWithFakeClock(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c, clock := replayCron(t, start, WithLocation(time.UTC))
	var mu sync.Mutex
	var fired []time.Time
	c.AddFunc("@every 90s", func() {
		mu.Lock()
		fired = append(fired, clock.Now())
		mu.Unlock()
	})
	c.Start()
	defer c.Stop()
	step := func(want int) {
		t.Helper()
		clock.Advance(90 * time.Second)
		if !waitFor(2*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(fired) == want
		}) {
			t.Fatalf("expected %d firings after step, got %d", want, len(fired))
		}
	}
	step(1)
	step(2)
	step(3)
	if !waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fired) == 3
	}) {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("expected 3 firings, got %d", len(fired))
	}
	mu.Lock()
	defer mu.Unlock()
	for i, f := range fired {
		want := start.Add(time.Duration(i+1) * 90 * time.Second)
		if !f.Equal(want) {
			t.Fatalf("firing %d at %v, want %v", i, f, want)
		}
	}
}
