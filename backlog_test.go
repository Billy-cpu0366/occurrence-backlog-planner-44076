package cron

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// settleOccurrences: live firing is newest; backlog is first-in-first-out and
// bounded; overflow remains visible as missed.
func TestSettleOccurrences(t *testing.T) {
	ts := func(n int) []time.Time {
		var out []time.Time
		for i := 1; i <= n; i++ {
			out = append(out, time.Unix(int64(i), 0).UTC())
		}
		return out
	}
	live, catchUp, missed := settleOccurrences(1, ts(5), 2)
	if !live.Equal(time.Unix(5, 0).UTC()) {
		t.Fatalf("live = %v", live)
	}
	if len(catchUp) != 2 || !catchUp[0].Equal(time.Unix(3, 0).UTC()) {
		t.Fatalf("catchUp must keep the newest 2 in FIFO order, got %v", catchUp)
	}
	if len(missed) != 2 || !missed[0].Equal(time.Unix(1, 0).UTC()) {
		t.Fatalf("overflow must remain visible and oldest-first, got %v", missed)
	}

	// Unlimited and zero limits.
	_, catchUp, missed = settleOccurrences(1, ts(4), -1)
	if len(catchUp) != 3 || len(missed) != 0 {
		t.Fatalf("unexpected unlimited result: %v %v", catchUp, missed)
	}
	_, catchUp, missed = settleOccurrences(1, ts(4), 0)
	if len(catchUp) != 0 || len(missed) != 3 {
		t.Fatalf("unexpected zero-limit result: %v %v", catchUp, missed)
	}
}

// After a stop/start gap, missed occurrences are caught up in order up to the
// cap, the oldest ones stay booked as missed, and the live firing adds exactly
// one run (no phantom occurrence on the first trigger).
func TestCatchUpAfterRestart(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(
		WithParser(secondParser), WithChain(), WithLocation(time.UTC),
		WithClock(clock),
		WithCatchUp(2, ledger),
	)
	var execMu sync.Mutex
	var execCount int
	c.AddFunc("0 0 * * * *", func() {
		execMu.Lock()
		execCount++
		execMu.Unlock()
	})
	execCountAtLeast := func(n int) bool {
		execMu.Lock()
		defer execMu.Unlock()
		return execCount >= n
	}
	c.Start()

	// Run at 01:00, then stop.
	clock.Set(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool {
		return len(ledger.History(EntryID(1))) == 1 && execCountAtLeast(1)
	}) {
		t.Fatal("01:00 run did not execute")
	}
	c.Stop()

	// Restart at 06:00: 02:00..05:00 were missed, 06:00 is the live firing.
	// Cap is 2, so 04:00 and 05:00 are caught up (in order), 02:00/03:00 stay
	// missed. The first trigger must not add an extra run.
	clock.Set(time.Date(2025, 1, 1, 6, 0, 0, 0, time.UTC))
	c.Start()
	defer c.Stop()
	if !waitFor(2*time.Second, func() bool {
		return len(ledger.History(EntryID(1))) == 6
	}) {
		t.Fatalf("expected 6 settled occurrences, got %d", len(ledger.History(EntryID(1))))
	}
	// All executed jobs (1 prior + 2 catch-up + 1 live) must have run.
	if !waitFor(2*time.Second, func() bool { return execCountAtLeast(4) }) {
		execMu.Lock()
		defer execMu.Unlock()
		t.Fatalf("only %d of 4 jobs executed", execCount)
	}
	// The scheduled order in the ledger is deterministic.
	var scheduled []int
	for _, r := range ledger.History(EntryID(1)) {
		if r.Status == RunRan || r.Status == RunCatchUp {
			scheduled = append(scheduled, r.Scheduled.Hour())
		}
	}
	for i, h := range []int{1, 4, 5, 6} {
		if scheduled[i] != h {
			t.Fatalf("run %d scheduled at %d:00, want %d:00", i, scheduled[i], h)
		}
	}

	id := EntryID(1)
	hist := ledger.History(id)
	statusAt := map[int]RunStatus{}
	for _, r := range hist {
		statusAt[r.Scheduled.Hour()] = r.Status
	}
	if statusAt[2] != RunMissed || statusAt[3] != RunMissed {
		t.Errorf("02:00/03:00 must stay missed, got %v", statusAt)
	}
	if statusAt[4] != RunCatchUp || statusAt[5] != RunCatchUp {
		t.Errorf("04:00/05:00 must be catch-up, got %v", statusAt)
	}
	if statusAt[6] != RunRan {
		t.Errorf("06:00 must be the live run, got %v", statusAt[6])
	}
	if pending := ledger.PendingMissed(id); len(pending) != 2 {
		t.Errorf("remaining debt = %d, want 2", len(pending))
	}

	// No occurrence is caught up twice: advancing further only adds one new
	// settled occurrence.
	clock.Set(time.Date(2025, 1, 1, 7, 0, 0, 0, time.UTC))
	waitFor(2*time.Second, func() bool {
		return len(ledger.History(EntryID(1))) == 7
	})
	if !waitFor(2*time.Second, func() bool { return execCountAtLeast(5) }) {
		t.Fatal("07:00 run did not execute")
	}
	time.Sleep(200 * time.Millisecond)
	execMu.Lock()
	defer execMu.Unlock()
	if execCount != 5 {
		t.Fatalf("an occurrence was settled twice: %d executions", execCount)
	}
}

// A JSON ledger written by one cron is read back by a fresh cron which keeps
// the same books after a process restart.
func TestJSONLedgerRestart(t *testing.T) {
	dir, err := ioutil.TempDir("", "cron-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "ledger.json")

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger, err := NewJSONLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	c := New(WithParser(secondParser), WithChain(), WithLocation(time.UTC),
		WithClock(clock), WithCatchUp(1, ledger))
	var mu sync.Mutex
	var ran []int
	c.AddFunc("0 0 * * * *", func() {
		mu.Lock()
		ran = append(ran, clock.Now().Hour())
		mu.Unlock()
	})
	c.Start()
	clock.Set(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC))
	waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ran) == 1
	})
	c.Stop()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ledger not persisted: %v", err)
	}

	// New process: reload ledger and clock, resume at 04:00. Only 03:00 is
	// caught up (cap 1); 02:00 is missed; 04:00 runs live.
	ledger2, err := NewJSONLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	clock2 := NewFakeClock(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC))
	c2 := New(WithParser(secondParser), WithChain(), WithLocation(time.UTC),
		WithClock(clock2), WithCatchUp(1, ledger2))
	c2.AddFunc("0 0 * * * *", func() {})
	c2.Start()
	defer c2.Stop()
	clock2.Set(time.Date(2025, 1, 1, 4, 0, 0, 0, time.UTC))
	waitFor(2*time.Second, func() bool {
		return ledger2.Watermark(EntryID(1)).Hour() == 4
	})
	statusAt := map[int]RunStatus{}
	for _, r := range ledger2.History(EntryID(1)) {
		statusAt[r.Scheduled.Hour()] = r.Status
	}
	if statusAt[2] != RunMissed || statusAt[3] != RunCatchUp || statusAt[4] != RunRan {
		t.Fatalf("restarted books wrong: %v", statusAt)
	}
}

// OverlapSkip: a slow run holds the slot; the due occurrence is skipped and
// recorded; after the slow run returns the slot frees and the next run fires
// normally (a crash/panic does not keep the slot locked).
func TestOverlapSkip(t *testing.T) {
	start := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(WithParser(secondParser), WithChain(Recover(DiscardLogger)),
		WithLocation(time.UTC), WithClock(clock),
		WithCatchUp(-1, ledger), WithOverlapPolicy(OverlapSkip))

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	var doneMu sync.Mutex
	var doneCount int
	c.AddFunc("0 * * * * *", func() {
		started <- struct{}{}
		<-release
		doneMu.Lock()
		doneCount++
		doneMu.Unlock()
	})
	c.Start()
	defer c.Stop()

	clock.Set(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	select {
	case <-started: // 00:00 running
	case <-time.After(2 * time.Second):
		t.Fatal("00:00 run never started")
	}
	// Advance one minute at a time and let the loop settle each step; with a
	// simulated clock the wall timer polls, so rapid leaps inside one poll
	// would otherwise be merged.
	waitSkip := func(n int) {
		t.Helper()
		if !waitFor(2*time.Second, func() bool {
			skips := 0
			for _, r := range ledger.History(EntryID(1)) {
				if r.Status == RunSkipped {
					skips++
				}
			}
			return skips == n
		}) {
			t.Fatalf("expected %d skipped occurrences", n)
		}
	}
	clock.Set(time.Date(2025, 1, 1, 0, 1, 0, 0, time.UTC))
	waitSkip(1)
	clock.Set(time.Date(2025, 1, 1, 0, 2, 0, 0, time.UTC))
	waitSkip(2)
	hist := ledger.History(EntryID(1))
	skips := 0
	for _, r := range hist {
		if r.Status == RunSkipped {
			skips++
		}
	}
	if skips != 2 {
		t.Fatalf("expected 2 skipped occurrences, got %d: %v", skips, hist)
	}
	close(release)
	clock.Set(time.Date(2025, 1, 1, 0, 3, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("slot never freed for the next run")
	}
}

// OverlapWait: the due occurrence waits for the previous run to finish, then
// runs; every occurrence eventually fires exactly once.
func TestOverlapWait(t *testing.T) {
	start := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(WithParser(secondParser), WithChain(Recover(DiscardLogger)),
		WithLocation(time.UTC), WithClock(clock),
		WithCatchUp(-1, ledger), WithOverlapPolicy(OverlapWait))
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	c.AddFunc("0 * * * * *", func() {
		started <- struct{}{}
		<-release
	})
	c.Start()
	defer c.Stop()
	clock.Set(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	<-started
	// The 00:01 occurrence must wait, not skip.
	clock.Set(time.Date(2025, 1, 1, 0, 1, 0, 0, time.UTC))
	time.Sleep(300 * time.Millisecond)
	for _, r := range ledger.History(EntryID(1)) {
		if r.Status == RunSkipped {
			t.Fatalf("OverlapWait must not skip: %v", r)
		}
	}
	select {
	case <-started:
		t.Fatal("waited occurrence must not start until the slot frees")
	default:
	}
	// Once the first run returns, the waiting occurrence starts. Note the
	// scheduler goroutine processes OverlapWait by blocking on the slot.
	close(release)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting occurrence never started after slot freed")
	}
}

// A panicking job must not keep its entry's slot locked forever.
func TestPanicReleasesSlot(t *testing.T) {
	start := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(WithParser(secondParser), WithChain(Recover(DiscardLogger)),
		WithLocation(time.UTC), WithClock(clock),
		WithCatchUp(-1, ledger), WithOverlapPolicy(OverlapSkip))
	var shouldPanic int32
	var runs int64
	var mu sync.Mutex
	c.AddFunc("0 * * * * *", func() {
		mu.Lock()
		p := atomic.LoadInt32(&shouldPanic) == 1
		atomic.AddInt64(&runs, 1)
		mu.Unlock()
		if p {
			panic("boom")
		}
	})
	c.Start()
	defer c.Stop()
	clock.Set(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&runs) == 1 }) {
		t.Fatal("first run did not happen")
	}
	atomic.StoreInt32(&shouldPanic, 1)
	clock.Set(time.Date(2025, 1, 1, 0, 1, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&runs) == 2 }) {
		t.Fatal("panicking run did not fire")
	}
	// Slot must be free: the next minute runs normally and is not skipped.
	atomic.StoreInt32(&shouldPanic, 0)
	clock.Set(time.Date(2025, 1, 1, 0, 2, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&runs) == 3 }) {
		t.Fatal("slot stayed locked after a panic")
	}
	time.Sleep(200 * time.Millisecond)
	for _, r := range ledger.History(EntryID(1)) {
		if r.Status == RunSkipped {
			t.Fatalf("panic must not cause a skip: %v", r)
		}
	}
}

// When the downtime exceeds the catch-up cap, the occurrences that ARE made up
// are the newest (most relevant) ones, and the discarded older ones remain
// bookable as RunMissed rather than disappearing.
func TestLongDowntimeKeepsNewestAndShowsDebt(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(WithParser(secondParser), WithChain(), WithLocation(time.UTC),
		WithClock(clock), WithCatchUp(2, ledger))
	c.AddFunc("0 0 * * * *", func() {})
	c.Start()
	defer c.Stop()
	// Down for a whole day; 23 backlog occurrences + the live run. The two
	// made up must be the freshest (22:00, 23:00); 01:00..21:00 stay missed.
	clock.Set(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool {
		return len(ledger.History(EntryID(1))) == 24
	}) {
		t.Fatalf("expected 24 settled, got %d", len(ledger.History(EntryID(1))))
	}
	var caught, missed []int
	for _, r := range ledger.History(EntryID(1)) {
		switch r.Status {
		case RunCatchUp:
			caught = append(caught, r.Scheduled.Hour())
		case RunMissed:
			missed = append(missed, r.Scheduled.Hour())
		}
	}
	if len(caught) != 2 || caught[0] != 22 || caught[1] != 23 {
		t.Fatalf("newest backlog must be caught up, got %v", caught)
	}
	if len(missed) != 21 || missed[0] != 1 || missed[len(missed)-1] != 21 {
		t.Fatalf("older debt must stay visible in order, got %v", missed)
	}
	if len(ledger.PendingMissed(EntryID(1))) != 21 {
		t.Fatal("PendingMissed must expose the remaining debt")
	}
}

// The first trigger after resume does not add a phantom occurrence: resuming
// exactly at a scheduled instant runs that instant once only.
func TestResumeNoPhantom(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	ledger := NewMemoryLedger()
	c := New(WithParser(secondParser), WithChain(), WithLocation(time.UTC),
		WithClock(clock), WithCatchUp(5, ledger))
	var runs int64
	c.AddFunc("0 0 * * * *", func() { atomic.AddInt64(&runs, 1) })
	c.Start()
	// Run at 01:00, then stop while down.
	clock.Set(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC))
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&runs) == 1 }) {
		t.Fatal("01:00 run did not happen")
	}
	c.Stop()
	// Resume exactly at 03:00: 02:00 caught up, 03:00 live, nothing extra.
	clock.Set(time.Date(2025, 1, 1, 3, 0, 0, 0, time.UTC))
	c.Start()
	defer c.Stop()
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&runs) == 3 }) {
		t.Fatalf("expected 01:00 prior + 02:00 catch-up + 03:00 live = 3 runs, got %d", atomic.LoadInt64(&runs))
	}
	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt64(&runs) != 3 {
		t.Fatalf("phantom run after resume: %d", atomic.LoadInt64(&runs))
	}
}
