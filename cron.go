package cron

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *addedEntry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
	clock     Clock
	catchUp   int
	ledger    Ledger
	overlap   OverlapPolicy
	locks     *overlapLocks
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// addedEntry bundles an entry submitted while running with the clock reading
// taken at submission time. The run loop must schedule it from that snapshot:
// reading the clock again when the add is processed can skip occurrences due
// between submission and processing (deterministic clock replay makes that
// gap immediate and reproducible).
type addedEntry struct {
	entry *Entry
	added time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// cursor is the last instant already accounted for on this entry. It is
	// the lower bound (exclusive) when enumerating due occurrences and is the
	// only place the scheduler reads "how far it has gotten" for an entry.
	cursor time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *addedEntry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
		locks:     newOverlapLocks(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- &addedEntry{entry: entry, added: c.now()}
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	// Capture the clock synchronously: with a controllable clock the
	// goroutine may not be scheduled until after the caller has advanced time,
	// so reading the clock inside the goroutine would treat occurrences due in
	// between as already past.
	go c.run(c.now())
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	startTime := c.now()
	c.runningMu.Unlock()
	c.run(startTime)
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(startTime time.Time) {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := startTime
	for _, entry := range c.entries {
		c.initCursorAt(entry, now)
		entry.Next = c.nextOccurrence(entry, entry.cursor, now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Account for anything already due before sleeping. A controllable
		// clock may have advanced while this loop was not inside the select,
		// so readiness must be derived from the current time on every pass
		// rather than only from an edge-triggered wake channel.
		now = c.now()
		c.runDue(now)

		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		wait := 100000 * time.Hour
		if len(c.entries) > 0 && !c.entries[0].Next.IsZero() {
			wait = c.entries[0].Next.Sub(now)
			if wait < 0 {
				wait = 0
			}
		}
		// With a controllable clock the wall timer cannot be trusted: a
		// FakeClock leap of days costs zero real milliseconds. Arm a
		// near-zero timer in that case; the real arbitration is against the
		// clock-change channel. Rounding up avoids busy-looping on tiny
		// negative drifts.
		if _, fake := c.clock.(*FakeClock); fake {
			// The change channel is edge-triggered (a closed channel): a
			// clock step while this loop is not in the select is missed. A
			// fixed short poll bounds how long a missed edge goes unnoticed,
			// so replay steps are picked up deterministically without
			// busy-looping.
			const poll = 5 * time.Millisecond
			if wait > poll {
				wait = poll
			}
		}
		timer = time.NewTimer(wait)
		wakeCh := c.clockChanged()

		for {
			select {
			case <-timer.C:
				if wakeCh != nil {
					// Controllable clock: the wall timer is only a poll tick
					// (real time tells us nothing about simulated time), and
					// `now` must not be overwritten with wall time. Break out
					// of the select so the shared settle step below runs
					// against the current clock reading.
					break
				}
				// Re-read the time from the clock. With a FakeClock this may be
				// later than the timer's nominal firing, so due occurrences are
				// enumerated up to this instant rather than assumed.
				now = c.now().In(c.location)
				c.logger.Info("wake", "now", now)

			case <-wakeCh:
				// The injected clock moved (a replay step). Drain the timer and
				// process everything due at the new time.
				timer.Stop()
				now = c.now().In(c.location)
				c.logger.Info("wake", "now", now)

			case ae := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry := ae.entry
				c.initCursorAt(newEntry, ae.added)
				// Catch up occurrences that became due between submission and
				// processing at the current simulated time.
				c.entries = append(c.entries, newEntry)
				c.runDue(now)
				newEntry.Next = c.nextOccurrence(newEntry, newEntry.cursor, now)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
			}

			// Settle everything due under the current time before re-arming,
			// covering clock steps that raced with this select as well as
			// entries added/removed mid-flight.
			now = c.now().In(c.location)
			c.runDue(now)
			break
		}
	}
}

// initCursor establishes an entry's high-water mark. When a ledger is in use
// (catch-up mode) the persisted watermark is honored after a restart; entries
// that have never been accounted for start at "now", matching the historical
// behavior of scheduling the first run strictly in the future.
func (c *Cron) initCursorAt(entry *Entry, now time.Time) {
	if c.ledger != nil {
		if wm := c.ledger.Watermark(entry.ID); !wm.IsZero() {
			entry.cursor = wm.In(c.location)
			return
		}
	}
	entry.cursor = now
}

// runDue accounts for every occurrence due at or before now across all
// entries. It never starts an occurrence twice: the per-entry cursor is the
// exclusive lower bound and is advanced as occurrences are settled.
func (c *Cron) runDue(now time.Time) {
	for _, e := range c.entries {
		if e.Next.IsZero() || e.Next.After(now) {
			continue
		}
		occ := c.dueOccurrences(e, e.cursor, now)
		if len(occ) == 0 {
			// The clock moved without an occurrence (or an entry was added
			// exactly at an instant not strictly greater than the cursor).
			e.Next = c.nextOccurrence(e, e.cursor, now)
			continue
		}

		live, catchUp, missed := settleOccurrences(e.ID, occ, c.effectiveCatchUp())
		for _, t := range missed {
			c.record(e, t, RunMissed, time.Time{})
		}
		for _, t := range catchUp {
			c.fire(e, t, RunCatchUp, now)
		}
		c.fire(e, live, RunRan, now)

		e.cursor = live
		e.Prev = live
		e.Next = c.nextOccurrence(e, e.cursor, now)
		c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
	}
}

// effectiveCatchUp returns the maximum number of missed occurrences made up
// in order; -1 means an unlimited backlog.
func (c *Cron) effectiveCatchUp() int {
	if c.ledger == nil {
		return 0
	}
	return c.catchUp
}

// dueOccurrences returns every occurrence of entry strictly after cursor and
// at or before now.
//
// The classic Schedule.Next(cursor) gives the first occurrence as understood
// without spring-forward compensation. Enumerating the small interval up to
// that candidate does two things at once:
//
//   - it stays cheap for dense schedules (a per-second job enumerates a
//     one-second interval instead of the whole scheduling horizon), and
//   - on a spring-forward night the enumerator places a nonexistent nominal
//     time at the start of the skipped gap, earlier than the candidate, so
//     that occurrence is visible as due as soon as the day is executable.
//
// Pages are produced until the frontier passes now, so long scheduler gaps
// (catch-up after downtime) are covered regardless of their length.
func (c *Cron) dueOccurrences(entry *Entry, cursor, now time.Time) []time.Time {
	var all []time.Time
	for {
		candidate := entry.Schedule.Next(cursor)
		if candidate.IsZero() || candidate.After(now) {
			// No classic occurrence due, but a spring-forward-compensated
			// instant can precede the candidate.
			if candidate.After(now) {
				if occ := Occurrences(entry.Schedule, cursor, candidate); len(occ) > 0 &&
					!occ[0].After(now) {
					all = append(all, occ[0])
				}
			}
			return all
		}
		// The interval (cursor, candidate] contains the classic candidate
		// and, on DST nights, the compensated instants that the classic Next
		// skips. It never contains another ordinary recurrence, so it is a
		// single page.
		page := Occurrences(entry.Schedule, cursor, candidate)
		for _, t := range page {
			if t.After(cursor) && !t.After(now) {
				all = append(all, t)
			}
		}
		cursor = candidate
		if len(all) >= maxOccurrences {
			return all
		}
	}
}

// nextOccurrence returns the next activation strictly after cursor.
func (c *Cron) nextOccurrence(entry *Entry, cursor, now time.Time) time.Time {
	candidate := entry.Schedule.Next(cursor)
	if candidate.IsZero() {
		return candidate
	}
	// Pick up a spring-forward-compensated instant when it precedes the
	// classic candidate.
	if occ := Occurrences(entry.Schedule, cursor, candidate); len(occ) > 0 {
		return occ[0]
	}
	return candidate
}

// record writes a settled occurrence to the ledger.
func (c *Cron) record(entry *Entry, scheduled time.Time, status RunStatus, ranAt time.Time) {
	if c.ledger == nil {
		return
	}
	c.ledger.Append(RunRecord{
		EntryID:   entry.ID,
		Scheduled: scheduled,
		Status:    status,
		RanAt:     ranAt,
	})
}

// fire starts one occurrence, applying the entry's overlap policy and
// recording the outcome. Every scheduled occurrence reaches exactly one
// outcome (ran, catchup, or skipped).
func (c *Cron) fire(entry *Entry, scheduled time.Time, status RunStatus, now time.Time) {
	job := entry.WrappedJob
	if c.overlap == OverlapSkip || c.overlap == OverlapWait {
		lock := c.locks.get(entry.ID)
		if c.overlap == OverlapSkip {
			got := lock.tryLock()
			if !got {
				c.record(entry, scheduled, RunSkipped, time.Time{})
				c.logger.Info("skip", "entry", entry.ID, "scheduled", scheduled)
				return
			}
			c.record(entry, scheduled, status, now)
			c.startGuardedJob(job, lock)
			return
		}
		// OverlapWait: block the scheduler thread for this entry until the
		// previous run returns. The deferred unlock also runs if the job
		// panics (the configured Recover wrapper keeps that contained), so an
		// entry can never hold its slot forever.
		lock.lock()
		c.record(entry, scheduled, status, now)
		c.startGuardedJob(job, lock)
		return
	}
	c.record(entry, scheduled, status, now)
	c.startJob(job)
}

// startGuardedJob runs the job in a new goroutine, releasing the slot when it
// returns.
func (c *Cron) startGuardedJob(j Job, lock *entryLock) {
	c.startJob(FuncJob(func() {
		defer lock.unlock()
		j.Run()
	}))
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location. Every scheduling decision reads
// the time through this method (and nowhere else), so a single injected clock
// fully determines what the scheduler does.
func (c *Cron) now() time.Time {
	if c.clock != nil {
		return c.clock.Now().In(c.location)
	}
	return time.Now().In(c.location)
}

// changed returns the wake channel of a controllable clock, or nil for a wall
// clock.
func (c *Cron) clockChanged() <-chan struct{} {
	if wc, ok := c.clock.(wakefulClock); ok {
		return wc.changed()
	}
	return nil
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}
