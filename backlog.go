package cron

import (
	"sync"
	"time"
)

// OverlapPolicy decides what happens when an entry's next occurrence is due
// while the previous run of that same entry has not returned yet.
type OverlapPolicy int

const (
	// OverlapAllow preserves the default cron behavior: every due occurrence
	// starts its own goroutine, even if the previous one is still running.
	OverlapAllow OverlapPolicy = iota
	// OverlapSkip drops the newer occurrence. The skipped occurrence is
	// recorded in the ledger.
	OverlapSkip
	// OverlapWait delays the newer occurrence until the previous run
	// completes. The lock is always released, even if the running job panics,
	// so an entry can never get stuck holding its slot.
	OverlapWait
)

// entryLock is a single-slot non-blocking lock. The buffered channel either
// holds one token (unlocked) or is empty (locked).
type entryLock struct {
	ch chan struct{}
}

func newEntryLock() *entryLock {
	l := &entryLock{ch: make(chan struct{}, 1)}
	l.ch <- struct{}{}
	return l
}

// tryLock takes the slot if it is free.
func (l *entryLock) tryLock() bool {
	select {
	case <-l.ch:
		return true
	default:
		return false
	}
}

// lock blocks until the slot is free and then takes it.
func (l *entryLock) lock() { <-l.ch }

// unlock returns the slot. It must be called once per successful lock.
func (l *entryLock) unlock() { l.ch <- struct{}{} }

// overlapLocks hands out one lock per entry.
type overlapLocks struct {
	mu    sync.Mutex
	locks map[EntryID]*entryLock
}

func newOverlapLocks() *overlapLocks {
	return &overlapLocks{locks: make(map[EntryID]*entryLock)}
}

func (o *overlapLocks) get(id EntryID) *entryLock {
	o.mu.Lock()
	defer o.mu.Unlock()
	l := o.locks[id]
	if l == nil {
		l = newEntryLock()
		o.locks[id] = l
	}
	return l
}

// settleOccurrences accounts for the occurrences of an entry that are due at
// or before now. The most recent occurrence is the live firing; any earlier
// occurrences are missed work that is caught up in order, up to limit.
//
// Occurrences older than the catch-up window are returned as missed so the
// remaining debt stays visible instead of being silently discarded.
//
// If limit is negative the whole backlog is caught up; if zero the backlog is
// missed entirely except for the live firing.
func settleOccurrences(id EntryID, occ []time.Time, limit int) (live time.Time, catchUp, missed []time.Time) {
	if len(occ) == 0 {
		return time.Time{}, nil, nil
	}
	live = occ[len(occ)-1]
	backlog := occ[:len(occ)-1]
	if limit < 0 {
		return live, backlog, nil
	}
	if len(backlog) > limit {
		missed = backlog[:len(backlog)-limit]
		catchUp = backlog[len(backlog)-limit:]
	} else {
		catchUp = backlog
	}
	return live, catchUp, missed
}
