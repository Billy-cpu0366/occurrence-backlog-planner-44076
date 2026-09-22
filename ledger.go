package cron

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// RunStatus is the way a scheduled occurrence was settled.
type RunStatus string

const (
	// RunRan is an occurrence that fired at its scheduled time.
	RunRan RunStatus = "ran"
	// RunCatchUp is a missed occurrence that fired after the scheduler
	// returned to service, within the configured catch-up limit.
	RunCatchUp RunStatus = "catchup"
	// RunSkipped is an occurrence that was not started because the previous
	// run of the same entry had not finished (OverlapSkip).
	RunSkipped RunStatus = "skipped"
	// RunMissed is an occurrence that could not be made up: the scheduler was
	// down when it was due and more than the catch-up limit of occurrences
	// had accumulated. These records keep the backlog visible.
	RunMissed RunStatus = "missed"
)

// RunRecord is the ledger entry for one scheduled occurrence.
type RunRecord struct {
	// EntryID is the entry the occurrence belongs to.
	EntryID EntryID `json:"entry_id"`
	// Scheduled is the instant the occurrence was nominally due.
	Scheduled time.Time `json:"scheduled"`
	// Status is how the occurrence was settled.
	Status RunStatus `json:"status"`
	// RanAt is the instant the job actually started (zero for skipped/missed).
	RanAt time.Time `json:"ran_at,omitempty"`
}

// Ledger is the durable bookkeeping for occurrences. It answers two
// questions for each entry:
//
//   - what is the last instant that was already accounted for (the
//     high-water mark used to find missed occurrences after a restart), and
//   - what happened to each occurrence (ran / caught up / skipped / missed).
//
// Implementations must be safe for concurrent use.
type Ledger interface {
	// Watermark returns the last accounted-for instant for the entry, or the
	// zero time if the entry has never been accounted for.
	Watermark(id EntryID) time.Time
	// SetWatermark records the high-water mark. It must not move backwards.
	SetWatermark(id EntryID, t time.Time)
	// Append records how an occurrence was settled, and advances the
	// high-water mark to the occurrence's scheduled instant.
	Append(rec RunRecord)
	// History returns the recorded occurrences of an entry, ordered by
	// scheduled time.
	History(id EntryID) []RunRecord
	// PendingMissed returns occurrences that could not be made up and are
	// still owed, newest first is not required; order is by scheduled time.
	PendingMissed(id EntryID) []RunRecord
	// Snapshot returns a copy of all records, grouped by entry ID.
	Snapshot() map[EntryID][]RunRecord
}

// entryLedger is the per-entry bookkeeping state.
type entryLedger struct {
	Watermark time.Time   `json:"watermark"`
	Records   []RunRecord `json:"records"`
}

// MemoryLedger keeps the books in memory. It is safe for concurrent use but
// does not survive process restarts; use JSONLedger for that.
type MemoryLedger struct {
	mu      sync.Mutex
	entries map[EntryID]*entryLedger
}

// NewMemoryLedger returns an empty in-memory ledger.
func NewMemoryLedger() *MemoryLedger {
	return &MemoryLedger{entries: make(map[EntryID]*entryLedger)}
}

func (l *MemoryLedger) locked(id EntryID) *entryLedger {
	e := l.entries[id]
	if e == nil {
		e = &entryLedger{}
		l.entries[id] = e
	}
	return e
}

// Watermark implements Ledger.
func (l *MemoryLedger) Watermark(id EntryID) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.entries[id]; e != nil {
		return e.Watermark
	}
	return time.Time{}
}

// SetWatermark implements Ledger.
func (l *MemoryLedger) SetWatermark(id EntryID, t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.locked(id)
	if t.After(e.Watermark) {
		e.Watermark = t
	}
}

// Append implements Ledger.
func (l *MemoryLedger) Append(rec RunRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.locked(rec.EntryID)
	i := sort.Search(len(e.Records), func(i int) bool {
		return !e.Records[i].Scheduled.Before(rec.Scheduled)
	})
	if i < len(e.Records) && e.Records[i].Scheduled.Equal(rec.Scheduled) {
		// An occurrence is settled exactly once.
		e.Records[i] = rec
	} else {
		e.Records = append(e.Records, RunRecord{})
		copy(e.Records[i+1:], e.Records[i:])
		e.Records[i] = rec
	}
	if rec.Scheduled.After(e.Watermark) {
		e.Watermark = rec.Scheduled
	}
}

// History implements Ledger.
func (l *MemoryLedger) History(id EntryID) []RunRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	src := l.entries[id]
	if src == nil {
		return nil
	}
	out := make([]RunRecord, len(src.Records))
	copy(out, src.Records)
	return out
}

// PendingMissed implements Ledger.
func (l *MemoryLedger) PendingMissed(id EntryID) []RunRecord {
	hist := l.History(id)
	out := hist[:0:0]
	for _, r := range hist {
		if r.Status == RunMissed {
			out = append(out, r)
		}
	}
	return out
}

// Snapshot implements Ledger.
func (l *MemoryLedger) Snapshot() map[EntryID][]RunRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[EntryID][]RunRecord, len(l.entries))
	for id, e := range l.entries {
		recs := make([]RunRecord, len(e.Records))
		copy(recs, e.Records)
		out[id] = recs
	}
	return out
}

// JSONLedger is a MemoryLedger whose state is persisted to a JSON file after
// every mutation, so a restarted process can continue with the same books.
type JSONLedger struct {
	*MemoryLedger
	path string
}

// NewJSONLedger loads the ledger at path (an absent file starts empty) and
// keeps it there.
func NewJSONLedger(path string) (*JSONLedger, error) {
	l := &JSONLedger{
		MemoryLedger: NewMemoryLedger(),
		path:         path,
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return l, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &l.entries); err != nil {
			return nil, err
		}
		if l.entries == nil {
			l.entries = make(map[EntryID]*entryLedger)
		}
	}
	return l, nil
}

// persistLocked writes the whole ledger to disk atomically (write to a temp
// file in the same directory, then rename).
func (l *JSONLedger) persistLocked() {
	data, err := json.MarshalIndent(l.entries, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(l.path)
	tmp, err := ioutil.TempFile(dir, ".ledger-")
	if err != nil {
		return
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return
	}
	_ = os.Rename(tmp.Name(), l.path)
}

// SetWatermark implements Ledger and persists the change.
func (l *JSONLedger) SetWatermark(id EntryID, t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.locked(id)
	if t.After(e.Watermark) {
		e.Watermark = t
		l.persistLocked()
	}
}

// Append implements Ledger and persists the change.
func (l *JSONLedger) Append(rec RunRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.locked(rec.EntryID)
	i := sort.Search(len(e.Records), func(i int) bool {
		return !e.Records[i].Scheduled.Before(rec.Scheduled)
	})
	if i < len(e.Records) && e.Records[i].Scheduled.Equal(rec.Scheduled) {
		e.Records[i] = rec
	} else {
		e.Records = append(e.Records, RunRecord{})
		copy(e.Records[i+1:], e.Records[i:])
		e.Records[i] = rec
	}
	if rec.Scheduled.After(e.Watermark) {
		e.Watermark = rec.Scheduled
	}
	l.persistLocked()
}
