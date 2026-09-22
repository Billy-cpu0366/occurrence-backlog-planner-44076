package cron

import "time"

// OccurrenceEnumerator is implemented by schedules that can list all their
// activation instants within a half-open interval.
//
// The returned instants satisfy:
//
//	after < instant <= until
//
// and are sorted and unique (deduplicated by absolute instant).
//
// Enumerating occurrences explicitly is what lets the scheduler account for
// daylight-savings transitions: a nominal time that does not exist on a
// spring-forward night (e.g. 02:30 when 02:00 jumps to 03:00) resolves to the
// first instant that local wall-clock configuration still reaches on the same
// day, while a nominal time that occurs twice on a fall-back night is yielded
// only once.
//
// Schedules that do not implement this interface are supported through a
// fallback that repeatedly calls Next; such schedules cannot be given the
// special DST treatment.
type OccurrenceEnumerator interface {
	Occurrences(after, until time.Time) []time.Time
}

// maxOccurrences caps how many instants a single enumeration may return, so an
// always-matching schedule over a long interval cannot exhaust memory.
const maxOccurrences = 100000

// Occurrences returns the activation instants of sched in (after, until].
// If sched implements OccurrenceEnumerator it is used directly; otherwise the
// occurrences are found by repeatedly advancing Next.
func Occurrences(sched Schedule, after, until time.Time) []time.Time {
	if !until.After(after) {
		return nil
	}
	if e, ok := sched.(OccurrenceEnumerator); ok {
		out := e.Occurrences(after, until)
		return out
	}
	return enumerateByNext(sched, after, until)
}

func enumerateByNext(sched Schedule, after, until time.Time) []time.Time {
	var out []time.Time
	t := sched.Next(after)
	for !t.IsZero() && !t.After(until) && len(out) < maxOccurrences {
		out = append(out, t)
		t = sched.Next(t)
	}
	return out
}

// scheduleLocation returns the location in which nominal fields of s are
// interpreted.
func (s *SpecSchedule) scheduleLocation(t time.Time) *time.Location {
	// A schedule parsed without an explicit CRON_TZ is interpreted in the
	// location of the time handed to it, just like SpecSchedule.Next. Only an
	// explicitly fixed location overrides that.
	if s.Location == nil || s.Location == time.Local {
		return t.Location()
	}
	return s.Location
}

// Occurrences implements OccurrenceEnumerator.
//
// It walks the calendar one day at a time (anchored at noon so DST shifts can
// never change which day is being inspected), enumerates the matching
// hour:minute:second combinations, and builds each nominal instant with
// time.Date. time.Date normalizes nonexistent spring-forward times forward to
// the first reachable instant of the same local day, and the fall-back double
// occurrence is removed by deduplicating on the absolute instant.
func (s *SpecSchedule) Occurrences(after, until time.Time) []time.Time {
	if !until.After(after) {
		return nil
	}
	loc := s.scheduleLocation(after)

	// Pre-compute matching second/minute/hour combinations.
	type hms struct{ h, m, sec int }
	var combos []hms
	for h := 0; h <= 23; h++ {
		if s.Hour&(1<<uint(h)) == 0 {
			continue
		}
		for m := 0; m <= 59; m++ {
			if s.Minute&(1<<uint(m)) == 0 {
				continue
			}
			for sec := 0; sec <= 59; sec++ {
				if s.Second&(1<<uint(sec)) == 0 {
					continue
				}
				combos = append(combos, hms{h, m, sec})
			}
		}
	}
	if len(combos) == 0 {
		return nil
	}

	// Iterate local days from after's date through until's date. The day is
	// anchored at noon so DST shifts (at most a few hours) can never move the
	// anchor into a neighboring day.
	aIn := after.In(loc)
	day := time.Date(aIn.Year(), aIn.Month(), aIn.Day(), 12, 0, 0, 0, loc)
	uIn := until.In(loc)
	end := time.Date(uIn.Year(), uIn.Month(), uIn.Day(), 12, 0, 0, 0, loc)

	var out []time.Time
	seen := make(map[int64]struct{})
	for !day.After(end) {
		if s.Month&(1<<uint(day.Month())) != 0 && dayMatches(s, day) {
			for _, c := range combos {
				nominal := normalizeOccurrence(day, c.h, c.m, c.sec, loc)
				key := nominal.UnixNano()
				if _, dup := seen[key]; dup {
					// A fall-back night presents the same absolute instant
					// for two nominal wall-clock readings; count it once.
					continue
				}
				seen[key] = struct{}{}
				if nominal.After(after) && !nominal.After(until) {
					out = append(out, nominal)
				}
			}
		}
		if len(out) >= maxOccurrences {
			break
		}
		day = day.AddDate(0, 0, 1)
	}
	return out
}

// Occurrences implements OccurrenceEnumerator for fixed-delay schedules. The
// delay is anchored at the instant immediately after `after` (rounded up to
// the second), matching ConstantDelaySchedule.Next.
func (s ConstantDelaySchedule) Occurrences(after, until time.Time) []time.Time {
	if !until.After(after) {
		return nil
	}
	delay := s.Delay
	if delay < time.Second {
		delay = time.Second
	}
	t := after.Add(delay - time.Duration(after.Nanosecond())*time.Nanosecond)
	var out []time.Time
	for !t.After(until) && len(out) < maxOccurrences {
		out = append(out, t)
		t = t.Add(delay)
	}
	return out
}

// normalizeOccurrence builds the absolute instant for a nominal wall-clock
// time on the given local day.
//
// On a spring-forward night the nominal time may not exist (e.g. 02:30 when
// the clock jumps from 02:00 to 03:00). Instead of silently adding the gap
// onto the scheduled time (which would delay this specific occurrence until
// after the skipped slot was already due to be over), it is moved to the
// first reachable instant of the same local day: the beginning of the gap.
// That makes the skipped slot get made up as soon as the day becomes
// executable, never silently and never on the following day.
func normalizeOccurrence(day time.Time, h, m, sec int, loc *time.Location) time.Time {
	t := time.Date(day.Year(), day.Month(), day.Day(), h, m, sec, 0, loc)
	if t.Hour() == h && t.Minute() == m && t.Second() == sec {
		return t
	}
	// Walk back by whole nominal minutes to the final wall-clock minute that
	// still exists, then step one minute forward: that instant is the gap
	// start (e.g. 03:00 EDT when 02:00-02:59 did not exist).
	hh, mm := h, m
	for {
		if mm == 0 {
			if hh == 0 {
				break
			}
			hh--
			mm = 59
		} else {
			mm--
		}
		cand := time.Date(day.Year(), day.Month(), day.Day(), hh, mm, 0, 0, loc)
		if cand.Hour() == hh && cand.Minute() == mm {
			return cand.Add(time.Minute)
		}
	}
	return t
}
