package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Hourly totals keep the cost and token numbers exact for any time range,
// however old: the detailed history (request rows) is bounded by
// history_days and history_max_requests, but a request's contribution to its
// hour is kept for rollup_days. One bucket per hour with traffic, about 250
// bytes in memory, so a year is about 2 MB. They are saved to
// data_dir/rollups.json.

const (
	rollupFile       = "rollups.json"
	rollupFileV      = 1
	latencyBuckets   = 24 // log2 milliseconds: [2^i, 2^(i+1)) ms
	rollupSaveEvery  = time.Minute
	defaultRollupAge = 400 * 24 * time.Hour
)

type hourBucket struct {
	Requests    int64 `json:"n"`
	Errors      int64 `json:"err,omitempty"`
	Canceled    int64 `json:"can,omitempty"`
	Retried     int64 `json:"ret,omitempty"`
	UnknownCost int64 `json:"unk,omitempty"`
	Unpriced    int64 `json:"unp,omitempty"`
	Input       int64 `json:"in,omitempty"`
	TotalInput  int64 `json:"tin,omitempty"`
	CacheRead   int64 `json:"cr,omitempty"`
	CacheWrite  int64 `json:"cw,omitempty"`
	Output      int64 `json:"out,omitempty"`
	// USD at the prices fixed when each request was recorded.
	Cost    float64 `json:"c,omitempty"`
	CostMin float64 `json:"cm,omitempty"`
	NoCache float64 `json:"nc,omitempty"`
	DurMS   float64 `json:"d,omitempty"`
	// Latency histogram for an approximate p95.
	Hist [latencyBuckets]int64 `json:"h"`
	// First and last request start in this hour (unix nanos).
	First int64 `json:"f,omitempty"`
	Last  int64 `json:"l,omitempty"`
}

// contribution is one request's share of its hour.
type contribution struct {
	hour int64
	b    hourBucket
	at   int64
}

func latencyBucket(ms float64) int {
	if ms < 1 {
		return 0
	}
	return min(int(math.Log2(ms)), latencyBuckets-1)
}

// contributionLocked derives a trace's hourly contribution. It is computed
// from the trace itself, so an update (a late usage record) is applied as
// "remove the old contribution, add the new one" without storing anything.
func (s *traceStore) contributionLocked(t *uiTrace) contribution {
	c := s.costLocked(t)
	b := hourBucket{Requests: 1, Input: t.input, TotalInput: t.totalInput, CacheRead: t.cacheRead,
		CacheWrite: t.cacheWrite, Output: t.output, Cost: c.cost, CostMin: c.costMin, NoCache: c.noCache}
	if t.isError() {
		b.Errors = 1
	}
	if t.isCanceled() {
		b.Canceled = 1
	}
	if t.attempts > 1 {
		b.Retried = 1
	}
	if c.unknown {
		b.UnknownCost = 1
	}
	if !c.priced {
		b.Unpriced = 1
	}
	d := t.durationMS()
	b.DurMS = d
	b.Hist[latencyBucket(d)] = 1
	return contribution{hour: t.start / int64(time.Hour), b: b, at: t.start}
}

// add adds (sign 1) or subtracts (sign -1) the counters of o; First and
// Last are left to the caller.
func (h *hourBucket) add(o *hourBucket, sign int64) {
	f := float64(sign)
	h.Requests += sign * o.Requests
	h.Errors += sign * o.Errors
	h.Canceled += sign * o.Canceled
	h.Retried += sign * o.Retried
	h.UnknownCost += sign * o.UnknownCost
	h.Unpriced += sign * o.Unpriced
	h.Input += sign * o.Input
	h.TotalInput += sign * o.TotalInput
	h.CacheRead += sign * o.CacheRead
	h.CacheWrite += sign * o.CacheWrite
	h.Output += sign * o.Output
	h.Cost += f * o.Cost
	h.CostMin += f * o.CostMin
	h.NoCache += f * o.NoCache
	h.DurMS += f * o.DurMS
	for i := range h.Hist {
		h.Hist[i] += sign * o.Hist[i]
	}
}

// rollups is the in-memory set of hourly buckets.
type rollups struct {
	hours map[int64]*hourBucket
	dirty bool
}

func newRollups() *rollups { return &rollups{hours: map[int64]*hourBucket{}} }

// apply adds (sign 1) or removes (sign -1) a contribution.
func (r *rollups) apply(c contribution, sign int64) {
	h := r.hours[c.hour]
	if h == nil {
		if sign < 0 {
			return
		}
		h = &hourBucket{}
		r.hours[c.hour] = h
	}
	h.add(&c.b, sign)
	if sign > 0 {
		if h.First == 0 || c.at < h.First {
			h.First = c.at
		}
		h.Last = max(h.Last, c.at)
	}
	if h.Requests <= 0 {
		delete(r.hours, c.hour)
	}
	r.dirty = true
}

// merge adds buckets loaded from disk to the live ones (requests recorded
// since start are not in the file, so nothing is counted twice).
func (r *rollups) merge(loaded map[int64]*hourBucket) {
	for hour, b := range loaded {
		r.apply(contribution{hour: hour, b: *b, at: b.First}, 1)
		if h := r.hours[hour]; h != nil {
			h.First = minNonZero(h.First, b.First)
			h.Last = max(h.Last, b.Last)
		}
	}
}

func minNonZero(a, b int64) int64 {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	}
	return min(a, b)
}

// prune drops buckets older than maxAge.
func (r *rollups) prune(now time.Time, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	oldest := now.Add(-maxAge).UnixNano() / int64(time.Hour)
	for hour := range r.hours {
		if hour < oldest {
			delete(r.hours, hour)
			r.dirty = true
		}
	}
}

// sumSince totals the buckets whose hour ends after since (since <= 0: all).
func (r *rollups) sumSince(since int64) (hourBucket, int64, int64) {
	var sum hourBucket
	var first, last int64
	from := int64(math.MinInt64)
	if since > 0 {
		from = since / int64(time.Hour)
	}
	for hour, b := range r.hours {
		if hour < from {
			continue
		}
		sum.add(b, 1)
		first = minNonZero(first, b.First)
		last = max(last, b.Last)
	}
	return sum, first, last
}

// approxP95 returns the upper edge of the histogram bucket holding the 95th
// percentile, in milliseconds.
func approxP95(h [latencyBuckets]int64, n int64) float64 {
	if n <= 0 {
		return 0
	}
	rank := int64(math.Ceil(0.95 * float64(n)))
	var seen int64
	for i, c := range h {
		seen += c
		if seen >= rank {
			return math.Pow(2, float64(i+1))
		}
	}
	return math.Pow(2, latencyBuckets)
}

// --- persistence ------------------------------------------------------------

type rollupFileJSON struct {
	V     int                    `json:"v"`
	Hours map[string]*hourBucket `json:"hours"`
}

func (r *rollups) encode() ([]byte, error) {
	out := rollupFileJSON{V: rollupFileV, Hours: make(map[string]*hourBucket, len(r.hours))}
	keys := make([]int64, 0, len(r.hours))
	for hour := range r.hours {
		keys = append(keys, hour)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, hour := range keys {
		b := *r.hours[hour]
		out.Hours[fmt.Sprint(hour)] = &b
	}
	return json.Marshal(out)
}

// decodeRollups validates a rollup file; damaged buckets are skipped.
func decodeRollups(raw []byte) (map[int64]*hourBucket, int, error) {
	var in rollupFileJSON
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, 0, err
	}
	if in.V != rollupFileV {
		return nil, 0, fmt.Errorf("unsupported rollup file version %d", in.V)
	}
	out := make(map[int64]*hourBucket, len(in.Hours))
	skipped := 0
	for key, b := range in.Hours {
		var hour int64
		if _, err := fmt.Sscan(key, &hour); err != nil || b == nil || !validBucket(b) {
			skipped++
			continue
		}
		out[hour] = b
	}
	return out, skipped, nil
}

func validBucket(b *hourBucket) bool {
	for _, n := range []int64{b.Requests, b.Errors, b.Canceled, b.Retried, b.UnknownCost, b.Unpriced,
		b.Input, b.TotalInput, b.CacheRead, b.CacheWrite, b.Output} {
		if n < 0 {
			return false
		}
	}
	for _, f := range []float64{b.Cost, b.CostMin, b.NoCache, b.DurMS} {
		if f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return false
		}
	}
	for _, n := range b.Hist {
		if n < 0 {
			return false
		}
	}
	return b.Requests > 0
}

// writeFileAtomic writes via a temporary file and a rename, so a crash never
// leaves a half-written file.
func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, errWrite := tmp.Write(data)
	errClose := tmp.Close()
	if err = errors.Join(errWrite, errClose); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err = os.Rename(name, path); err != nil {
		_ = os.Remove(name)
	}
	return err
}
