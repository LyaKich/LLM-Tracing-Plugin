package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func costOf(s *traceStore, traces []*uiTrace) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0.0
	for _, t := range traces {
		total += s.costLocked(t).cost
	}
	return roundUSD(total)
}

// The friend's bug: totals fell when old requests left the request history.
// All-time totals must not change when the history drops requests.
func TestAllTimeTotalsSurviveHistoryLimits(t *testing.T) {
	clock := newFakeClock()
	s := newTraceStore()
	s.now = clock.Now
	s.configure(true, 3, 5, 0) // keep only 5 requests
	var all []*uiTrace
	for i := 0; i < 12; i++ {
		tr := opusTurn(i+1, time.Duration(i)*time.Hour, 200_000, 150_000, 10_000)
		all = append(all, tr)
		s.add(tr)
	}
	if s.len() != 5 {
		t.Fatalf("history len %d", s.len())
	}
	want := costOf(s, all)
	got := s.summaryFor(clock.Now().Add(24*time.Hour), 0)
	if got.Source != "hourly" || got.Requests != 12 || got.CostUSD != want || !got.P95Approx {
		t.Fatalf("all time: source=%s requests=%d cost=%v want %v", got.Source, got.Requests, got.CostUSD, want)
	}
	// A range inside the remaining history stays exact.
	recent := s.summaryFor(baseTime.Add(11*time.Hour+30*time.Minute), time.Hour)
	if recent.Source != "requests" || recent.Requests != 1 || recent.P95Approx {
		t.Fatalf("recent: %+v", recent)
	}
}

func TestLateAttemptUpdatesHourlyTotals(t *testing.T) {
	clock := newFakeClock()
	tr := newTestTracer(t, uiConfig(), clock)
	start := clock.Now()
	tr.addCompletion(completionAt("late", start, start.Add(time.Second), outcomeSucceeded, 200))
	tr.addUsage(usageAt("late", "late-a", start.Add(time.Millisecond), time.Millisecond))
	finalize(tr, clock)
	tr.addUsage(usageAt("late", "late-b", start.Add(2*time.Millisecond), time.Millisecond))
	s := tr.store
	s.mu.Lock()
	sum, _, _ := s.roll.sumSince(0)
	want := s.costLocked(s.buf[0]).cost
	s.mu.Unlock()
	if sum.Requests != 1 || sum.Input != 20 || !near(sum.Cost, want) || sum.Cost <= 0 {
		t.Fatalf("hourly after late attempt: %+v want cost %v", sum, want)
	}
}

func TestHourlyTotalsPersistAndSeed(t *testing.T) {
	clock := newFakeClock()
	s := insightStore(opusTurn(1, 0, 100_000, 90_000, 0), opusTurn(2, 2*time.Hour, 100_000, 90_000, 0))
	data, changed := s.rollupSnapshot(clock.Now())
	if !changed {
		t.Fatal("not dirty after adds")
	}
	if _, again := s.rollupSnapshot(clock.Now()); again {
		t.Fatal("snapshot must clear the dirty flag")
	}
	hours, skipped, err := decodeRollups(data)
	if err != nil || skipped != 0 || len(hours) != 2 {
		t.Fatalf("decode: %v %d %d", err, skipped, len(hours))
	}
	// Restart with a saved file: merged, and imported requests are not
	// counted a second time.
	fresh := newTraceStore()
	fresh.configure(true, 500, 20000, 0)
	fresh.mergeRollups(hours)
	loaded := []*uiTrace{opusTurn(1, 0, 100_000, 90_000, 0), opusTurn(2, 2*time.Hour, 100_000, 90_000, 0)}
	fresh.importHistory(loaded, false)
	a := s.summaryFor(clock.Now().Add(3*time.Hour), 0)
	b := fresh.summaryFor(clock.Now().Add(3*time.Hour), 0)
	if a.CostUSD != b.CostUSD || b.Requests != 2 {
		t.Fatalf("persisted %+v vs %+v", a, b)
	}
	// First start without a file: totals are rebuilt from the history.
	seeded := newTraceStore()
	seeded.configure(true, 500, 20000, 0)
	seeded.importHistory([]*uiTrace{opusTurn(1, 0, 100_000, 90_000, 0)}, true)
	seeded.mu.Lock()
	sum, _, _ := seeded.roll.sumSince(0)
	seeded.mu.Unlock()
	if sum.Requests != 1 {
		t.Fatalf("seeded: %+v", sum)
	}
	// Damaged buckets are skipped, not trusted.
	if _, skipped, _ := decodeRollups([]byte(`{"v":1,"hours":{"1":{"n":-3,"h":[]},"x":{"n":1}}}`)); skipped != 2 {
		t.Fatalf("skipped %d", skipped)
	}
}

// Restart after more than history_days: the request history is gone, but
// the all-time cost is not.
func TestPluginKeepsAllTimeCostAcrossRestartAndAge(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock()
	cfg := uiConfig()
	cfg.DataDir = dir
	first := newPluginState(clock.Now, nil, false)
	first.tracer.reconfigure(cfg)
	first.mu.Lock()
	first.syncHistoryLocked(cfg)
	first.mu.Unlock()
	waitLoaded(t, first)
	historyTracerInto(t, first.tracer, clock, 4)
	before := first.tracer.summaryFor(0, "all")
	first.shutdown()
	if _, err := os.Stat(filepath.Join(dir, rollupFile)); err != nil {
		t.Fatalf("rollups.json not saved: %v", err)
	}

	clock.Advance(10 * 24 * time.Hour) // older than history_days (7)
	second := newPluginState(clock.Now, nil, false)
	second.tracer.reconfigure(cfg)
	second.mu.Lock()
	second.syncHistoryLocked(cfg)
	second.mu.Unlock()
	waitLoaded(t, second)
	after := second.tracer.summaryFor(0, "all")
	if second.tracer.store.len() != 0 || after.Requests != 4 || after.CostUSD != before.CostUSD || after.CostUSD <= 0 {
		t.Fatalf("after restart: stored=%d requests=%d cost=%v want %v", second.tracer.store.len(), after.Requests, after.CostUSD, before.CostUSD)
	}
	week := second.tracer.summaryFor(7*24*time.Hour, "7d")
	if week.Requests != 0 {
		t.Fatalf("last 7 days must be empty: %d", week.Requests)
	}
	second.shutdown()
}

func TestCompactHistoryFile(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock()
	h := newHistoryFile(dir, 7, clock.Now)
	tr := newTestTracer(t, uiConfig(), clock)
	tr.store.setPersist(h.enqueue)
	historyTracerInto(t, tr, clock, 5)
	tr.addUsage(usageAt("h01", "h01-late", clock.Now(), time.Millisecond)) // a second line for one request
	h.close()
	before := tr.summaryFor(0, "all")
	files, _ := historyFiles(dir)
	path := filepath.Join(dir, files[0])
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), `"sp2":`) {
		t.Fatal("fixture has no spans")
	}
	if err := compactHistoryFile(path); err != nil {
		t.Fatal(err)
	}
	small, _ := os.ReadFile(path)
	if strings.Contains(string(small), `"sp2":`) || strings.Count(string(small), "\n") != 5 || len(small)*2 > len(raw) {
		t.Fatalf("compacted %d -> %d bytes, %d lines", len(raw), len(small), strings.Count(string(small), "\n"))
	}
	loaded, err := newHistoryFile(dir, 7, clock.Now).load()
	if err != nil || len(loaded) != 5 {
		t.Fatalf("load after compaction: %d %v", len(loaded), err)
	}
	fresh := newTestTracer(t, uiConfig(), clock)
	fresh.store.importHistory(loaded, true)
	if after := fresh.summaryFor(0, "all"); after.CostUSD != before.CostUSD || after.Requests != 5 {
		t.Fatalf("totals changed: %v vs %v", after.CostUSD, before.CostUSD)
	}
}

func TestDataDirWarning(t *testing.T) {
	no := func(string) bool { return false }
	env := func(string) string { return "" }
	tmp := filepath.Join(t.TempDir(), "sys-tmp")
	if w := dataDirWarning(filepath.Join(tmp, "llm-tracing"), no, env, tmp); !strings.Contains(w, "temporary") {
		t.Fatalf("temp: %q", w)
	}
	docker := func(p string) bool { return p == "/.dockerenv" }
	if w := dataDirWarning("/app/plugins/llm-tracing", docker, env, tmp); !strings.Contains(w, "container") {
		t.Fatalf("docker: %q", w)
	}
	if w := dataDirWarning("/srv/cpa/plugins/llm-tracing", no, env, tmp); w != "" {
		t.Fatalf("plain: %q", w)
	}
}

func TestApproxP95(t *testing.T) {
	var h [latencyBuckets]int64
	h[latencyBucket(500)] = 90  // 256..512 ms
	h[latencyBucket(3000)] = 10 // 2048..4096 ms
	if got := approxP95(h, 100); got != 4096 {
		t.Fatalf("p95 %v", got)
	}
	if approxP95(h, 0) != 0 || latencyBucket(0.2) != 0 || latencyBucket(1e12) != latencyBuckets-1 {
		t.Fatal("edges")
	}
}
