package main

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// percentile uses the nearest-rank method on a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	rank := max(int(math.Ceil(p/100*float64(len(sorted))))-1, 0)

	return sorted[min(rank, len(sorted)-1)]
}

var distHeader = []string{"n", "min", "p50", "p90", "p99", "p99.9", "max"}

// distRow summarises durations as n, min, p50, p90, p99, p99.9, max.
func distRow(ds []time.Duration) []string {
	if len(ds) == 0 {
		return []string{"0", "-", "-", "-", "-", "-", "-"}
	}

	s := slices.Clone(ds)
	slices.Sort(s)

	return []string{
		strconv.Itoa(len(s)), fmtDur(s[0]), fmtDur(percentile(s, 50)), fmtDur(percentile(s, 90)),
		fmtDur(percentile(s, 99)), fmtDur(percentile(s, 99.9)), fmtDur(s[len(s)-1]),
	}
}

func fmtDur(d time.Duration) string {
	abs := d.Abs()

	switch {
	case abs >= time.Minute:
		return d.Round(time.Second).String()
	case abs >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}

func intPercentile(values []int, p float64) int {
	if len(values) == 0 {
		return 0
	}

	s := slices.Clone(values)
	slices.Sort(s)
	rank := max(int(math.Ceil(p/100*float64(len(s))))-1, 0)

	return s[min(rank, len(s)-1)]
}

// md writes Markdown and keeps the first write error, so report code stays linear.
type md struct {
	w   io.Writer
	err error
}

func (m *md) printf(format string, args ...any) {
	if m.err == nil {
		_, m.err = fmt.Fprintf(m.w, format, args...)
	}
}

func (m *md) heading(title string) { m.printf("\n## %s\n\n", title) }

func (m *md) table(header []string, rows [][]string) {
	if len(rows) == 0 {
		m.printf("_none_\n\n")

		return
	}

	m.printf("| %s |\n|%s\n", strings.Join(header, " | "), strings.Repeat("---|", len(header)))

	for _, row := range rows {
		cells := make([]string, len(row))
		for i, c := range row {
			cells[i] = strings.ReplaceAll(strings.ReplaceAll(c, "|", `\|`), "\n", " ")
		}

		m.printf("| %s |\n", strings.Join(cells, " | "))
	}

	m.printf("\n")
}

// counts writes a two-column table of a count map, most frequent first.
func (m *md) counts(label string, counts map[string]int) {
	m.table([]string{label, "count"}, countRows(counts))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "…"
}

// countRows turns a count map into rows sorted by count, then by key.
func countRows(counts map[string]int) [][]string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b string) int {
		if counts[a] != counts[b] {
			return counts[b] - counts[a]
		}

		return strings.Compare(a, b)
	})

	rows := make([][]string, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, []string{k, strconv.Itoa(counts[k])})
	}

	return rows
}

func secs(ns int64) string {
	return strconv.FormatFloat(float64(ns)/1e9, 'f', 1, 64) + " s"
}
