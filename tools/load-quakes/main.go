// Command load-quakes sends an earthquake catalog, and optionally a burst in one region, to the Aggregator's admin ingest route.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// say writes one formatted line and ignores the write error: a closed terminal is not worth stopping for.
func say(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format+"\n", args...)
}

// options are the command line flags.
type options struct {
	file, mapSpec, base, boxSpec string
	batch, burstN                int
	pause                        time.Duration
	dry                          bool
}

var errUsage = errors.New("usage")

func parseOptions(args []string, errOut io.Writer) (options, error) {
	var o options

	fs := flag.NewFlagSet("load-quakes", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.StringVar(&o.file, "file", "", "catalog file, CSV with a header row or a JSON array")
	fs.StringVar(&o.mapSpec, "map", "", "column names, for example time=Origin,lat=Latitude,lon=Longitude,mag=Mag (fields: id, time, mag, depth, lat, lon, region; time may be Date+Time)")
	fs.StringVar(&o.base, "url", "", "Aggregator base URL, for example http://localhost:8080")
	fs.IntVar(&o.batch, "batch", 500, "events per request, at most 1000")
	fs.DurationVar(&o.pause, "pause", 0, "wait between requests")
	fs.IntVar(&o.burstN, "burst", 0, "also send this many events inside one box, for the regional burst")
	fs.StringVar(&o.boxSpec, "box", "-10,2,94,112", "burst box as latMin,latMax,lonMin,lonMax (default: the Sunda arc)")
	fs.BoolVar(&o.dry, "dry-run", false, "read and check the catalog, send nothing")

	if err := fs.Parse(args); err != nil {
		return o, errUsage
	}

	switch {
	case o.file == "" && o.burstN == 0:
		return o, errors.New("give --file, --burst, or both")
	case o.batch < 1 || o.batch > 1000:
		return o, errors.New("--batch must be 1 to 1000")
	}

	return o, nil
}

// loadCatalog reads the catalog if there is one and prints what it found.
func loadCatalog(o options, now time.Time, out io.Writer) ([]event, error) {
	if o.file == "" {
		return nil, nil
	}

	cols, err := parseColumns(o.mapSpec)
	if err != nil {
		return nil, err
	}

	catalog, rep, err := load(o.file, cols, now)
	if err != nil {
		return nil, err
	}

	say(out, "rows_read=%d unique_rows=%d duplicates=%d rejected=%d (times without a zone are read as UTC)", rep.Read, rep.Kept, rep.Duplicates, total(rep.Rejected))

	for why, n := range rep.Rejected {
		say(out, "rejected %d: %s", n, why)
	}

	return catalog, nil
}

func run(args []string, out, errOut io.Writer) int {
	o, err := parseOptions(args, errOut)
	if err != nil {
		if !errors.Is(err, errUsage) {
			say(errOut, "%v", err)
		}

		return 2
	}

	box, err := parseBox(o.boxSpec)
	if err != nil {
		say(errOut, "%v", err)

		return 2
	}

	now := time.Now().UTC()

	catalog, err := loadCatalog(o, now, out)
	if err != nil {
		say(errOut, "%v", err)

		return 2
	}

	//nolint:gosec // load shaping for a test burst, not a security use
	burstEvs := burst(o.burstN, box, now, rand.New(rand.NewPCG(uint64(now.UnixNano()), 1)))

	if o.dry {
		say(out, "dry run: would send %d catalog events and %d burst events", len(catalog), len(burstEvs))

		return 0
	}

	token := os.Getenv("ADMIN_TOKEN")
	if o.base == "" || token == "" {
		say(errOut, "sending needs --url and the ADMIN_TOKEN environment variable")

		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	s := &sender{url: strings.TrimSuffix(o.base, "/") + "/internal/admin/ingest", token: token, client: &http.Client{Timeout: 3 * time.Minute}, batch: o.batch, pause: o.pause, out: out}

	return sendAndCheck(ctx, s, o.base, catalog, burstEvs, out, errOut)
}

// sendPhases sends the catalog and then the burst, printing the clock time of each so a report can line up node load with them.
func sendPhases(ctx context.Context, s *sender, catalog, burstEvs []event, out, errOut io.Writer) int {
	for _, phase := range []struct {
		name string
		evs  []event
	}{{"catalog", catalog}, {"burst", burstEvs}} {
		if len(phase.evs) == 0 {
			continue
		}

		say(out, "phase=%s start=%s events=%d", phase.name, time.Now().UTC().Format(time.RFC3339), len(phase.evs))

		if err := s.send(ctx, phase.evs); err != nil {
			say(errOut, "phase %s stopped: %v", phase.name, err)

			return 1
		}

		say(out, "phase=%s end=%s", phase.name, time.Now().UTC().Format(time.RFC3339))
	}

	return 0
}

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}

	return n
}

func parseBox(spec string) ([4]float64, error) {
	var box [4]float64

	parts := strings.Split(spec, ",")
	if len(parts) != 4 {
		return box, fmt.Errorf("--box wants latMin,latMax,lonMin,lonMax, got %q", spec)
	}

	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return box, fmt.Errorf("--box: %q is not a number", p)
		}

		box[i] = f
	}

	if box[0] >= box[1] || box[2] >= box[3] {
		return box, fmt.Errorf("--box wants min before max, got %q", spec)
	}

	return box, nil
}

// sender posts events in batches and stops at the first batch the Aggregator refuses.
type sender struct {
	url, token string
	client     *http.Client
	batch      int
	pause      time.Duration
	out        io.Writer
}

func (s *sender) send(ctx context.Context, evs []event) error {
	for start := 0; start < len(evs); start += s.batch {
		end := min(start+s.batch, len(evs))

		if err := s.post(ctx, evs[start:end], start); err != nil {
			return err
		}

		say(s.out, "sent %d of %d", end, len(evs))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pause):
		}
	}

	return nil
}

func (s *sender) post(ctx context.Context, evs []event, start int) error {
	body, err := json.Marshal(evs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

		return fmt.Errorf("batch at row %d: %s: %s", start, resp.Status, strings.TrimSpace(string(msg)))
	}

	return nil
}
