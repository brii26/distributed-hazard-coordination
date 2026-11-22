package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// recentWindow is how far back the first look goes, so a large store is not read in full when the load just happened.
const recentWindow = 10 * time.Minute

// auditIDs lists the source_ref_id of the BMKG events the Audit API holds, ingested since the given time (all of them when since is zero). partial says a storage unit did not answer.
func auditIDs(ctx context.Context, c *http.Client, base, token string, since time.Time) (ids map[string]bool, partial bool, err error) {
	ids = map[string]bool{}
	cursor := ""

	for {
		q := url.Values{"source": {"BMKG"}, "limit": {"1000"}}
		if !since.IsZero() {
			q.Set("since", since.UTC().Format(time.RFC3339))
		}

		if cursor != "" {
			q.Set("cursor", cursor)
		}

		page, err := auditPage(ctx, c, strings.TrimSuffix(base, "/")+"/internal/audit/events?"+q.Encode(), token)
		if err != nil {
			return nil, false, err
		}

		for _, it := range page.Items {
			ids[it.SourceRefID] = true
		}

		partial = partial || page.Partial

		if page.NextCursor == nil || *page.NextCursor == "" {
			return ids, partial, nil
		}

		cursor = *page.NextCursor
	}
}

type auditEventsPage struct {
	Items []struct {
		SourceRefID string `json:"source_ref_id"`
	} `json:"items"`
	NextCursor *string `json:"next_cursor"`
	Partial    bool    `json:"partial"`
}

func auditPage(ctx context.Context, c *http.Client, u, token string) (auditEventsPage, error) {
	var page auditEventsPage

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return page, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.Do(req)
	if err != nil {
		return page, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))

		return page, fmt.Errorf("audit events: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}

	return page, json.NewDecoder(resp.Body).Decode(&page)
}

// missingFrom returns the sent ids the Audit API does not list.
func missingFrom(sent []event, have map[string]bool) []string {
	var out []string

	for _, e := range sent {
		if !have[e.EventID] {
			out = append(out, e.EventID)
		}
	}

	return out
}

// sendAndCheck sends everything and, with AUDIT_TOKEN set, checks that every sent id is listed by the Audit API. It looks at recent events first and reads the whole source only when something is missing, so events from the live feed cannot hide a loss and a rerun of an old file is not called a loss.
func sendAndCheck(ctx context.Context, s *sender, base string, catalog, burstEvs []event, out, errOut io.Writer) int {
	token := os.Getenv("AUDIT_TOKEN")
	started := time.Now()

	if code := sendPhases(ctx, s, catalog, burstEvs, out, errOut); code != 0 || token == "" {
		return code
	}

	sent := append(append([]event{}, catalog...), burstEvs...)

	have, partial, err := auditIDs(ctx, s.client, base, token, started.Add(-recentWindow))
	if err != nil {
		say(errOut, "%v", err)

		return 2
	}

	missing := missingFrom(sent, have)

	if len(missing) > 0 {
		if have, partial, err = auditIDs(ctx, s.client, base, token, time.Time{}); err != nil {
			say(errOut, "%v", err)

			return 2
		}

		missing = missingFrom(sent, have)
	}

	say(out, "audit_listed=%d sent=%d missing=%d partial=%t", len(have), len(sent), len(missing), partial)

	if len(missing) > 0 || partial {
		say(out, "FAIL: %d of %d sent ids are not in the Audit API, first %v (a partial answer is unreliable)", len(missing), len(sent), missing[:min(5, len(missing))])

		return 1
	}

	say(out, "PASS: every sent id is in the Audit API")

	return 0
}
