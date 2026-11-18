package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"
)

// sessionLine is one /v1/session poll as printed by "feedprobe session".
type sessionLine struct {
	Wall    string          `json:"wall_rfc3339nano"`
	MonoNs  int64           `json:"mono_ns"`
	Status  int             `json:"status"`
	Session json.RawMessage `json:"session,omitempty"`
	Body    string          `json:"body,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func runSession(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("session", flag.ContinueOnError)
	baseURL := fs.String("base-url", defaultBaseURL, "feed base URL")
	poll := fs.Duration("poll", 5*time.Second, "interval between polls")
	total := fs.Duration("for", 0, "how long to keep polling; 0 polls once")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *poll <= 0 {
		return fmt.Errorf("--poll must be positive, got %s", *poll)
	}

	client, err := newFeedClient(*baseURL, false)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	start := time.Now()
	enc := json.NewEncoder(stdout)

	for {
		status, body, err := client.session(ctx)
		now := time.Now()
		line := sessionLine{Wall: now.UTC().Format(time.RFC3339Nano), MonoNs: now.Sub(start).Nanoseconds(), Status: status}
		setBody(&line.Session, &line.Body, body)

		if err != nil {
			line.Error = err.Error()
		}

		if err := enc.Encode(line); err != nil {
			return fmt.Errorf("write session line: %w", err)
		}

		if now.Add(*poll).Sub(start) > *total || !sleepCtx(ctx, *poll) {
			return nil
		}
	}
}

// setBody stores a body as raw JSON when it is valid JSON, otherwise as text.
func setBody(raw *json.RawMessage, text *string, body []byte) {
	if len(body) == 0 {
		return
	}

	if json.Valid(body) {
		*raw = append(json.RawMessage(nil), body...)

		return
	}

	*text = string(body)
}
