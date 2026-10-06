package main

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"testing"
)

//nolint:paralleltest // swaps the global logger
func TestPlainLogLinesAreJSONWithTheRequiredFields(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	log.Printf("dashboard-updater listening on :%d", 1234)

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not one JSON object: %q", buf.String())
	}

	for _, f := range []string{"timestamp", "level", "service", "correlation_id", "message"} {
		if _, ok := line[f]; !ok {
			t.Fatalf("log line %v has no %q", line, f)
		}
	}

	if line["service"] != serviceName || line["correlation_id"] != "-" || line["message"] != "dashboard-updater listening on :1234" || line["level"] != "info" {
		t.Fatalf("unexpected line %v", line)
	}
}

//nolint:paralleltest // swaps the global logger
func TestWithIDCarriesTheMessagesCorrelationID(t *testing.T) {
	var buf bytes.Buffer

	setupLogging(&buf)
	t.Cleanup(func() { setupLogging(os.Stdout) })

	withID("cycle-9").Info("message received")
	withID("").Info("no id")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))

	for i, want := range []string{"cycle-9", "-"} {
		var line map[string]any
		if err := json.Unmarshal(lines[i], &line); err != nil || line["correlation_id"] != want {
			t.Fatalf("line %d: expected correlation_id %q, got %s", i, want, lines[i])
		}
	}
}
