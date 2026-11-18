package main

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func readAll(t *testing.T, stream string) []sseEvent {
	t.Helper()

	r := newSSEReader(strings.NewReader(stream))

	var got []sseEvent

	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			return got
		}

		if err != nil {
			t.Fatalf("Next: %v", err)
		}

		got = append(got, ev)
	}
}

func TestSSEReader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		stream string
		want   []sseEvent
	}{
		{
			name:   "event with id and data",
			stream: "id: 7\nevent: detection\ndata: {\"a\":1}\n\n",
			want:   []sseEvent{{ID: new("7"), Event: "detection", Data: `{"a":1}`, HasData: true}},
		},
		{
			name:   "multi-line data joins with LF",
			stream: "data: line one\ndata: line two\ndata:\n\n",
			want:   []sseEvent{{Event: defaultEvent, Data: "line one\nline two\n", HasData: true}},
		},
		{
			name:   "CRLF and lone CR line endings",
			stream: "id: 1\r\ndata: x\r\n\r\nid: 2\rdata: y\r\r",
			want: []sseEvent{
				{ID: new("1"), Event: defaultEvent, Data: "x", HasData: true},
				{ID: new("2"), Event: defaultEvent, Data: "y", HasData: true},
			},
		},
		{
			name:   "comment lines are returned on their own",
			stream: ": keep-alive\n\n:no-space\ndata: z\n\n",
			want: []sseEvent{
				{Event: commentEvent, Data: " keep-alive", HasData: true, Comment: true},
				{Event: commentEvent, Data: "no-space", HasData: true, Comment: true},
				{Event: defaultEvent, Data: "z", HasData: true},
			},
		},
		{
			name:   "missing event defaults to message and only one leading space is stripped",
			stream: "data:  two spaces\n\n",
			want:   []sseEvent{{Event: defaultEvent, Data: " two spaces", HasData: true}},
		},
		{
			name:   "retry digits only, block without data still recorded",
			stream: "retry: 3000\n\nretry: 3s\nevent: heartbeat\n\n",
			want: []sseEvent{
				{Event: defaultEvent, Retry: new(int64(3000))},
				{Event: "heartbeat"},
			},
		},
		{
			name:   "unknown fields are kept by name, blank lines alone dispatch nothing",
			stream: "\n\nfoo: bar\ndata: q\n\n",
			want:   []sseEvent{{Event: defaultEvent, Data: "q", HasData: true, Unknown: []string{"foo"}}},
		},
		{
			name:   "block cut off by EOF is discarded",
			stream: "data: complete\n\ndata: partial\n",
			want:   []sseEvent{{Event: defaultEvent, Data: "complete", HasData: true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := readAll(t, tc.stream)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got  %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestSSEReaderLineTooLong(t *testing.T) {
	t.Parallel()

	r := newSSEReader(strings.NewReader("data: " + strings.Repeat("x", maxLineBytes) + "\n\n"))
	if _, err := r.Next(); !errors.Is(err, errLineTooLong) {
		t.Fatalf("got %v, want errLineTooLong", err)
	}
}
