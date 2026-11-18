package main

import (
	"bufio"
	"cmp"
	"errors"
	"io"
	"strconv"
	"strings"
)

// maxLineBytes bounds one SSE line so a broken stream cannot grow memory without limit.
const maxLineBytes = 1 << 20

const (
	// commentEvent is the event name recorded for an SSE comment line (": ...").
	commentEvent = "_comment"
	// defaultEvent is the SSE event type when a block has no event field.
	defaultEvent = "message"
)

var errLineTooLong = errors.New("sse line longer than 1 MiB")

// sseEvent is one dispatched SSE block, or one comment line.
type sseEvent struct {
	ID      *string  // id field of this block; nil when the block carries none
	Event   string   // event field, "message" when the block names none
	Data    string   // data lines joined with "\n", exactly as received
	HasData bool     // at least one data field was present
	Retry   *int64   // retry field in ms; nil when absent or not all digits
	Comment bool     // the line started with ":"
	Unknown []string // field names the SSE spec does not define
}

// sseReader parses text/event-stream as the WHATWG HTML spec describes it:
// lines end in CRLF, LF or CR, a blank line dispatches the block, and a line
// starting with ":" is a comment. Unlike a browser it also returns blocks
// without data and comment lines, because the probe records everything.
type sseReader struct {
	r       *bufio.Reader
	skipLF  bool // the previous line ended in CR, so a leading LF belongs to it
	fields  int
	id      *string
	event   string
	data    strings.Builder
	hasData bool
	retry   *int64
	unknown []string
}

func newSSEReader(r io.Reader) *sseReader {
	return &sseReader{r: bufio.NewReaderSize(r, 64<<10)}
}

// Next returns the next block or comment. A block cut off by EOF is discarded, as the spec says.
func (s *sseReader) Next() (sseEvent, error) {
	for {
		line, err := s.readLine()
		if err != nil {
			return sseEvent{}, err
		}

		switch {
		case line == "":
			if s.fields > 0 {
				return s.dispatch(), nil
			}
		case line[0] == ':':
			return sseEvent{Event: commentEvent, Data: line[1:], HasData: true, Comment: true}, nil
		default:
			s.field(line)
		}
	}
}

// readLine reads byte by byte so a CR at the end of a chunk dispatches at once
// instead of waiting for the next byte to see whether an LF follows.
func (s *sseReader) readLine() (string, error) {
	var buf []byte

	for {
		b, err := s.r.ReadByte()
		if err != nil {
			return "", err
		}

		if s.skipLF {
			s.skipLF = false

			if b == '\n' {
				continue
			}
		}

		switch b {
		case '\n':
			return string(buf), nil
		case '\r':
			s.skipLF = true

			return string(buf), nil
		}

		if len(buf) >= maxLineBytes {
			return "", errLineTooLong
		}

		buf = append(buf, b)
	}
}

func (s *sseReader) field(line string) {
	name, value, found := strings.Cut(line, ":")
	if found {
		value = strings.TrimPrefix(value, " ")
	}

	s.fields++

	switch name {
	case "event":
		s.event = value
	case "data":
		if s.hasData {
			s.data.WriteByte('\n')
		}

		s.data.WriteString(value)
		s.hasData = true
	case "id":
		if !strings.ContainsRune(value, 0) {
			s.id = &value
		}
	case "retry":
		if n, err := strconv.ParseInt(value, 10, 64); err == nil && allDigits(value) {
			s.retry = &n
		}
	default:
		s.unknown = append(s.unknown, name)
	}
}

func (s *sseReader) dispatch() sseEvent {
	ev := sseEvent{
		ID:      s.id,
		Event:   cmp.Or(s.event, defaultEvent),
		Data:    s.data.String(),
		HasData: s.hasData,
		Retry:   s.retry,
		Unknown: s.unknown,
	}

	s.fields, s.id, s.event, s.hasData, s.retry, s.unknown = 0, nil, "", false, nil, nil
	s.data.Reset()

	return ev
}

func allDigits(v string) bool {
	if v == "" {
		return false
	}

	for _, c := range v {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}
