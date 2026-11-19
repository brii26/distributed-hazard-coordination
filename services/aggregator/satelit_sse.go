package main

import (
	"bufio"
	"cmp"
	"errors"
	"io"
	"strings"
)

// A text/event-stream parser, copied from tools/feedprobe/sse.go (a separate module, so it cannot be imported) and cut down to the id, event and data fields. Comment lines and retry are dropped: the feed sends neither (F8, F12). Keep the line handling in step with the original.

// sseMaxLine bounds one line so a broken stream cannot grow memory without limit.
const sseMaxLine = 1 << 20

var errSSELineTooLong = errors.New("sse line longer than 1 MiB")

type sseEvent struct {
	ID    *string // nil when the block has no id field
	Event string  // "message" when the block names none
	Data  string  // data lines joined with "\n"
}

// sseReader follows the WHATWG rules: lines end in CRLF, LF or CR, and a blank line dispatches the block. A block cut off by EOF is discarded.
type sseReader struct {
	r      *bufio.Reader
	skipLF bool // the previous line ended in CR, so a leading LF belongs to it
	fields int
	id     *string
	event  string
	data   strings.Builder
	inData bool
}

func newSSEReader(r io.Reader) *sseReader {
	return &sseReader{r: bufio.NewReaderSize(r, 64<<10)}
}

func (s *sseReader) next() (sseEvent, error) {
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
		case line[0] == ':': // comment
		default:
			s.field(line)
		}
	}
}

// readLine reads byte by byte so a CR at the end of a chunk ends the line at once instead of waiting to see whether an LF follows.
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

		if len(buf) >= sseMaxLine {
			return "", errSSELineTooLong
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
		if s.inData {
			s.data.WriteByte('\n')
		}

		s.data.WriteString(value)
		s.inData = true
	case "id":
		if !strings.ContainsRune(value, 0) {
			s.id = &value
		}
	}
}

func (s *sseReader) dispatch() sseEvent {
	ev := sseEvent{ID: s.id, Event: cmp.Or(s.event, "message"), Data: s.data.String()}

	s.fields, s.id, s.event, s.inData = 0, nil, "", false
	s.data.Reset()

	return ev
}
