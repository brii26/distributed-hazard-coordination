package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const maxJSONDepth = 64

var errNotArray = errors.New("body is not a JSON array")

// A record or a stretch of body that was set aside before mapping, with why.
type rejectedRaw struct {
	raw []byte
	err error
}

// Says whether one record is safe to hand to the mapper, cheapest test first: size, UTF-8, nesting depth, then JSON syntax.
// The depth scan is a flat loop, so a deeply nested input cannot overflow the stack.
func checkRecord(raw []byte) error {
	if len(raw) > maxFailedPayload {
		return errOversize
	}

	return checkShape(raw)
}

func checkShape(raw []byte) error {
	if !utf8.Valid(raw) {
		return errNotUTF8
	}

	depth, inStr, esc := 0, false, false

	for _, c := range raw {
		switch {
		case esc:
			esc = false
		case inStr:
			inStr, esc = c != '"', c == '\\'
		case c == '"':
			inStr = true
		case c == '{' || c == '[':
			if depth++; depth > maxJSONDepth {
				return errTooDeep
			}
		case c == '}' || c == ']':
			depth--
		}
	}

	if json.Valid(raw) {
		return nil
	}

	var v any

	return json.Unmarshal(raw, &v)
}

// Reads a whole poll body. A body longer than limit is cut at limit and reported as cut. A read error (the network dropped it) is returned as is.
func readBody(r io.Reader, limit int) (body []byte, cut bool, err error) {
	body, err = io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, false, err
	}

	if len(body) > limit {
		return body[:limit], true, nil
	}

	return body, false, nil
}

var errNullRecord = errors.New("record is null, not an object")

// Finds where the array element that starts at from ends: the offset of the "," or "]" that closes it at depth 0.
// A flat loop that knows strings and counts brackets, so it never recurses and has no depth limit of its own
// (encoding/json refuses past 10000 levels, which is why the decoder is not used to find the end). ok is false when the body ends first.
func elementEnd(body []byte, from int) (end int, ok bool) {
	depth, inStr, esc := 0, false, false

	for i := from; i < len(body); i++ {
		c := body[i]

		switch {
		case esc:
			esc = false
		case inStr:
			inStr, esc = c != '"', c == '\\'
		case c == '"':
			inStr = true
		case c == '{' || c == '[':
			depth++
		case c == '}':
			depth--
		case c == ']':
			if depth == 0 {
				return i, true
			}

			depth--
		case c == ',' && depth <= 0:
			return i, true
		}
	}

	return len(body), false
}

func skipSpace(body []byte, i int) int {
	for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
		i++
	}

	return i
}

// Streams the elements of a JSON array whose "[" ends just before body[pos]. Each element is cut out with elementEnd, so a record
// of any depth costs only its own slice: good ones go to keep, bad ones come back with their reason and the stream goes on.
// next is the offset after the closing "]". A break in the structure ends the stream: the rest of the body comes back as one
// rejected stretch, or as too large when the body was cut at the cap.
func streamArray(body []byte, pos int, cut bool, keep func(json.RawMessage) error) (bad []rejectedRaw, next int, broken bool) {
	rest := func(from int, err error) ([]rejectedRaw, int, bool) {
		tail := body[min(from, len(body)):]
		if cut {
			err = errOversize
		} else if e := checkShape(tail); e != nil {
			err = e
		}

		return append(bad, rejectedRaw{tail, err}), len(body), true
	}

	if pos = skipSpace(body, pos); pos < len(body) && body[pos] == ']' {
		return nil, pos + 1, false
	}

	for {
		start := skipSpace(body, pos)

		end, ok := elementEnd(body, start)
		if !ok {
			return rest(start, io.ErrUnexpectedEOF)
		}

		e := bytes.TrimRight(body[start:end], " \t\r\n")

		switch err := checkRecord(e); {
		case err != nil:
			bad = append(bad, rejectedRaw{e, err})
		case bytes.Equal(e, []byte("null")):
			bad = append(bad, rejectedRaw{e, errNullRecord})
		default:
			if err := keep(json.RawMessage(e)); err != nil {
				bad = append(bad, rejectedRaw{e, err})
			}
		}

		if body[end] == ']' {
			return bad, end + 1, false
		}

		pos = end + 1
	}
}

// Decodes a body that is one JSON array of objects. A record that is not an object is rejected like any other bad record.
func decodeRecords(body []byte, cut bool) (recs []map[string]any, raws [][]byte, bad []rejectedRaw, err error) {
	dec := json.NewDecoder(bytes.NewReader(body))

	tok, terr := dec.Token()
	if terr == nil && tok == nil {
		return nil, nil, nil, nil
	}

	if terr != nil || tok != json.Delim('[') {
		if cut {
			terr = errOversize
		} else if terr = checkShape(body); terr == nil {
			terr = errNotArray
		}

		return nil, nil, []rejectedRaw{{body, terr}}, terr
	}

	recs, raws = []map[string]any{}, [][]byte{}

	bad, _, _ = streamArray(body, int(dec.InputOffset()), cut, func(e json.RawMessage) error {
		var m map[string]any
		if uerr := json.Unmarshal(e, &m); uerr != nil {
			return uerr
		}

		recs, raws = append(recs, m), append(raws, e)

		return nil
	})

	return recs, raws, bad, nil
}
