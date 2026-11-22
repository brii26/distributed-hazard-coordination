package main

import (
	"log"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

const typeNumber = "number"

// What the operator declared for PVMBG fields that drifted from the contract. The zero value changes nothing.
type schemaAdjust struct {
	rename   map[string]string // name the source now uses -> contract name
	toNumber map[string]bool
	defaults map[string]any
}

func (a schemaAdjust) empty() bool { return len(a.rename)+len(a.toNumber)+len(a.defaults) == 0 }

// Reads PVMBG_SCHEMA_ADJUST: comma separated rename:<new>=<contract name>, type:<field> and default:<field>=<value>.
// A term that cannot apply is logged and skipped.
// ponytail: a default value cannot hold a comma; move to a JSON value if one is ever needed.
func parseSchemaAdjust(raw string) schemaAdjust {
	a := schemaAdjust{rename: map[string]string{}, toNumber: map[string]bool{}, defaults: map[string]any{}}

	for term := range strings.SplitSeq(raw, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}

		if why := a.add(term); why != "" {
			log.Printf("WARNING: PVMBG_SCHEMA_ADJUST term ignored: %q: %s", term, why)
		}
	}

	if !a.empty() {
		log.Printf("PVMBG schema adjustments active: %d rename, %d type, %d default", len(a.rename), len(a.toNumber), len(a.defaults))
	}

	return a
}

// Adds one term and returns why it was refused, or an empty string.
func (a schemaAdjust) add(term string) string {
	kind, rest, _ := strings.Cut(term, ":")
	left, right, hasEq := strings.Cut(rest, "=")
	contract := knownFields[sourcePVMBG]

	switch kind {
	case "rename":
		return a.addRename(left, right, hasEq, contract)
	case "type":
		if hasEq || contract[rest] != typeNumber {
			return "want type:<contract field of type number>"
		}

		a.toNumber[rest] = true

		return ""
	case "default":
		return a.addDefault(left, right, hasEq, contract)
	}

	return "unknown kind, want rename, type or default"
}

func (a schemaAdjust) addRename(from, to string, hasEq bool, contract map[string]string) string {
	if !hasEq || from == "" || contract[to] == "" {
		return "want rename:<new name>=<contract field>"
	}

	if contract[from] != "" {
		return "the new name is already a contract field"
	}

	a.rename[from] = to

	return ""
}

func (a schemaAdjust) addDefault(field, value string, hasEq bool, contract map[string]string) string {
	if !hasEq || contract[field] == "" || value == "" {
		return "want default:<contract field>=<value>"
	}

	if field == "report_id" || field == "reported_at" {
		return "the id and the time of a report must come from the source"
	}

	if contract[field] != typeNumber {
		a.defaults[field] = value

		return ""
	}

	f, ok := finiteNumber(value)
	if !ok {
		return "the value is not a number"
	}

	a.defaults[field] = f

	return ""
}

func finiteNumber(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)

	return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
}

// Applies the declared adjustments in the order rename, text to number, default.
// Works on a copy, never on the source record. The second result lists what was applied.
func (a schemaAdjust) apply(r map[string]any) (map[string]any, []string) {
	if a.empty() {
		return r, nil
	}

	var marks []string

	out := r
	edit := func(mark string) {
		if len(marks) == 0 {
			out = maps.Clone(r)
		}

		marks = append(marks, mark)
	}

	for from, to := range a.rename {
		if v := r[from]; v != nil && out[to] == nil {
			edit("rename:" + from)

			out[to] = v
			delete(out, from)
		}
	}

	for field := range a.toNumber {
		s, ok := out[field].(string)
		if !ok {
			continue
		}

		if f, ok := finiteNumber(s); ok {
			edit("type:" + field)

			out[field] = f
		}
	}

	for field, v := range a.defaults {
		if out[field] == nil {
			edit("default:" + field)

			out[field] = v
		}
	}

	slices.Sort(marks)

	return out, marks
}
