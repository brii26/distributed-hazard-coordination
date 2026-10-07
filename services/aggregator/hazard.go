package main

import (
	"crypto/rand"
	"fmt"
	"time"
)

const (
	sourceBMKG  = "BMKG"
	sourcePVMBG = "PVMBG"

	typeSeismic  = "SEISMIC"
	typeVolcanic = "VOLCANIC"

	severityNormal  = "NORMAL"
	severityWaspada = "WASPADA"
	severitySiaga   = "SIAGA"
	severityAwas    = "AWAS"
)

// record kanonik hazard, field yang nggak disediakan source jadi pointer biar keluar null.
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    *string        `json:"severity"`
	AreaName    *string        `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

// UUID v4 random, tanpa dependency.
func newHazardID() string {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}

	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
