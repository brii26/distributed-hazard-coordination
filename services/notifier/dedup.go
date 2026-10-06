package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// dedupKey is (hazard_id, severity), not hazard_id alone, so an escalation of the
// same event (SIAGA to AWAS) is a new alert, not a duplicate (doc/TRD_M1.md 8.2).
type dedupKey struct{ hazardID, severity string }

// dedupStore keeps the processed keys on disk and in memory. It is loaded once at
// start and checked in memory; record writes to disk before memory, so a key is
// only ever considered seen once it is safely on disk.
type dedupStore struct {
	mu   sync.Mutex
	seen map[dedupKey]bool
	file *os.File
}

// openDedup loads path (one "hazard_id<TAB>severity" line per record) and opens it
// for appending, creating it and its directory on first start. A line cut short by
// a crash is skipped: its message was never acked, so it will come back.
func openDedup(path string) (*dedupStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}

	d := &dedupStore{seen: map[dedupKey]bool{}}

	f, err := os.Open(path) //nolint:gosec // DEDUP_STORE_PATH is operator config, not request input
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			id, sev, ok := strings.Cut(sc.Text(), "\t")
			if !ok || id == "" || sev == "" {
				log.Printf("dedup store: skipping unreadable line %q", sc.Text())

				continue
			}

			d.seen[dedupKey{id, sev}] = true
		}

		_ = f.Close()

		if err := sc.Err(); err != nil {
			return nil, err
		}
	}

	if d.file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err != nil { //nolint:gosec // see above
		return nil, err
	}

	log.Printf("dedup store loaded path=%s records=%d", path, len(d.seen))

	return d, nil
}

func (d *dedupStore) has(k dedupKey) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.seen[k]
}

// record appends k and syncs it to disk, then remembers it in memory.
func (d *dedupStore) record(k dedupKey) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, err := fmt.Fprintf(d.file, "%s\t%s\n", k.hazardID, k.severity); err != nil {
		return err
	}

	if err := d.file.Sync(); err != nil {
		return err
	}

	d.seen[k] = true

	return nil
}

func (d *dedupStore) close() error { return d.file.Close() }
