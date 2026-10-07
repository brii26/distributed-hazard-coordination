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

// Dedup key is (hazard_id, severity), so an escalation like SIAGA to AWAS is logged again.
type dedupKey struct{ hazardID, severity string }

// Keeps processed keys on disk and in memory. Loaded once at start, record writes to disk first so a key only counts as seen once it is safe.
type dedupStore struct {
	mu   sync.Mutex
	seen map[dedupKey]bool
	file *os.File
}

// Loads path (one "hazard_id<TAB>severity" line per record) and opens it for appending. A line cut off by a crash is skipped, its message was never acked.
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

// Appends k, syncs to disk, then remembers it in memory.
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
