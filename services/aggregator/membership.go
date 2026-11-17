package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"time"
)

// Finds store nodes: resolve the headless Service to pod IPs, ask each IP for its bnpb.node_name. Both steps are injected so tests need no cluster.
type membership struct {
	resolve  func(ctx context.Context) ([]string, error)
	nodeName func(ctx context.Context, ip string) (*node, error)
	grace    time.Duration
	now      func() time.Time

	mu       sync.Mutex
	lastSeen map[string]time.Time
	known    map[string]*node
}

func newMembership(resolve func(context.Context) ([]string, error), nodeName func(context.Context, string) (*node, error), grace time.Duration) *membership {
	return &membership{
		resolve: resolve, nodeName: nodeName, grace: grace, now: time.Now,
		lastSeen: map[string]time.Time{}, known: map[string]*node{},
	}
}

// One refresh. A node that answers is live. One that went quiet stays live until grace has passed, so a fast pod restart moves no data.
func (m *membership) refresh(ctx context.Context) {
	ips, err := m.resolve(ctx)
	now := m.now()

	// Probe without the lock: each probe can take seconds and live() must not wait for it.
	var found []*node

	if err == nil {
		for _, ip := range ips {
			if n, nerr := m.nodeName(ctx, ip); nerr == nil {
				n.ip = ip
				found = append(found, n)
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, n := range found {
		m.seen(n, now)
	}

	for name, t := range m.lastSeen {
		if now.Sub(t) > m.grace {
			closeNode(m.known[name])
			delete(m.lastSeen, name)
			delete(m.known, name)
		}
	}
}

// Marks n live. If the node is known at the same IP the fresh pool is closed and the old one kept.
// If the name came back on a new IP (recreated pod) the old pool is closed and the fresh one used.
func (m *membership) seen(n *node, now time.Time) {
	if old, ok := m.known[n.name]; ok {
		if old.ip == n.ip {
			closeNode(n)

			n = old
		} else {
			closeNode(old)
		}
	}

	m.lastSeen[n.name], m.known[n.name] = now, n
}

func closeNode(n *node) {
	if n.db != nil {
		_ = n.db.Close()
	}
}

// The current live nodes, in no particular order.
func (m *membership) live() []*node {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*node, 0, len(m.known))
	for _, n := range m.known {
		out = append(out, n)
	}

	return out
}

// Resolves the headless Service name to the IPs of its ready pods.
func dnsResolver(service string) func(context.Context) ([]string, error) { //nolint:unused // used once the router is wired in
	return func(ctx context.Context) ([]string, error) {
		return net.DefaultResolver.LookupHost(ctx, service)
	}
}

// Opens a small pool to the pod at ip (dsnTemplate has one %s for the host) and reads its node name.
func pgNodeName(dsnTemplate string) func(context.Context, string) (*node, error) { //nolint:unused // used once the router is wired in
	return func(ctx context.Context, ip string) (*node, error) {
		db, err := sql.Open("pgx", fmt.Sprintf(dsnTemplate, ip))
		if err != nil {
			return nil, err
		}

		db.SetMaxOpenConns(4)

		ctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		defer cancel()

		var name string
		if err := db.QueryRowContext(ctx, "SHOW bnpb.node_name").Scan(&name); err != nil {
			_ = db.Close()

			return nil, err
		}

		return &node{name: name, db: db}, nil
	}
}
