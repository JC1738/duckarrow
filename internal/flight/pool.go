package flight

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PooledClient wraps a Client with pool metadata
type PooledClient struct {
	client   *Client
	lastUsed time.Time
	key      string
	inUse    atomic.Bool // Track if connection is actively streaming
}

// Pool manages reusable Flight SQL connections
type Pool struct {
	mu      sync.Mutex
	clients map[string]*PooledClient
	maxIdle time.Duration
}

// ConnectionResult holds a connection and whether it came from the pool
type ConnectionResult struct {
	Client   *Client
	IsPooled bool // If true, use ReleaseConnection; if false, use Client.Close()
}

// Global pool instance
var globalPool = NewPool()

// NewPool creates a new connection pool
func NewPool() *Pool {
	return &Pool{
		clients: make(map[string]*PooledClient),
		maxIdle: 5 * time.Minute, // Default idle timeout
	}
}

// GetConnection gets a connection from the pool or creates a new one.
// Returns ConnectionResult which indicates whether to use ReleaseConnection or Close.
func GetConnection(ctx context.Context, cfg Config) (*ConnectionResult, error) {
	return globalPool.Get(ctx, cfg)
}

// ReleaseConnection returns a pooled connection to the pool.
// Only call this for connections where ConnectionResult.IsPooled is true.
func ReleaseConnection(cfg Config) {
	globalPool.Release(cfg)
}

// ClosePool closes all pooled connections
func ClosePool() {
	globalPool.Close()
}

// configKey generates a unique key using null-byte delimiters to prevent collisions
func (p *Pool) configKey(cfg Config) string {
	h := sha256.New()
	h.Write([]byte(cfg.URI))
	h.Write([]byte{0}) // null byte delimiter
	h.Write([]byte(cfg.Username))
	h.Write([]byte{0})
	h.Write([]byte(cfg.Password))
	h.Write([]byte{0})
	h.Write([]byte(fmt.Sprintf("%v", cfg.SkipVerify)))
	return hex.EncodeToString(h.Sum(nil))
}

// Get retrieves a connection from the pool or creates a new one
func (p *Pool) Get(ctx context.Context, cfg Config) (*ConnectionResult, error) {
	key := p.configKey(cfg)

	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, ok := p.clients[key]; ok {
		// Case 1: Connection in use - create new unmanaged connection
		if pc.inUse.Load() {
			client, err := Connect(ctx, cfg)
			if err != nil {
				return nil, err
			}
			// Return as non-pooled - caller must Close() directly
			return &ConnectionResult{Client: client, IsPooled: false}, nil
		}

		// Case 2: Connection not in use - check health and staleness
		if pc.client.IsHealthy() && time.Since(pc.lastUsed) < p.maxIdle {
			// Healthy and fresh - reuse
			pc.inUse.Store(true)
			pc.lastUsed = time.Now()
			return &ConnectionResult{Client: pc.client, IsPooled: true}, nil
		}

		// Case 3: Unhealthy or stale - close and remove
		pc.client.Close()
		delete(p.clients, key)
	}

	// Create new connection and add to pool
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pc := &PooledClient{
		client:   client,
		lastUsed: time.Now(),
		key:      key,
	}
	pc.inUse.Store(true)
	p.clients[key] = pc

	return &ConnectionResult{Client: client, IsPooled: true}, nil
}

// Release marks a pooled connection as available for reuse
func (p *Pool) Release(cfg Config) {
	key := p.configKey(cfg)

	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, ok := p.clients[key]; ok {
		pc.lastUsed = time.Now()
		pc.inUse.Store(false)
	}
}

// Close closes all connections in the pool
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, pc := range p.clients {
		pc.client.Close()
		delete(p.clients, key)
	}
}
