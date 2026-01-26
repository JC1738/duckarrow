package flight

import (
	"sync"
	"testing"
)

func TestConfigKey(t *testing.T) {
	pool := NewPool()

	tests := []struct {
		name        string
		cfg1        Config
		cfg2        Config
		shouldMatch bool
	}{
		{
			name:        "identical configs same key",
			cfg1:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass", SkipVerify: false},
			cfg2:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass", SkipVerify: false},
			shouldMatch: true,
		},
		{
			name:        "different URI different key",
			cfg1:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass"},
			cfg2:        Config{URI: "grpc://localhost:8080", Username: "user", Password: "pass"},
			shouldMatch: false,
		},
		{
			name:        "different username different key",
			cfg1:        Config{URI: "grpc://localhost:31337", Username: "user1", Password: "pass"},
			cfg2:        Config{URI: "grpc://localhost:31337", Username: "user2", Password: "pass"},
			shouldMatch: false,
		},
		{
			name:        "different password different key",
			cfg1:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass1"},
			cfg2:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass2"},
			shouldMatch: false,
		},
		{
			name:        "different skipVerify different key",
			cfg1:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass", SkipVerify: false},
			cfg2:        Config{URI: "grpc://localhost:31337", Username: "user", Password: "pass", SkipVerify: true},
			shouldMatch: false,
		},
		{
			name:        "empty configs same key",
			cfg1:        Config{},
			cfg2:        Config{},
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1 := pool.configKey(tt.cfg1)
			key2 := pool.configKey(tt.cfg2)

			if tt.shouldMatch && key1 != key2 {
				t.Errorf("expected same key for identical configs, got %q and %q", key1, key2)
			}
			if !tt.shouldMatch && key1 == key2 {
				t.Errorf("expected different keys, got same key %q", key1)
			}
		})
	}
}

func TestConfigKeyCollisionAttack(t *testing.T) {
	// Test that null-byte delimiters prevent collision attacks
	// Without proper delimiting, these could produce the same concatenated string
	pool := NewPool()

	// Attack vector: user="u\x00p", pass="" vs user="u", pass="\x00p"
	// If we just concatenate, both would be "u\x00p"
	cfg1 := Config{URI: "grpc://localhost:31337", Username: "u\x00p", Password: ""}
	cfg2 := Config{URI: "grpc://localhost:31337", Username: "u", Password: "\x00p"}

	key1 := pool.configKey(cfg1)
	key2 := pool.configKey(cfg2)

	if key1 == key2 {
		t.Error("collision attack: different configs produced same key")
	}
}

func TestPoolClose(t *testing.T) {
	// Test that Close() on an empty pool doesn't panic
	pool := NewPool()
	pool.Close() // Should not panic

	// Verify pool is empty after close
	if len(pool.clients) != 0 {
		t.Errorf("expected empty clients map after Close, got %d entries", len(pool.clients))
	}
}

func TestPoolCloseMultipleTimes(t *testing.T) {
	// Test that Close() can be called multiple times without panic
	pool := NewPool()
	pool.Close()
	pool.Close() // Second close should not panic
	pool.Close() // Third close should not panic
}

func TestClientIsHealthy(t *testing.T) {
	// Note: We can't easily test with non-nil db/conn without a real server,
	// but we can test the nil cases which are the failure paths.
	// The IsHealthy() function returns true only when BOTH db and conn are non-nil.

	t.Run("both nil returns false", func(t *testing.T) {
		client := &Client{db: nil, conn: nil}
		if client.IsHealthy() {
			t.Error("IsHealthy() with both nil should return false")
		}
	})

	// Note: Testing with only one nil requires mock interfaces which aren't
	// easily available without significant refactoring. The implementation
	// uses `c.conn != nil && c.db != nil`, so both must be non-nil to return true.
}

func TestNewPool(t *testing.T) {
	pool := NewPool()

	if pool == nil {
		t.Fatal("NewPool() returned nil")
	}
	if pool.clients == nil {
		t.Error("NewPool() clients map is nil")
	}
	if len(pool.clients) != 0 {
		t.Errorf("NewPool() clients should be empty, got %d entries", len(pool.clients))
	}
	if pool.maxIdle == 0 {
		t.Error("NewPool() maxIdle should be set")
	}
}

func TestConfigKeyDeterministic(t *testing.T) {
	pool := NewPool()
	cfg := Config{
		URI:        "grpc+tls://server.example.com:443",
		Username:   "testuser",
		Password:   "testpass",
		SkipVerify: true,
	}

	// Generate key multiple times - should be identical
	key1 := pool.configKey(cfg)
	key2 := pool.configKey(cfg)
	key3 := pool.configKey(cfg)

	if key1 != key2 || key2 != key3 {
		t.Errorf("configKey is not deterministic: %q, %q, %q", key1, key2, key3)
	}

	// Key should be a valid hex-encoded SHA256 hash (64 chars)
	if len(key1) != 64 {
		t.Errorf("configKey length = %d, want 64 (SHA256 hex)", len(key1))
	}
}

func TestPoolReleaseConcurrent(t *testing.T) {
	// Test concurrent access to pool operations is safe
	// Note: This tests the mutex protection, not actual connections
	pool := NewPool()
	defer pool.Close()

	const numGoroutines = 50
	const numIterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Concurrent configKey generation (read-only, should be safe)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			cfg := Config{
				URI:      "grpc://localhost:31337",
				Username: "user",
				Password: "pass",
			}
			for j := 0; j < numIterations; j++ {
				_ = pool.configKey(cfg)
			}
		}(i)
	}

	// Concurrent Release calls (tests mutex)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			cfg := Config{
				URI:      "grpc://localhost:31337",
				Username: "user",
				Password: "pass",
			}
			for j := 0; j < numIterations; j++ {
				pool.Release(cfg) // Release on non-existent key should be safe
			}
		}(i)
	}

	wg.Wait()
}

func TestPoolReleaseUnknownKey(t *testing.T) {
	// Test that Release() with a config that was never in the pool doesn't panic
	pool := NewPool()
	defer pool.Close()

	unknownConfig := Config{
		URI:      "grpc://unknown:9999",
		Username: "nobody",
		Password: "nothing",
	}

	// Should not panic or cause any issues
	pool.Release(unknownConfig)

	// Pool should still be empty
	if len(pool.clients) != 0 {
		t.Errorf("expected empty pool after releasing unknown config, got %d entries", len(pool.clients))
	}
}

func TestPoolConcurrentCloseAndRelease(t *testing.T) {
	// Test that concurrent Close and Release operations don't cause race conditions
	pool := NewPool()

	var wg sync.WaitGroup
	wg.Add(3)

	// Multiple goroutines trying to release
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.Release(Config{URI: "grpc://test:1234"})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.Release(Config{URI: "grpc://test:5678"})
		}
	}()

	// Close while releases are happening
	go func() {
		defer wg.Done()
		pool.Close()
	}()

	wg.Wait()
}
