package flight

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config for DuckArrow Flight SQL connection
type Config struct {
	URI        string // e.g., "grpc+tls://localhost:31337"
	Username   string
	Password   string
	SkipVerify bool
}

// Client wraps ADBC Flight SQL connection
type Client struct {
	db   adbc.Database
	conn adbc.Connection
}

// Connect establishes connection to Flight SQL server
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	drv := flightsql.NewDriver(nil)

	opts := map[string]string{
		adbc.OptionKeyURI:      cfg.URI,
		adbc.OptionKeyUsername: cfg.Username,
		adbc.OptionKeyPassword: cfg.Password,
	}

	if cfg.SkipVerify {
		opts[flightsql.OptionSSLSkipVerify] = "true"
	}

	// Increase gRPC message size from 16MB to 256MB for large result sets
	maxMsgSize := 256 * 1024 * 1024
	dialOpts := grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(maxMsgSize),
		grpc.MaxCallSendMsgSize(maxMsgSize),
	)

	// Add gRPC keepalive to prevent stale connections
	// Use conservative settings to avoid server's ENHANCE_YOUR_CALM/too_many_pings
	keepaliveOpts := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                2 * time.Minute,  // Ping interval during active streams
		Timeout:             20 * time.Second, // Wait 20s for ping response
		PermitWithoutStream: false,            // Only ping with active streams
	})

	db, err := drv.NewDatabaseWithOptions(opts, dialOpts, keepaliveOpts)
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open connection: %w", err)
	}

	return &Client{db: db, conn: conn}, nil
}

// QueryResult holds the reader and statement for cleanup
type QueryResult struct {
	Reader array.RecordReader
	Stmt   adbc.Statement
}

// Query executes SQL and returns Arrow RecordReader
// Note: Caller must call result.Reader.Release() and result.Stmt.Close() when done
func (c *Client) Query(ctx context.Context, sql string) (*QueryResult, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("create statement: %w", err)
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("execute query: %w", err)
	}

	return &QueryResult{
		Reader: reader,
		Stmt:   stmt,
	}, nil
}

// IsHealthy checks if the connection is still valid
func (c *Client) IsHealthy() bool {
	return c.conn != nil && c.db != nil
}

// Close closes connection and database
func (c *Client) Close() error {
	var errs []error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
