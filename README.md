# DuckArrow - DuckDB Flight SQL Extension

[![CI](https://github.com/JC1738/duckarrow/actions/workflows/ci.yml/badge.svg)](https://github.com/JC1738/duckarrow/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/Website-gh%20pages-blue)](https://jc1738.github.io/duckarrow/)

A DuckDB extension written in Go that enables querying remote [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) servers directly from DuckDB SQL.

## Features

- **Simple syntax**: Query remote tables with `SELECT * FROM duckarrow."TableName"`
- **DDL/DML support**: Execute CREATE, DROP, INSERT, UPDATE, DELETE via `duckarrow_execute()`
- **Column projection pushdown**: Only fetches requested columns (7-9x speedup)
- **Connection pooling**: Reuses gRPC connections across queries
- **Full type support**: 20+ Arrow types including DECIMAL, LIST, STRUCT, MAP
- **Security**: SQL injection prevention, TLS support, input validation
- **Multi-platform**: Builds for Linux, macOS, and Windows (x86_64 and ARM64)

## Quick Start

> **Note**: This extension requires a Flight SQL server to connect to. It was developed and tested with [GizmoSQL](https://github.com/gizmodata/gizmosql), but should work with any Arrow Flight SQL compliant server.

```bash
# Clone with submodules
git clone --recursive <repo-url>
cd duckarrow

# Build the extension
make build

# Load in DuckDB (requires -unsigned flag)
duckdb -unsigned
```

```sql
-- Load the extension
LOAD './build/linux_amd64/duckarrow.duckdb_extension';

-- Configure your Flight SQL server
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'username', 'password');

-- Query remote tables
SELECT * FROM duckarrow."Orders" LIMIT 10;
SELECT id, customer_name FROM duckarrow."Orders" WHERE status = 'COMPLETED';
```

## Installation

### Download Pre-built Release (Recommended)

Download the latest release for your platform from [GitHub Releases](https://github.com/JC1738/duckarrow/releases):

```bash
# Using gh CLI (recommended)
gh release download --repo JC1738/duckarrow --pattern "*linux_amd64*"    # Linux x86_64
gh release download --repo JC1738/duckarrow --pattern "*osx_arm64*"      # macOS Apple Silicon
gh release download --repo JC1738/duckarrow --pattern "*osx_amd64*"      # macOS Intel
gh release download --repo JC1738/duckarrow --pattern "*windows_amd64*"  # Windows

# Or download specific version with curl (replace VERSION with desired tag)
VERSION=v0.0.3
curl -LO "https://github.com/JC1738/duckarrow/releases/download/${VERSION}/duckarrow-${VERSION}-linux_amd64.duckdb_extension"
```

### Build from Source

#### Prerequisites

- Go 1.24.0+
- Python 3 (for metadata embedding)
- CGO enabled

#### Clone and Build

```bash
# Clone with submodules
git clone --recursive <repo-url>
cd duckarrow

# Or if already cloned without --recursive:
git submodule update --init

# Build for current platform
make build
# Output: build/<platform>/duckarrow.duckdb_extension
```

### Multi-Platform Builds

```bash
# Auto-detect current platform
make build

# Build for specific platforms
make build-linux-amd64        # Linux x86_64
make build-linux-arm64        # Linux ARM64
make build-darwin-amd64       # macOS Intel
make build-darwin-arm64       # macOS Apple Silicon
make build-windows-amd64      # Windows x86_64
make build-windows-arm64      # Windows ARM64

# Build all platforms for an OS
make build-linux              # Both Linux platforms
make build-darwin             # Both macOS platforms
make build-windows            # Both Windows platforms
```

**Note**: Cross-compilation requires appropriate C toolchains. Native builds on each platform are recommended.

### Verify Installation

```bash
duckdb -unsigned -c "LOAD './build/linux_amd64/duckarrow.duckdb_extension';"
# Should complete without error
```

## Usage

### Check Version

```sql
SELECT duckarrow_version();
-- Returns: v0.0.3 (or "dev" for local builds)
```

### Configuration

Configure your Flight SQL server credentials once per session:

```sql
SELECT duckarrow_configure(uri, username, password);

-- For self-signed certificates, skip TLS verification:
SELECT duckarrow_configure(uri, username, password, true);
```

| Parameter | Type | Required | Default | Example |
|-----------|------|----------|---------|---------|
| uri | VARCHAR | Yes | - | `'grpc+tls://localhost:31337'` |
| username | VARCHAR | No | `''` | `'admin'` |
| password | VARCHAR | No | `''` | `'secret'` |
| skip_verify | BOOLEAN | No | `false` | `true` |

**Supported URI schemes:**
- `grpc://` - Unencrypted gRPC
- `grpc+tls://` - TLS-encrypted gRPC (recommended)

**TLS Certificate Verification:**
- By default, TLS certificates are verified (`skip_verify = false`)
- Set `skip_verify = true` only for development/testing with self-signed certificates
- For production, use properly signed certificates and keep verification enabled

### Password Security

**⚠️ Security Notice**: DuckDB v1.2.0 CLI displays all function parameters in plain text.
To avoid exposing passwords in terminal history or screen sharing:

**Option 1 - Environment Variable (Recommended)**:
```bash
export DUCKARROW_PASSWORD='your_secret_password'
export DUCKARROW_USERNAME='your_username'  # Optional
duckdb <<EOF
  LOAD './build/duckarrow.duckdb_extension';
  SELECT duckarrow_configure('grpc+tls://localhost:31337', 'username', '', true);
  SELECT * FROM duckarrow."TableName" LIMIT 10;
EOF
```

**Option 2 - Traditional (Password visible in CLI)**:
```sql
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'username', 'password');
-- ⚠️ Password will be visible in terminal and duckdb history
```

**Environment Variables**:
- `DUCKARROW_PASSWORD`: Password fallback when password parameter is empty string
- `DUCKARROW_USERNAME`: Username fallback when username parameter is empty string

**Priority Order**: Function parameter > Environment variable > Empty string

### Query Syntax

**Replacement scan (recommended):**
```sql
-- Simple select
SELECT * FROM duckarrow."TableName";

-- With column projection (only fetches needed columns)
SELECT id, name FROM duckarrow."Orders";

-- Filtering, aggregation, joins all work
SELECT COUNT(*) FROM duckarrow."Orders" WHERE status = 'COMPLETED';
```

**Direct table function:**
```sql
SELECT * FROM duckarrow_query(
    'grpc+tls://server:port',
    'SELECT * FROM "TableName"'
);
```

### DDL/DML Execution

For statements that don't return results (CREATE, DROP, INSERT, UPDATE, DELETE), use `duckarrow_execute()`:

```sql
-- Create a table on the remote server
SELECT duckarrow_execute('CREATE TABLE "my_table" (id INTEGER, name VARCHAR)');

-- Insert data
SELECT duckarrow_execute('INSERT INTO "my_table" VALUES (1, ''Alice'')');

-- Drop a table
SELECT duckarrow_execute('DROP TABLE "my_table"');
```

The function returns the number of affected rows (or -1 if the server doesn't provide this information).

**Note**: Unlike `duckarrow.*` syntax which only works for SELECT queries, `duckarrow_execute()` is required for DDL/DML because DuckDB's replacement scan only intercepts table references in FROM clauses.

### Examples

```sql
-- Basic queries
SELECT * FROM duckarrow."Orders" LIMIT 5;
SELECT id, customer_name, total FROM duckarrow."Orders";

-- Aggregation
SELECT status, COUNT(*) as count, SUM(total) as revenue
FROM duckarrow."Orders"
GROUP BY status;

-- Join with local data
CREATE TEMP TABLE local_ids AS SELECT 1 as id UNION SELECT 2;
SELECT o.* FROM duckarrow."Orders" o JOIN local_ids l ON o.id = l.id;

-- Subqueries
SELECT * FROM (
    SELECT id, name FROM duckarrow."Products" LIMIT 100
) sub WHERE id > 50;

-- Inspect schema
DESCRIBE SELECT * FROM duckarrow."Orders";
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User SQL Query                          │
│            SELECT id, name FROM duckarrow."Orders"              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Replacement Scan                             │
│  • Validates table name (SQL injection prevention)              │
│  • Rewrites to duckarrow_query() table function                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Table Function - Bind Phase                     │
│  • Connect to Flight SQL server (via connection pool)           │
│  • Execute schema query: SELECT * FROM "Orders" WHERE 1=0       │
│  • Store column metadata for projection pushdown                │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Table Function - Init Phase                     │
│  • DuckDB provides list of needed columns                       │
│  • Build optimized query: SELECT id, name FROM "Orders"         │
│  • Execute query with Flight SQL client                         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Table Function - Scan Phase                     │
│  • Stream Arrow RecordBatches from Flight SQL                   │
│  • Convert Arrow → DuckDB (type mapping, NULL handling)         │
│  • Return data chunks to DuckDB                                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                       DuckDB Result                             │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
duckarrow/
├── main.go                     # Extension entry point, CGO bindings
├── table_function.go           # Core table function, type conversion
├── replacement_scan.go         # duckarrow.* syntax rewriter
├── config_function.go          # duckarrow_configure() function
├── execute_function.go         # duckarrow_execute() for DDL/DML
├── version_function.go         # duckarrow_version() function
├── query_builder.go            # Query construction with projection
├── internal/
│   ├── flight/
│   │   ├── client.go          # Flight SQL client (ADBC wrapper)
│   │   ├── pool.go            # Connection pooling
│   │   └── pool_test.go       # Pool tests
│   └── validation/
│       ├── validation.go      # Input validation
│       └── validation_test.go # Validation tests
├── tests/                      # SQL integration tests
├── duckdb-go-api/             # Git submodule: DuckDB headers & Go API
├── build/                      # Compiled extension output
│   └── <platform>/            # Platform-specific builds
├── Makefile                    # Build & test automation
└── CMakeLists.txt             # CMake wrapper for community extensions
```

## Supported Data Types

| Arrow Type | DuckDB Type | Notes |
|------------|-------------|-------|
| INT8/16/32/64 | TINYINT/SMALLINT/INTEGER/BIGINT | Full range |
| UINT8/16/32/64 | UTINYINT/USMALLINT/UINTEGER/UBIGINT | Full range |
| FLOAT32/64 | FLOAT/DOUBLE | Includes Infinity, NaN |
| BOOL | BOOLEAN | |
| STRING/LARGE_STRING | VARCHAR | UTF-8 |
| BINARY/LARGE_BINARY | BLOB | |
| TIMESTAMP | TIMESTAMP | Any precision |
| DATE32/64 | DATE | |
| TIME32/64 | TIME | |
| DECIMAL128/256 | DECIMAL | Native precision |
| LIST/LARGE_LIST | LIST | Recursive |
| STRUCT | STRUCT | Recursive |
| MAP | MAP | As LIST of STRUCT |

## Testing

### Go Unit Tests

```bash
make test-unit          # Run all unit tests
make test-coverage      # Run with coverage report
```

Coverage: 92.9% for validation, 21.7% for pool (unit-testable parts)

### SQL Integration Tests

SQL integration tests require a Flight SQL server running at `localhost:31337`. We recommend [GizmoSQL](https://github.com/gizmodata/gizmosql) for testing.

```bash
make test               # Core SQL integration tests
make test-all           # Full suite (unit + all SQL tests)

# Individual test targets
make test-types         # Data type conversions
make test-edge-cases    # Edge cases (unicode, large results)
make test-errors        # Error handling, SQL injection prevention
```

### Test Coverage

| Area | Status |
|------|--------|
| Data Types | ✓ All 20+ types with NULL handling |
| Large Results | ✓ 5000+ rows, multi-batch streaming |
| Unicode | ✓ UTF-8 in data and identifiers |
| SQL Injection | ✓ Semicolon, comments blocked |
| Connection Pool | ✓ Concurrent access, reuse |
| Error Recovery | ✓ Graceful failure handling |

## Development

### Makefile Targets

```bash
make build               # Build for current platform
make build-linux-amd64   # Build for specific platform
make clean               # Remove build artifacts
make deps                # Install dependencies
make fmt                 # Format Go code
make test                # Run SQL tests
make test-unit           # Run Go unit tests
make test-coverage       # Coverage report
make test-all            # Full test suite
make help                # Show all targets
```

### Feature Status

| Feature | Status |
|---------|--------|
| Core functionality | ✓ Complete |
| Connection pooling | ✓ Complete |
| Column projection pushdown | ✓ Complete |
| Complex types (DECIMAL, LIST, STRUCT, MAP) | ✓ Complete |
| Predicate pushdown | Blocked (DuckDB PR #14591) |
| Multi-server support | Planned |

## Performance

Column projection pushdown provides significant speedups when selecting subset of columns:

| Query | Columns | Improvement |
|-------|---------|-------------|
| `SELECT *` | 90 | Baseline |
| `SELECT id, name` | 2 | ~7.5x faster |
| `SELECT id` | 1 | ~9x faster |

Connection pooling reduces overhead for subsequent queries from ~100ms to ~5ms.

## Security

- **SQL injection prevention**: Table names validated against dangerous patterns (`;`, `--`, `/*`, control characters)
- **URI validation**: Only `grpc://` and `grpc+tls://` schemes allowed
- **TLS support**: Encrypted connections via `grpc+tls://`
- **Input length limits**: Table names (255 chars), URIs (2048 chars)

## Limitations

- **Predicate pushdown not yet implemented**: WHERE clauses filtered locally
- **Single server per session**: Cannot query multiple Flight SQL servers simultaneously
- **No catalog integration**: Remote tables don't appear in `information_schema`
- **DDL/DML requires explicit function**: Use `duckarrow_execute()` for CREATE/DROP/INSERT/UPDATE/DELETE (the `duckarrow.*` syntax only works for SELECT)

## Dependencies

- [Apache Arrow ADBC](https://github.com/apache/arrow-adbc) - Flight SQL client
- [Apache Arrow Go](https://github.com/apache/arrow-go) - Arrow format handling
- [gRPC](https://grpc.io/) - Transport layer

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `make test-all`
4. Submit a pull request

CI runs automatically on pull requests (lint, unit tests, multi-platform builds).
