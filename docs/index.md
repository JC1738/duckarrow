# DuckArrow

<img src="duck_arrow.jpg" alt="DuckArrow Logo" width="200">

**Query Apache Arrow Flight SQL servers directly from DuckDB**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Linux](https://img.shields.io/badge/Linux-x86__64%20%7C%20ARM64-blue)]()
[![macOS](https://img.shields.io/badge/macOS-x86__64%20%7C%20ARM64-blue)]()
[![Windows](https://img.shields.io/badge/Windows-x86__64%20%7C%20ARM64-blue)]()

## Why DuckArrow?

DuckArrow bridges DuckDB with Apache Arrow Flight SQL servers, letting you query remote data with familiar SQL syntax. No ETL pipelines, no data movement—just query.

## Features

- **Simple Syntax** — Query remote tables with `SELECT * FROM duckarrow."TableName"`
- **Blazing Fast** — 7-9x speedup with column projection pushdown
- **Connection Pooling** — Reuses gRPC connections for quick subsequent queries
- **Full Type Support** — 20+ Arrow types including DECIMAL, LIST, STRUCT, MAP
- **Secure** — SQL injection prevention, TLS encryption support
- **Cross-Platform** — Linux, macOS, Windows (x86_64 and ARM64)

## Quick Start

```sql
-- Load the extension
LOAD './duckarrow.duckdb_extension';

-- Configure your Flight SQL server
SELECT duckarrow_configure('grpc+tls://server:31337', 'username', 'password');

-- Query remote tables like they're local
SELECT id, customer_name, total FROM duckarrow."Orders" LIMIT 10;
```

## Learn More

- [Full Documentation & Installation Guide](https://github.com/jc1738/duckarrow#readme)
- [GitHub Repository](https://github.com/jc1738/duckarrow)

---

*MIT License — Built with DuckDB + Apache Arrow*
