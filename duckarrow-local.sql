-- duckarrow-local.sql
-- DuckDB init file for loading the duckarrow extension
-- Usage: duckdb -unsigned --init ./duckarrow-local.sql

-- Load the extension (requires -unsigned flag)
LOAD './build/duckarrow.duckdb_extension';

-- Configure duckarrow with credentials (enables replacement scan)
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'duckarrow_user', 'duckarrow_password');

-- Query remote tables directly using duckarrow."TableName" syntax:
--   SELECT * FROM duckarrow."Order" LIMIT 5;
--   SELECT id, name, status FROM duckarrow."Order" WHERE status = 'COMPLETED';
--   SELECT COUNT(*) FROM duckarrow."Order";
