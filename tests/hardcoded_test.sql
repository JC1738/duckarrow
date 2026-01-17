.bail on
LOAD './build/duckarrow.duckdb_extension';
-- Basic extension load and function registration test
-- Verify the extension loads and functions are available
SELECT 'Extension loaded successfully' as status;
