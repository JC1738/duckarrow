.bail on
LOAD './build/duckarrow.duckdb_extension';
-- Configure credentials
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'gizmosql_user', 'gizmosql_password', true);
SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT 1 as test_value'
) LIMIT 1;
