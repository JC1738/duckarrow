.bail on
LOAD './build/duckarrow.duckdb_extension';
-- Configure credentials
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'duckarrow_user', 'duckarrow_password', true);
SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT 1 as test_value'
) LIMIT 1;
