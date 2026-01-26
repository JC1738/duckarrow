-- Test environment variable fallback for credentials
-- This test requires DUCKARROW_PASSWORD and DUCKARROW_USERNAME to be set
-- Example: export DUCKARROW_PASSWORD='test_pass' DUCKARROW_USERNAME='test_user'

-- Test 1: Configure with empty password string (should use env var)
SELECT duckarrow_configure('grpc+tls://localhost:31337', '', '', true)
  AS env_var_test_1;
-- Expected: Success, password from DUCKARROW_PASSWORD

-- Test 2: Configure with empty username string (should use env var)
SELECT duckarrow_configure('grpc+tls://localhost:31337', '', '', true)
  AS env_var_test_2;
-- Expected: Success, username from DUCKARROW_USERNAME

-- Test 3: Configure with NULL password (should use env var)
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'test_user', NULL, true)
  AS env_var_test_3;
-- Expected: Success, password from DUCKARROW_PASSWORD

-- Test 4: Function parameter overrides env var
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'override_user', 'override_pass', true)
  AS env_var_test_4;
-- Expected: Success, uses 'override_user' and 'override_pass' from parameters

-- Test 5: No env var set, empty parameters (should still work)
SELECT duckarrow_configure('grpc+tls://localhost:31337', '', '', true)
  AS env_var_test_5;
-- Expected: Success, empty username and password
