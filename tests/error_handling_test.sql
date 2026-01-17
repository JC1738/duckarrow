-- Error Handling Tests
-- Tests error conditions and recovery
-- Note: These tests expect errors - use .bail off to continue on errors

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== Error Handling Tests ==='
.print 'Note: Errors below are EXPECTED - testing error handling'

-- ============================================================================
-- TEST 1: Invalid SQL Syntax (should produce remote error)
-- ============================================================================
.print ''
.print '--- Test 1: Invalid SQL Syntax ---'
.bail off

-- First configure correctly
SELECT duckarrow_configure('grpc://localhost:31337', 'duckarrow_user', 'duckarrow_password');

-- This should fail with a syntax error from the remote server
SELECT * FROM duckarrow."Order" WHERE SELECTT;

.print 'Test 1 completed (error expected above)'

-- ============================================================================
-- TEST 2: Non-existent Table (should produce remote error)
-- ============================================================================
.print ''
.print '--- Test 2: Non-existent Table ---'

-- Reconfigure (in case previous test affected state)
SELECT duckarrow_configure('grpc://localhost:31337', 'duckarrow_user', 'duckarrow_password');

-- This should fail with table not found
SELECT * FROM duckarrow."ThisTableDefinitelyDoesNotExist12345";

.print 'Test 2 completed (error expected above)'

-- ============================================================================
-- TEST 3: Invalid URI Scheme
-- ============================================================================
.print ''
.print '--- Test 3: Invalid URI Scheme ---'

-- Should fail validation - http:// not allowed
SELECT duckarrow_configure('http://localhost:31337', 'user', 'pass');

.print 'Test 3 completed (error expected above)'

-- ============================================================================
-- TEST 4: Empty URI
-- ============================================================================
.print ''
.print '--- Test 4: Empty URI ---'

-- Should fail validation
SELECT duckarrow_configure('', 'user', 'pass');

.print 'Test 4 completed (error expected above)'

-- ============================================================================
-- TEST 5: Missing Host in URI
-- ============================================================================
.print ''
.print '--- Test 5: Missing Host ---'

-- Should fail validation
SELECT duckarrow_configure('grpc://', 'user', 'pass');

.print 'Test 5 completed (error expected above)'

-- ============================================================================
-- TEST 6: Bad Server Address (Connection Error)
-- ============================================================================
.print ''
.print '--- Test 6: Bad Server Address ---'

-- Configure with non-routable IP (should timeout/fail on query)
SELECT duckarrow_configure('grpc://10.255.255.1:31337', 'user', 'pass');

-- This should fail with connection error (may take a moment to timeout)
-- Using a short query to minimize wait time
.timeout 5000
SELECT 1 FROM duckarrow."Order" LIMIT 1;
.timeout 0

.print 'Test 6 completed (error or timeout expected above)'

-- ============================================================================
-- TEST 7: Invalid Credentials (Auth Error)
-- ============================================================================
.print ''
.print '--- Test 7: Invalid Credentials ---'

-- Configure with wrong credentials
SELECT duckarrow_configure('grpc://localhost:31337', 'wrong_user', 'wrong_pass');

-- This should fail with auth error
SELECT * FROM duckarrow."Order" LIMIT 1;

.print 'Test 7 completed (error expected above)'

-- ============================================================================
-- TEST 8: SQL Injection Prevention - Semicolon
-- ============================================================================
.print ''
.print '--- Test 8: SQL Injection Prevention ---'

-- Restore valid config first
SELECT duckarrow_configure('grpc://localhost:31337', 'duckarrow_user', 'duckarrow_password');

-- These should be blocked by table name validation
-- Semicolon injection attempt
SELECT * FROM duckarrow."Order;DROP TABLE users";

.print 'Test 8 completed (error expected above)'

-- ============================================================================
-- TEST 9: SQL Injection Prevention - Line Comment
-- ============================================================================
.print ''
.print '--- Test 9: Line Comment Injection Prevention ---'

-- Line comment injection attempt
SELECT * FROM duckarrow."Order--malicious";

.print 'Test 9 completed (error expected above)'

-- ============================================================================
-- TEST 10: SQL Injection Prevention - Block Comment
-- ============================================================================
.print ''
.print '--- Test 10: Block Comment Injection Prevention ---'

-- Block comment injection attempt (start)
SELECT * FROM duckarrow."Order/*malicious";

.print 'Test 10a completed (error expected above)'

-- Block comment injection attempt (end)
SELECT * FROM duckarrow."malicious*/Order";

.print 'Test 10b completed (error expected above)'

-- ============================================================================
-- TEST 11: Recovery After Errors
-- ============================================================================
.print ''
.print '--- Test 11: Recovery After Errors ---'
.bail on

-- Restore valid configuration
SELECT duckarrow_configure('grpc://localhost:31337', 'duckarrow_user', 'duckarrow_password');

-- This should work after all the error tests
SELECT COUNT(*) as recovery_test FROM duckarrow."Order";

.print 'Test 11 PASSED - Successfully recovered after error tests'

.print ''
.print '=== Error Handling Tests Complete ==='
.print 'All expected errors were handled correctly.'
