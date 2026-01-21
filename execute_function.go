package main

/*
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>

// Forward declaration of Go callback
void duckarrow_execute_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
*/
import "C"
import (
	"context"
	"duckdb"
	"runtime"
	"unsafe"

	"main/internal/flight"
)

// duckarrow_execute_callback is the scalar function callback for duckarrow_execute(sql).
// It executes DDL/DML statements on the configured Flight SQL server.
//
// Parameters:
//   - info: Function execution context for error reporting
//   - input: Data chunk containing one parameter:
//   - sql (VARCHAR): SQL statement to execute (required)
//   - output: Output vector for the affected row count (BIGINT)
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
//
//export duckarrow_execute_callback
func duckarrow_execute_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Get input size
	inputSize := C.duckdb_data_chunk_get_size(input)
	if inputSize == 0 {
		return
	}

	// Bounds check: DuckDB chunks should never exceed maxDuckDBChunkSize
	if inputSize > maxDuckDBChunkSize {
		setExecuteError(info, "input chunk size exceeds maximum")
		return
	}

	// Get the input vector (sql)
	sqlVec := C.duckdb_data_chunk_get_vector(input, 0)
	if sqlVec == nil {
		setExecuteError(info, "failed to get input vector")
		return
	}

	// Get data pointer
	sqlDataPtr := C.duckdb_vector_get_data(sqlVec)
	if sqlDataPtr == nil {
		setExecuteError(info, "failed to get input data")
		return
	}

	// Get validity mask
	sqlValidity := C.duckdb_vector_get_validity(sqlVec)

	// Get output data pointer (BIGINT = int64)
	outputDataPtr := (*C.int64_t)(C.duckdb_vector_get_data(output))
	outputData := unsafe.Slice(outputDataPtr, inputSize)

	// Get config for connection
	uri, username, password, skipVerify := GetDuckArrowConfig()
	if uri == "" {
		setExecuteError(info, "not configured - call duckarrow_configure() first")
		return
	}

	// Build config for connection pool
	cfg := flight.Config{
		URI:        uri,
		Username:   username,
		Password:   password,
		SkipVerify: skipVerify,
	}

	// Get connection from pool
	ctx := context.Background()
	connResult, err := flight.GetConnection(ctx, cfg)
	if err != nil {
		setExecuteError(info, "connection failed: "+err.Error())
		return
	}
	// Defer connection release/close
	defer func() {
		if connResult.IsPooled {
			flight.ReleaseConnection(cfg)
		} else {
			connResult.Client.Close()
		}
	}()

	// Process each row (typically just one for scalar functions)
	for i := C.idx_t(0); i < inputSize; i++ {
		// Check for NULL input
		if sqlValidity != nil && !rowIsValid(sqlValidity, uint64(i), uint64(inputSize)) {
			// NULL SQL - return NULL
			C.duckdb_vector_ensure_validity_writable(output)
			outValidity := C.duckdb_vector_get_validity(output)
			if outValidity != nil {
				setRowInvalid(outValidity, uint64(i), uint64(inputSize))
			}
			continue
		}

		// Extract SQL string
		sql, err := extractString(sqlDataPtr, i)
		if err != nil {
			setExecuteError(info, "failed to read SQL: "+err.Error())
			return
		}

		// Validate SQL input
		if sql == "" {
			setExecuteError(info, "SQL statement cannot be empty")
			return
		}
		// Limit SQL length to prevent resource exhaustion (1MB max)
		const maxSQLLength = 1024 * 1024
		if len(sql) > maxSQLLength {
			setExecuteError(info, "SQL statement exceeds maximum length (1MB)")
			return
		}
		// Reject null bytes which could cause truncation issues
		for _, c := range sql {
			if c == 0 {
				setExecuteError(info, "SQL statement contains invalid null byte")
				return
			}
		}

		// Execute the statement on remote Flight SQL server
		affected, err := connResult.Client.Execute(ctx, sql)
		if err != nil {
			setExecuteError(info, "remote server: "+err.Error())
			return
		}

		// Return the affected row count
		outputData[i] = C.int64_t(affected)
	}
}

// setExecuteError is a helper to set an error on the execute function with consistent formatting.
func setExecuteError(info C.duckdb_function_info, msg string) {
	errMsg := C.CString("duckarrow_execute: " + msg)
	C.duckdb_scalar_function_set_error(info, errMsg)
	C.free(unsafe.Pointer(errMsg))
}

// RegisterDuckArrowExecuteFunction registers the duckarrow_execute(sql) scalar function.
// This function allows users to execute DDL/DML statements on the Flight SQL server.
//
// The function is intended for statements that don't return result sets:
//   - DDL: CREATE TABLE, DROP TABLE, ALTER TABLE, etc.
//   - DML: INSERT, UPDATE, DELETE
//
// For SELECT queries, use the duckarrow.* syntax or duckarrow_query() table function instead.
// Passing SELECT to duckarrow_execute() will likely fail or return unexpected results.
//
// Security notes:
//   - SQL is executed on the remote Flight SQL server with the configured credentials
//   - Maximum SQL length is 1MB to prevent resource exhaustion
//   - Null bytes are rejected to prevent truncation attacks
//
// Usage in SQL:
//
//	SELECT duckarrow_execute('DROP TABLE "my_table"');
//	SELECT duckarrow_execute('CREATE TABLE test (id INTEGER)');
//	SELECT duckarrow_execute('INSERT INTO test VALUES (1)');
//
// Parameters:
//   - conn: Active DuckDB connection for function registration
//
// Returns:
//   - duckdb.STATE_OK on success, duckdb.STATE_ERROR on failure
func RegisterDuckArrowExecuteFunction(conn duckdb.Connection) duckdb.State {
	// Create scalar function
	scalarFunc := C.duckdb_create_scalar_function()
	defer C.duckdb_destroy_scalar_function(&scalarFunc)

	// Set name
	name := C.CString("duckarrow_execute")
	defer C.free(unsafe.Pointer(name))
	C.duckdb_scalar_function_set_name(scalarFunc, name)

	// Add one VARCHAR parameter (sql)
	varcharType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
	C.duckdb_scalar_function_add_parameter(scalarFunc, varcharType)
	C.duckdb_destroy_logical_type(&varcharType)

	// Set BIGINT return type (affected row count)
	bigintType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT)
	C.duckdb_scalar_function_set_return_type(scalarFunc, bigintType)
	C.duckdb_destroy_logical_type(&bigintType)

	// Set the callback
	C.duckdb_scalar_function_set_function(scalarFunc,
		C.duckdb_scalar_function_t(C.duckarrow_execute_callback))

	// Register the function
	return duckdb.State(C.duckdb_register_scalar_function(
		C.duckdb_connection(conn.Ptr), scalarFunc))
}
