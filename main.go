// Package main implements a DuckDB extension for querying Flight SQL servers.
//
// CGO Callback Pattern:
// This extension uses Go's //export directive to create C-callable functions.
// When you mark a Go function with //export, cgo automatically generates C stubs
// in _cgo_export.h that DuckDB can call. These stubs handle the Go runtime setup
// (stack, goroutine context) before invoking the Go function. This is why we don't
// need separate C wrapper files - cgo generates them during the build process.
//
// Key exports in this extension:
//   - duckarrow_init_c_api: Extension initialization entry point
//   - duckarrow_bind_wrapper: Table function bind phase
//   - duckarrow_init_wrapper: Table function init phase
//   - duckarrow_scan_wrapper: Table function scan phase (returns data)
//   - duckarrow_configure_callback: Scalar function for configuration
//   - duckarrow_version_callback: Scalar function returning extension version
//   - duckarrow_replacement_scan_callback: Rewrites duckarrow.* table references
package main

/*
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>
*/
import "C"
import (
	"duckdb"
	"fmt"
	"strings"
	"unsafe"
)

//export duckarrow_init_c_api
func duckarrow_init_c_api(info unsafe.Pointer, access unsafe.Pointer) bool {
	api, err := duckdb.Init("v1.2.0", info, access)
	if err != nil {
		return false
	}

	// Get database and open connection for registration
	db := api.Database()
	var conn duckdb.Connection
	if state := duckdb.Connect(db, &conn); state == duckdb.STATE_ERROR {
		return false
	}
	defer duckdb.Disconnect(&conn)

	// Create the "duckarrow" schema for replacement scan to work
	// This schema acts as a namespace for tables that will be resolved via replacement scan
	createSchemaQuery := C.CString("CREATE SCHEMA IF NOT EXISTS duckarrow")
	defer C.free(unsafe.Pointer(createSchemaQuery))
	var result C.duckdb_result
	if state := C.duckdb_query(C.duckdb_connection(conn.Ptr), createSchemaQuery, &result); state == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_result_error(&result))
		C.duckdb_destroy_result(&result)
		// Only continue if error indicates schema already exists (shouldn't happen with IF NOT EXISTS)
		// For any other error, fail initialization
		if !strings.Contains(strings.ToLower(errMsg), "already exists") {
			fmt.Printf("[duckarrow] Failed to create duckarrow schema: %s\n", errMsg)
			return false
		}
	} else {
		C.duckdb_destroy_result(&result)
	}

	// Register table function
	if state := RegisterDuckArrowQuery(conn); state == duckdb.STATE_ERROR {
		fmt.Println("[duckarrow] Failed to register table function")
		return false
	}

	// Register duckarrow_configure scalar function
	if state := RegisterDuckArrowConfigureFunction(conn); state == duckdb.STATE_ERROR {
		fmt.Println("[duckarrow] Failed to register duckarrow_configure function")
		return false
	}

	// Register duckarrow_version scalar function
	if state := RegisterDuckArrowVersionFunction(conn); state == duckdb.STATE_ERROR {
		fmt.Println("[duckarrow] Failed to register duckarrow_version function")
		return false
	}

	// Register duckarrow_execute scalar function
	if state := RegisterDuckArrowExecuteFunction(conn); state == duckdb.STATE_ERROR {
		fmt.Println("[duckarrow] Failed to register duckarrow_execute function")
		return false
	}

	// Register replacement scan for duckarrow.* tables
	RegisterReplacementScan(db)

	fmt.Printf("[duckarrow] Extension %s loaded successfully\n", Version)
	return true
}

func main() {
	// Required for CGO compiler to compile as C shared library
}
