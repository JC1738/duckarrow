package main

/*
#cgo CFLAGS: -I${SRCDIR}/../duckdb-go-api -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>

// Forward declaration of Go callback
void duckarrow_replacement_scan_callback(duckdb_replacement_scan_info info, char *table_name, void *extra_data);
*/
import "C"
import (
	"duckdb"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"main/internal/validation"
)

// DuckArrowConfig holds the global configuration for duckarrow replacement scan.
//
// Thread Safety and TOCTOU Considerations:
// The configuration is protected by a RWMutex for concurrent access. However, there is an
// intentional TOCTOU (time-of-check-time-of-use) window: if SetDuckArrowConfig is called
// while a query is in progress, the in-flight query will complete with the old config,
// while new queries will use the new config. This is acceptable behavior because:
//   - Each query gets a consistent configuration for its entire execution
//   - Config changes are rare (typically once at session start)
//   - The alternative (locking for entire query duration) would cause deadlocks
//
// Users should call duckarrow_configure() before running queries, not during.
type DuckArrowConfig struct {
	mu         sync.RWMutex
	uri        string
	username   string
	password   string
	skipVerify bool
}

var duckArrowConfig = &DuckArrowConfig{}

// SetDuckArrowConfig sets the connection configuration for duckarrow.
// This is called by duckarrow_configure() and takes effect for subsequent queries.
// In-flight queries will complete with the previous configuration.
func SetDuckArrowConfig(uri, username, password string, skipVerify bool) {
	duckArrowConfig.mu.Lock()
	defer duckArrowConfig.mu.Unlock()
	duckArrowConfig.uri = uri
	duckArrowConfig.username = username
	duckArrowConfig.password = password
	duckArrowConfig.skipVerify = skipVerify
}

// GetDuckArrowConfig gets the current duckarrow configuration.
// Returns empty strings if not configured, which causes replacement scan to skip.
// skipVerify defaults to false (secure) if not explicitly set.
func GetDuckArrowConfig() (uri, username, password string, skipVerify bool) {
	duckArrowConfig.mu.RLock()
	defer duckArrowConfig.mu.RUnlock()
	return duckArrowConfig.uri, duckArrowConfig.username, duckArrowConfig.password, duckArrowConfig.skipVerify
}

// GetDuckArrowURI gets the current duckarrow URI (for backward compatibility).
// Returns empty string if not configured, which causes replacement scan to skip.
func GetDuckArrowURI() string {
	duckArrowConfig.mu.RLock()
	defer duckArrowConfig.mu.RUnlock()
	return duckArrowConfig.uri
}

// validateTableName delegates to the validation package for testability.
func validateTableName(name string) error {
	return validation.ValidateTableName(name)
}

// duckarrow_replacement_scan_callback is called by DuckDB when it encounters an unknown table
// in the "duckarrow" schema. It rewrites the query to use our duckarrow_query table function.
//
// Thread safety: This callback may be invoked from multiple DuckDB threads concurrently.
// The URI is read atomically via GetDuckArrowURI(). If the URI changes during query execution,
// subsequent queries will use the new URI.
//
//export duckarrow_replacement_scan_callback
func duckarrow_replacement_scan_callback(info C.duckdb_replacement_scan_info, tableName *C.char, extraData unsafe.Pointer) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	name := C.GoString(tableName)

	// Get the configured URI
	uri := GetDuckArrowURI()
	if uri == "" {
		// Not configured yet - don't handle this table
		return
	}

	// Strip exactly one pair of surrounding quotes if present (DuckDB may pass quoted identifiers)
	// Using strings.Trim would incorrectly strip ALL quotes from names like ""table"" -> table
	// Instead, we only strip one pair: ""table"" -> "table" (a table name containing quotes)
	actualTable := name
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		actualTable = name[1 : len(name)-1]
	}

	// Skip if it looks like a DuckDB internal or system table
	if validation.ShouldSkipTable(actualTable) {
		return
	}

	// Validate table name to prevent SQL injection on the remote server
	if err := validateTableName(actualTable); err != nil {
		errCStr := C.CString(fmt.Sprintf("duckarrow: %s", err.Error()))
		C.duckdb_replacement_scan_set_error(info, errCStr)
		C.free(unsafe.Pointer(errCStr))
		return
	}

	// Escape embedded double quotes to prevent SQL injection
	// In SQL, a literal double quote inside a quoted identifier is escaped by doubling it
	escapedTable := strings.ReplaceAll(actualTable, `"`, `""`)

	// Generate the query - quote the table name for safety
	query := fmt.Sprintf(`SELECT * FROM "%s"`, escapedTable)

	// Set the function name to our table function
	funcName := C.CString("duckarrow_query")
	defer C.free(unsafe.Pointer(funcName))
	C.duckdb_replacement_scan_set_function_name(info, funcName)

	// Add URI parameter
	uriCStr := C.CString(uri)
	uriValue := C.duckdb_create_varchar(uriCStr)
	C.free(unsafe.Pointer(uriCStr))
	C.duckdb_replacement_scan_add_parameter(info, uriValue)
	C.duckdb_destroy_value(&uriValue)

	// Add query parameter
	queryCStr := C.CString(query)
	queryValue := C.duckdb_create_varchar(queryCStr)
	C.free(unsafe.Pointer(queryCStr))
	C.duckdb_replacement_scan_add_parameter(info, queryValue)
	C.duckdb_destroy_value(&queryValue)
}

// RegisterReplacementScan registers the duckarrow replacement scan with the database
func RegisterReplacementScan(db duckdb.Database) {
	C.duckdb_add_replacement_scan(
		C.duckdb_database(db.Ptr),
		C.duckdb_replacement_callback_t(C.duckarrow_replacement_scan_callback),
		nil,
		nil,
	)
}
