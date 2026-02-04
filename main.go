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
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -I${SRCDIR}/cpp -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>
#include <go_callbacks.h>

// Defined in callback_registration.c - registers Go callbacks with C++
void duckarrow_register_go_callbacks(void);
*/
import "C"
import (
	"context"
	"duckdb"
	"fmt"
	"runtime"
	"runtime/cgo"
	"strings"
	"unsafe"

	"main/internal/flight"
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

	// Register the storage extension for ATTACH ... (TYPE duckarrow) syntax
	if !C.duckarrow_register_storage_extension(db.Ptr) {
		fmt.Println("[duckarrow] Failed to register storage extension")
		return false
	}

	// Register Go callbacks with C++ for schema/table/column metadata queries
	// These are called by the DuckArrow catalog when querying Flight SQL servers
	C.duckarrow_register_go_callbacks()

	fmt.Printf("[duckarrow] Extension %s loaded successfully\n", Version)
	return true
}

// ConnectionHandle wraps a Flight SQL client for use as an opaque handle.
// The handle is passed to C++ and back via DuckArrowConnectionHandle (void*).
type ConnectionHandle struct {
	Client   *flight.Client
	Config   flight.Config
	IsPooled bool
}

// duckarrow_go_connect is called by C++ during ATTACH to establish a Flight SQL connection.
// It creates a new connection to the Flight SQL server and returns an opaque handle.
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
// Memory: The returned error string is allocated with C.CString and must be freed by C++.
//
//export duckarrow_go_connect
func duckarrow_go_connect(uri *C.char, username *C.char, password *C.char, token *C.char) C.DuckArrowConnectResult {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowConnectResult
	result.handle = nil
	result.error = nil

	// Validate URI
	if uri == nil {
		result.error = C.CString("duckarrow_go_connect: URI is required")
		return result
	}

	// Build connection config
	cfg := flight.Config{
		URI: C.GoString(uri),
	}
	if username != nil {
		cfg.Username = C.GoString(username)
	}
	if password != nil {
		cfg.Password = C.GoString(password)
	}
	// Note: token support could be added to flight.Config if needed

	// Establish connection to Flight SQL server
	ctx := context.Background()
	client, err := flight.Connect(ctx, cfg)
	if err != nil {
		errMsg := fmt.Sprintf("duckarrow_go_connect: %v", err)
		result.error = C.CString(errMsg)
		return result
	}

	// Create connection handle and convert to opaque pointer
	connHandle := &ConnectionHandle{
		Client:   client,
		Config:   cfg,
		IsPooled: false,
	}

	// Use cgo.Handle to create a stable pointer that survives GC
	handle := cgo.NewHandle(connHandle)
	result.handle = C.DuckArrowConnectionHandle(uintptr(handle))

	return result
}

// duckarrow_go_list_schemas is called by C++ to list schemas from a Flight SQL server.
// It accepts an opaque connection handle and returns a DuckArrowSchemaList.
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
// Memory: All returned strings are allocated with C.malloc so C++ can free them.
//
//export duckarrow_go_list_schemas
func duckarrow_go_list_schemas(connection C.DuckArrowConnectionHandle, catalog *C.char) C.DuckArrowSchemaList {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowSchemaList
	result.schemas = nil
	result.count = 0
	result.error = nil

	// Validate connection handle
	if connection == nil {
		result.error = C.CString("duckarrow_go_list_schemas: nil connection handle")
		return result
	}

	// Recover the ConnectionHandle from the opaque pointer
	handle := cgo.Handle(uintptr(connection))
	connHandle, ok := handle.Value().(*ConnectionHandle)
	if !ok || connHandle == nil || connHandle.Client == nil {
		result.error = C.CString("duckarrow_go_list_schemas: invalid connection handle")
		return result
	}

	// Get schemas from Flight SQL server
	ctx := context.Background()
	schemas, err := connHandle.Client.GetSchemas(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("duckarrow_go_list_schemas: %v", err)
		result.error = C.CString(errMsg)
		return result
	}

	// Handle empty result
	if len(schemas) == 0 {
		return result
	}

	// Allocate array of DuckArrowSchemaInfo using C.malloc
	schemaInfoSize := C.size_t(unsafe.Sizeof(C.DuckArrowSchemaInfo{}))
	arraySize := schemaInfoSize * C.size_t(len(schemas))
	schemasPtr := (*C.DuckArrowSchemaInfo)(C.malloc(arraySize))
	if schemasPtr == nil {
		result.error = C.CString("duckarrow_go_list_schemas: failed to allocate schema array")
		return result
	}

	// Zero the allocated memory
	C.memset(unsafe.Pointer(schemasPtr), 0, arraySize)

	// Convert Go slice to C array
	schemasSlice := unsafe.Slice(schemasPtr, len(schemas))
	for i, schemaName := range schemas {
		// Allocate and copy schema name using C.CString (uses C.malloc internally)
		schemasSlice[i].schema_name = C.CString(schemaName)
	}

	result.schemas = schemasPtr
	result.count = C.size_t(len(schemas))
	return result
}

// duckarrow_go_list_tables is called by C++ to list tables from a Flight SQL server.
// It accepts an opaque connection handle, catalog (unused), and schema filter.
// Returns a DuckArrowTableList with table names and types.
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
// Memory: All returned strings are allocated with C.malloc so C++ can free them.
//
//export duckarrow_go_list_tables
func duckarrow_go_list_tables(connection C.DuckArrowConnectionHandle, catalog *C.char, schema *C.char) C.DuckArrowTableList {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowTableList
	result.tables = nil
	result.count = 0
	result.error = nil

	// Validate connection handle
	if connection == nil {
		result.error = C.CString("duckarrow_go_list_tables: nil connection handle")
		return result
	}

	// Recover the ConnectionHandle from the opaque pointer
	handle := cgo.Handle(uintptr(connection))
	connHandle, ok := handle.Value().(*ConnectionHandle)
	if !ok || connHandle == nil || connHandle.Client == nil {
		result.error = C.CString("duckarrow_go_list_tables: invalid connection handle")
		return result
	}

	// Convert schema parameter (catalog is currently unused by Flight SQL GetTables)
	var schemaName string
	if schema != nil {
		schemaName = C.GoString(schema)
	}

	// Get tables from Flight SQL server
	ctx := context.Background()
	tables, err := connHandle.Client.GetTables(ctx, schemaName)
	if err != nil {
		errMsg := fmt.Sprintf("duckarrow_go_list_tables: %v", err)
		result.error = C.CString(errMsg)
		return result
	}

	// Handle empty result
	if len(tables) == 0 {
		return result
	}

	// Allocate array of DuckArrowTableInfo using C.malloc
	tableInfoSize := C.size_t(unsafe.Sizeof(C.DuckArrowTableInfo{}))
	arraySize := tableInfoSize * C.size_t(len(tables))
	tablesPtr := (*C.DuckArrowTableInfo)(C.malloc(arraySize))
	if tablesPtr == nil {
		result.error = C.CString("duckarrow_go_list_tables: failed to allocate table array")
		return result
	}

	// Zero the allocated memory
	C.memset(unsafe.Pointer(tablesPtr), 0, arraySize)

	// Convert Go slice to C array
	tablesSlice := unsafe.Slice(tablesPtr, len(tables))
	for i, table := range tables {
		// Allocate and copy table name using C.CString (uses C.malloc internally)
		tablesSlice[i].table_name = C.CString(table.Name)
		// Set table_type to "TABLE" as default since TableInfo doesn't track type
		tablesSlice[i].table_type = C.CString("TABLE")
	}

	result.tables = tablesPtr
	result.count = C.size_t(len(tables))
	return result
}

// duckarrow_go_get_columns is called by C++ to get column metadata for a table from Flight SQL.
// It accepts an opaque connection handle, catalog (unused), schema, and table name.
// Returns a DuckArrowColumnList with column names, types, positions, and nullability.
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
// Memory: All returned strings are allocated with C.malloc so C++ can free them.
//
//export duckarrow_go_get_columns
func duckarrow_go_get_columns(connection C.DuckArrowConnectionHandle, catalog *C.char, schema *C.char, table *C.char) C.DuckArrowColumnList {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowColumnList
	result.columns = nil
	result.count = 0
	result.error = nil

	// Validate connection handle
	if connection == nil {
		result.error = C.CString("duckarrow_go_get_columns: nil connection handle")
		return result
	}

	// Validate table parameter (required)
	if table == nil {
		result.error = C.CString("duckarrow_go_get_columns: table name is required")
		return result
	}

	// Recover the ConnectionHandle from the opaque pointer
	handle := cgo.Handle(uintptr(connection))
	connHandle, ok := handle.Value().(*ConnectionHandle)
	if !ok || connHandle == nil || connHandle.Client == nil {
		result.error = C.CString("duckarrow_go_get_columns: invalid connection handle")
		return result
	}

	// Convert C strings to Go strings
	tableStr := C.GoString(table)
	var schemaStr string
	if schema != nil {
		schemaStr = C.GoString(schema)
	}
	// Note: catalog is currently unused as Flight SQL column queries typically don't use catalog

	// Get columns from Flight SQL server
	ctx := context.Background()
	columns, err := connHandle.Client.GetColumns(ctx, schemaStr, tableStr)
	if err != nil {
		errMsg := fmt.Sprintf("duckarrow_go_get_columns: %v", err)
		result.error = C.CString(errMsg)
		return result
	}

	// Handle empty result (table exists but has no columns)
	if len(columns) == 0 {
		return result
	}

	// Allocate array of DuckArrowColumnInfo using C.malloc
	columnInfoSize := C.size_t(unsafe.Sizeof(C.DuckArrowColumnInfo{}))
	arraySize := columnInfoSize * C.size_t(len(columns))
	columnsPtr := (*C.DuckArrowColumnInfo)(C.malloc(arraySize))
	if columnsPtr == nil {
		result.error = C.CString("duckarrow_go_get_columns: failed to allocate column array")
		return result
	}

	// Zero the allocated memory
	C.memset(unsafe.Pointer(columnsPtr), 0, arraySize)

	// Convert Go slice to C array
	columnsSlice := unsafe.Slice(columnsPtr, len(columns))
	for i, col := range columns {
		// Allocate and copy column name using C.CString (uses C.malloc internally)
		columnsSlice[i].column_name = C.CString(col.Name)
		// Allocate and copy type name
		columnsSlice[i].column_type = C.CString(col.TypeName)
		// Set ordinal position (1-based)
		columnsSlice[i].ordinal_position = C.int32_t(col.OrdinalPosition)
		// Set nullability
		columnsSlice[i].is_nullable = C.bool(col.Nullable)
	}

	result.columns = columnsPtr
	result.count = C.size_t(len(columns))
	return result
}

// duckarrow_go_free_schema_list frees memory allocated by duckarrow_go_list_schemas.
// Called by C++ after consuming the schema list.
//
// Memory: Frees all C.CString allocated strings and the schemas array.
//
//export duckarrow_go_free_schema_list
func duckarrow_go_free_schema_list(list *C.DuckArrowSchemaList) {
	if list == nil {
		return
	}

	// Free error string if present
	if list.error != nil {
		C.free(unsafe.Pointer(list.error))
		list.error = nil
	}

	// Free each schema_name string, then the array itself
	if list.schemas != nil && list.count > 0 {
		schemasSlice := unsafe.Slice(list.schemas, list.count)
		for i := range schemasSlice {
			if schemasSlice[i].schema_name != nil {
				C.free(unsafe.Pointer(schemasSlice[i].schema_name))
			}
		}
		C.free(unsafe.Pointer(list.schemas))
		list.schemas = nil
		list.count = 0
	}
}

// duckarrow_go_free_table_list frees memory allocated by duckarrow_go_list_tables.
// Called by C++ after consuming the table list.
//
// Memory: Frees all C.CString allocated strings and the tables array.
//
//export duckarrow_go_free_table_list
func duckarrow_go_free_table_list(list *C.DuckArrowTableList) {
	if list == nil {
		return
	}

	// Free error string if present
	if list.error != nil {
		C.free(unsafe.Pointer(list.error))
		list.error = nil
	}

	// Free each table_name and table_type string, then the array itself
	if list.tables != nil && list.count > 0 {
		tablesSlice := unsafe.Slice(list.tables, list.count)
		for i := range tablesSlice {
			if tablesSlice[i].table_name != nil {
				C.free(unsafe.Pointer(tablesSlice[i].table_name))
			}
			if tablesSlice[i].table_type != nil {
				C.free(unsafe.Pointer(tablesSlice[i].table_type))
			}
		}
		C.free(unsafe.Pointer(list.tables))
		list.tables = nil
		list.count = 0
	}
}

// duckarrow_go_free_column_list frees memory allocated by duckarrow_go_get_columns.
// Called by C++ after consuming the column list.
//
// Memory: Frees all C.CString allocated strings and the columns array.
//
//export duckarrow_go_free_column_list
func duckarrow_go_free_column_list(list *C.DuckArrowColumnList) {
	if list == nil {
		return
	}

	// Free error string if present
	if list.error != nil {
		C.free(unsafe.Pointer(list.error))
		list.error = nil
	}

	// Free each column_name and column_type string, then the array itself
	if list.columns != nil && list.count > 0 {
		columnsSlice := unsafe.Slice(list.columns, list.count)
		for i := range columnsSlice {
			if columnsSlice[i].column_name != nil {
				C.free(unsafe.Pointer(columnsSlice[i].column_name))
			}
			if columnsSlice[i].column_type != nil {
				C.free(unsafe.Pointer(columnsSlice[i].column_type))
			}
		}
		C.free(unsafe.Pointer(list.columns))
		list.columns = nil
		list.count = 0
	}
}

func main() {
	// Required for CGO compiler to compile as C shared library
}
