//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// go_callbacks.h
//
//===----------------------------------------------------------------------===//
//
// This file defines the C interface for Go callbacks used by the DuckArrow
// storage extension. These callbacks allow the C++ storage extension to query
// schema, table, and column metadata from Go's Flight SQL client.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

//===--------------------------------------------------------------------===//
// Export macros for cross-platform symbol visibility
//===--------------------------------------------------------------------===//

#ifdef _WIN32
#ifdef DUCKARROW_STORAGE_EXPORTS
#define DUCKARROW_API __declspec(dllexport)
#else
#define DUCKARROW_API __declspec(dllimport)
#endif
#else
// On Unix, use visibility attribute for exported symbols
#define DUCKARROW_API __attribute__((visibility("default")))
#endif

//===--------------------------------------------------------------------===//
// Extern "C" guards for C++ inclusion
//===--------------------------------------------------------------------===//

#ifdef __cplusplus
extern "C" {
#endif

//===--------------------------------------------------------------------===//
// Schema structures
//===--------------------------------------------------------------------===//

// Information about a single schema (catalog/database)
typedef struct {
    const char* schema_name;  // Schema name (owned by this struct)
} DuckArrowSchemaInfo;

// List of schemas returned from Flight SQL
typedef struct {
    DuckArrowSchemaInfo* schemas;  // Array of schema info structs
    size_t count;                  // Number of schemas
    const char* error;             // Error message if any (NULL on success)
} DuckArrowSchemaList;

//===--------------------------------------------------------------------===//
// Table structures
//===--------------------------------------------------------------------===//

// Information about a single table
typedef struct {
    const char* table_name;   // Table name (owned by this struct)
    const char* table_type;   // Table type: "TABLE", "VIEW", etc. (owned by this struct)
} DuckArrowTableInfo;

// List of tables returned from Flight SQL
typedef struct {
    DuckArrowTableInfo* tables;  // Array of table info structs
    size_t count;                // Number of tables
    const char* error;           // Error message if any (NULL on success)
} DuckArrowTableList;

//===--------------------------------------------------------------------===//
// Column structures
//===--------------------------------------------------------------------===//

// Information about a single column
typedef struct {
    const char* column_name;      // Column name (owned by this struct)
    const char* column_type;      // Column type as string (owned by this struct)
    int32_t ordinal_position;     // 1-based column position
    bool is_nullable;             // Whether column allows NULL
} DuckArrowColumnInfo;

// List of columns returned from Flight SQL
typedef struct {
    DuckArrowColumnInfo* columns;  // Array of column info structs
    size_t count;                  // Number of columns
    const char* error;             // Error message if any (NULL on success)
} DuckArrowColumnList;

//===--------------------------------------------------------------------===//
// Connection handle
//===--------------------------------------------------------------------===//

// Opaque handle to a DuckArrow connection (managed by Go)
typedef void* DuckArrowConnectionHandle;

//===--------------------------------------------------------------------===//
// Connection result structure
//===--------------------------------------------------------------------===//

// Result from duckarrow_go_connect callback
typedef struct {
    DuckArrowConnectionHandle handle;  // Connection handle on success (NULL on error)
    const char* error;                 // Error message if any (NULL on success)
} DuckArrowConnectResult;

//===--------------------------------------------------------------------===//
// Go callback function signatures
//===--------------------------------------------------------------------===//

// Connect to a Flight SQL server
// uri: the connection URI (e.g., grpc://host:port or grpc+tls://host:port)
// username: optional username for authentication (can be NULL)
// password: optional password for authentication (can be NULL)
// token: optional token for authentication (can be NULL)
// Returns: DuckArrowConnectResult with connection handle or error
typedef DuckArrowConnectResult (*duckarrow_connect_fn)(
    const char* uri,
    const char* username,
    const char* password,
    const char* token
);

// Get list of schemas from Flight SQL server
// connection: opaque handle to DuckArrow connection
// catalog: catalog name to filter by (can be NULL for all)
// Returns: DuckArrowSchemaList with schema names or error
typedef DuckArrowSchemaList (*duckarrow_list_schemas_fn)(
    DuckArrowConnectionHandle connection,
    const char* catalog
);

// Get list of tables from Flight SQL server
// connection: opaque handle to DuckArrow connection
// catalog: catalog name to filter by (can be NULL for all)
// schema: schema name to filter by (can be NULL for all)
// Returns: DuckArrowTableList with table names or error
typedef DuckArrowTableList (*duckarrow_list_tables_fn)(
    DuckArrowConnectionHandle connection,
    const char* catalog,
    const char* schema
);

// Get column information for a table from Flight SQL server
// connection: opaque handle to DuckArrow connection
// catalog: catalog name (can be NULL)
// schema: schema name (can be NULL)
// table: table name (required, must not be NULL)
// Returns: DuckArrowColumnList with column metadata or error
typedef DuckArrowColumnList (*duckarrow_get_columns_fn)(
    DuckArrowConnectionHandle connection,
    const char* catalog,
    const char* schema,
    const char* table
);

//===--------------------------------------------------------------------===//
// Go callback registration functions (called by Go to register callbacks)
//===--------------------------------------------------------------------===//

// Register the connect callback
DUCKARROW_API void duckarrow_register_connect(duckarrow_connect_fn callback);

// Register the list_schemas callback
DUCKARROW_API void duckarrow_register_list_schemas(duckarrow_list_schemas_fn callback);

// Register the list_tables callback
DUCKARROW_API void duckarrow_register_list_tables(duckarrow_list_tables_fn callback);

// Register the get_columns callback
DUCKARROW_API void duckarrow_register_get_columns(duckarrow_get_columns_fn callback);

//===--------------------------------------------------------------------===//
// Memory cleanup functions (called by C++ to free Go-allocated memory)
//===--------------------------------------------------------------------===//

// Free function pointer type for Go-allocated memory
typedef void (*duckarrow_free_fn)(void* ptr);

// Register the memory free function (called by Go)
DUCKARROW_API void duckarrow_register_free(duckarrow_free_fn callback);

// Free a DuckArrowSchemaList and all its contents
// Must be called after consuming the schema list
DUCKARROW_API void duckarrow_free_schema_list(DuckArrowSchemaList* list);

// Free a DuckArrowTableList and all its contents
// Must be called after consuming the table list
DUCKARROW_API void duckarrow_free_table_list(DuckArrowTableList* list);

// Free a DuckArrowColumnList and all its contents
// Must be called after consuming the column list
DUCKARROW_API void duckarrow_free_column_list(DuckArrowColumnList* list);

//===--------------------------------------------------------------------===//
// Scan Function Callbacks for GetScanFunction
//===--------------------------------------------------------------------===//
// These callbacks allow C++ to execute table scans by calling back into Go.
// Go handles the actual Flight SQL query execution via the existing
// duckarrow_query infrastructure.

// Opaque handle to scan state (managed by Go)
typedef void* DuckArrowScanHandle;

// Result from scan bind
typedef struct {
    DuckArrowScanHandle handle;  // Scan handle on success
    const char* error;           // Error message on failure (NULL on success)
    size_t column_count;         // Number of columns
} DuckArrowScanBindResult;

// Column information returned during bind
typedef struct {
    const char* name;            // Column name
    const char* type_name;       // Column type string (e.g., "VARCHAR", "BIGINT")
} DuckArrowScanColumn;

// Bind a table scan
// uri: Flight SQL server URI
// schema_name: Schema name (can be NULL)
// table_name: Table name (required)
// Returns: Bind result with scan handle and column info
typedef DuckArrowScanBindResult (*duckarrow_scan_table_bind_fn)(
    const char* uri,
    const char* schema_name,
    const char* table_name
);

// Get column information for a bound scan
// handle: Scan handle from bind
// index: Column index (0-based)
// Returns: Column information (name and type)
typedef DuckArrowScanColumn (*duckarrow_scan_get_column_fn)(
    DuckArrowScanHandle handle,
    size_t index
);

// Initialize scan (called before scanning starts)
// handle: Scan handle from bind
// column_ids: Array of column indices to fetch (for projection pushdown)
// column_count: Number of columns to fetch
// Returns: Error message on failure, NULL on success
typedef const char* (*duckarrow_scan_init_fn)(
    DuckArrowScanHandle handle,
    const size_t* column_ids,
    size_t column_count
);

// Scan next chunk of data
// handle: Scan handle
// output: Pointer to duckdb_data_chunk to fill
// Returns: Number of rows returned (0 means done), negative on error
typedef int64_t (*duckarrow_scan_next_fn)(
    DuckArrowScanHandle handle,
    void* output  // duckdb_data_chunk
);

// Free scan handle and associated resources
typedef void (*duckarrow_scan_free_fn)(DuckArrowScanHandle handle);

// Register scan callbacks
DUCKARROW_API void duckarrow_register_scan_table_bind(duckarrow_scan_table_bind_fn callback);
DUCKARROW_API void duckarrow_register_scan_get_column(duckarrow_scan_get_column_fn callback);
DUCKARROW_API void duckarrow_register_scan_init(duckarrow_scan_init_fn callback);
DUCKARROW_API void duckarrow_register_scan_next(duckarrow_scan_next_fn callback);
DUCKARROW_API void duckarrow_register_scan_free(duckarrow_scan_free_fn callback);

//===--------------------------------------------------------------------===//
// Storage Extension Registration
//===--------------------------------------------------------------------===//

// Register the DuckArrow storage extension with a DuckDB database.
// This enables the ATTACH ... (TYPE duckarrow) syntax for attaching
// Flight SQL servers as external databases.
//
// Parameters:
//   db_handle: The DuckDB database handle (duckdb_database from C API)
//
// Returns:
//   true if registration succeeded, false otherwise
//
// Usage from Go:
//   db := api.Database()
//   C.duckarrow_register_storage_extension(unsafe.Pointer(db))
//
// Thread safety: Call once during extension initialization.
DUCKARROW_API bool duckarrow_register_storage_extension(void* db_handle);

#ifdef __cplusplus
} // extern "C"
#endif
