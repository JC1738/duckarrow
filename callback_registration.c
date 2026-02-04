//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// callback_registration.c
//
//===----------------------------------------------------------------------===//
//
// This file provides a C shim to register Go callbacks with C++.
// CGO generates _cgo_export.h with declarations for //export functions.
// This file includes those declarations and calls the registration functions.
//
//===----------------------------------------------------------------------===//

#include <stdlib.h>
#include "go_callbacks.h"

// These are declared in the cgo-generated _cgo_export.h header.
// We forward-declare them here to avoid circular include issues during build.
extern DuckArrowConnectResult duckarrow_go_connect(const char* uri, const char* username, const char* password, const char* token);
extern DuckArrowSchemaList duckarrow_go_list_schemas(DuckArrowConnectionHandle connection, const char* catalog);
extern DuckArrowTableList duckarrow_go_list_tables(DuckArrowConnectionHandle connection, const char* catalog, const char* schema);
extern DuckArrowColumnList duckarrow_go_get_columns(DuckArrowConnectionHandle connection, const char* catalog, const char* schema, const char* table);

// Scan callbacks for GetScanFunction
extern DuckArrowScanBindResult duckarrow_go_scan_table_bind(const char* uri, const char* schema_name, const char* table_name);
extern DuckArrowScanColumn duckarrow_go_scan_get_column(DuckArrowScanHandle handle, size_t index);
extern const char* duckarrow_go_scan_init(DuckArrowScanHandle handle, const size_t* column_ids, size_t column_count);
extern int64_t duckarrow_go_scan_next(DuckArrowScanHandle handle, void* output);
extern void duckarrow_go_scan_free(DuckArrowScanHandle handle);

// duckarrow_register_go_callbacks registers all Go callbacks with the C++ storage extension.
// This must be called once during extension initialization, after the storage extension is registered.
void duckarrow_register_go_callbacks(void) {
    duckarrow_register_connect(duckarrow_go_connect);
    duckarrow_register_list_schemas(duckarrow_go_list_schemas);
    duckarrow_register_list_tables(duckarrow_go_list_tables);
    duckarrow_register_get_columns(duckarrow_go_get_columns);
    // Register free() as the memory cleanup function since Go uses C.CString (which uses malloc)
    duckarrow_register_free(free);

    // Register scan callbacks for GetScanFunction
    duckarrow_register_scan_table_bind(duckarrow_go_scan_table_bind);
    duckarrow_register_scan_get_column(duckarrow_go_scan_get_column);
    duckarrow_register_scan_init(duckarrow_go_scan_init);
    duckarrow_register_scan_next(duckarrow_go_scan_next);
    duckarrow_register_scan_free(duckarrow_go_scan_free);
}
