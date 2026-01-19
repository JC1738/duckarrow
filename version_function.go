package main

/*
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>

// Forward declaration of Go callback
void duckarrow_version_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
*/
import "C"
import (
	"duckdb"
	"runtime"
	"unsafe"
)

// Version is set at build time via -ldflags
var Version = "dev"

// duckarrow_version_callback is the scalar function callback for duckarrow_version().
// It returns the extension version string.
//
//export duckarrow_version_callback
func duckarrow_version_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	_ = info // unused but required by callback signature
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Get input size (typically 1 for scalar functions with no args)
	inputSize := int(C.duckdb_data_chunk_get_size(input))
	if inputSize == 0 {
		return
	}

	// Return the version string for each row
	versionCStr := C.CString(Version)
	defer C.free(unsafe.Pointer(versionCStr))

	for i := range inputSize {
		C.duckdb_vector_assign_string_element(output, C.idx_t(i), versionCStr)
	}
}

// RegisterDuckArrowVersionFunction registers the duckarrow_version() scalar function.
//
// Usage in SQL:
//
//	SELECT duckarrow_version();
//
// Returns:
//   - duckdb.STATE_OK on success, duckdb.STATE_ERROR on failure
func RegisterDuckArrowVersionFunction(conn duckdb.Connection) duckdb.State {
	// Create scalar function
	scalarFunc := C.duckdb_create_scalar_function()
	defer C.duckdb_destroy_scalar_function(&scalarFunc)

	// Set name
	name := C.CString("duckarrow_version")
	defer C.free(unsafe.Pointer(name))
	C.duckdb_scalar_function_set_name(scalarFunc, name)

	// No parameters needed

	// Set VARCHAR return type
	varcharRetType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
	C.duckdb_scalar_function_set_return_type(scalarFunc, varcharRetType)
	C.duckdb_destroy_logical_type(&varcharRetType)

	// Set the callback
	C.duckdb_scalar_function_set_function(scalarFunc,
		C.duckdb_scalar_function_t(C.duckarrow_version_callback))

	// Register the function
	return duckdb.State(C.duckdb_register_scalar_function(
		C.duckdb_connection(conn.Ptr), scalarFunc))
}
