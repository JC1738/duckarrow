package main

/*
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>

// Forward declaration of Go callback
void duckarrow_configure_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
*/
import "C"
import (
	"duckdb"
	"fmt"
	"runtime"
	"unsafe"

	"main/internal/validation"
)

const (
	// duckdbStringTSize is the size of duckdb_string_t in bytes.
	// This is a 16-byte structure containing either:
	// - Inlined: 4-byte length + 12 bytes of inline character data, OR
	// - Pointer: 4-byte length + 4-byte prefix + 8-byte pointer
	// Verified against DuckDB v1.2.0 source code and runtime sizeof() check.
	duckdbStringTSize = 16

	// maxDuckDBChunkSize is the maximum number of rows in a DuckDB data chunk.
	// DuckDB uses a default vector size of 2048 rows.
	maxDuckDBChunkSize = 2048
)

// duckarrow_configure_callback is the scalar function callback for duckarrow_configure(uri, username, password, [skip_verify]).
// It validates and stores the connection configuration for subsequent duckarrow.* table queries.
//
// Parameters:
//   - info: Function execution context for error reporting
//   - input: Data chunk containing three or four parameters:
//     - uri (VARCHAR): gRPC URI (required)
//     - username (VARCHAR): Authentication username (optional, can be empty)
//     - password (VARCHAR): Authentication password (optional, can be empty)
//     - skip_verify (BOOLEAN): Skip TLS certificate verification (optional, defaults to false)
//   - output: Output vector for the result message
//
// Thread safety: Uses runtime.LockOSThread() as required for CGO callbacks.
// The configuration is stored atomically via SetDuckArrowConfig().
//
//export duckarrow_configure_callback
func duckarrow_configure_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Get input size
	inputSize := C.duckdb_data_chunk_get_size(input)
	if inputSize == 0 {
		return
	}

	// Bounds check: DuckDB chunks should never exceed maxDuckDBChunkSize
	if inputSize > maxDuckDBChunkSize {
		setScalarError(info, "input chunk size exceeds maximum")
		return
	}

	// Get the input vectors (uri, username, password, and optional skip_verify)
	uriVec := C.duckdb_data_chunk_get_vector(input, 0)
	usernameVec := C.duckdb_data_chunk_get_vector(input, 1)
	passwordVec := C.duckdb_data_chunk_get_vector(input, 2)

	if uriVec == nil || usernameVec == nil || passwordVec == nil {
		setScalarError(info, "failed to get input vectors")
		return
	}

	// Get optional skip_verify vector (4th parameter)
	columnCount := C.duckdb_data_chunk_get_column_count(input)
	var skipVerifyVec C.duckdb_vector
	if columnCount >= 4 {
		skipVerifyVec = C.duckdb_data_chunk_get_vector(input, 3)
	}

	// Get data pointers for each vector
	uriDataPtr := C.duckdb_vector_get_data(uriVec)
	usernameDataPtr := C.duckdb_vector_get_data(usernameVec)
	passwordDataPtr := C.duckdb_vector_get_data(passwordVec)

	if uriDataPtr == nil || usernameDataPtr == nil || passwordDataPtr == nil {
		setScalarError(info, "failed to get input data")
		return
	}

	// Get validity masks
	uriValidity := C.duckdb_vector_get_validity(uriVec)
	usernameValidity := C.duckdb_vector_get_validity(usernameVec)
	passwordValidity := C.duckdb_vector_get_validity(passwordVec)

	// Process each row (typically just one for scalar functions)
	for i := C.idx_t(0); i < inputSize; i++ {
		// Check for NULL inputs - URI is required, but username/password can be empty strings
		if uriValidity != nil && !rowIsValid(uriValidity, uint64(i), uint64(inputSize)) {
			// NULL URI - return NULL
			C.duckdb_vector_ensure_validity_writable(output)
			outValidity := C.duckdb_vector_get_validity(output)
			if outValidity != nil {
				setRowInvalid(outValidity, uint64(i), uint64(inputSize))
			}
			continue
		}

		// Extract URI (required)
		uri, err := extractString(uriDataPtr, i)
		if err != nil {
			setScalarError(info, "failed to read URI: "+err.Error())
			return
		}

		// Validate URI
		if err := validateURI(uri); err != nil {
			setScalarError(info, err.Error())
			return
		}

		// Extract username (use empty string if NULL)
		var username string
		if usernameValidity == nil || rowIsValid(usernameValidity, uint64(i), uint64(inputSize)) {
			username, _ = extractString(usernameDataPtr, i)
		}

		// Extract password (use empty string if NULL)
		var password string
		if passwordValidity == nil || rowIsValid(passwordValidity, uint64(i), uint64(inputSize)) {
			password, _ = extractString(passwordDataPtr, i)
		}

		// Extract skip_verify (default to false for security)
		skipVerify := false
		if skipVerifyVec != nil {
			skipVerifyDataPtr := C.duckdb_vector_get_data(skipVerifyVec)
			skipVerifyValidity := C.duckdb_vector_get_validity(skipVerifyVec)
			if skipVerifyDataPtr != nil && (skipVerifyValidity == nil || rowIsValid(skipVerifyValidity, uint64(i), uint64(inputSize))) {
				// Boolean is stored as uint8 (0 = false, non-zero = true)
				boolPtr := (*C.uint8_t)(unsafe.Pointer(uintptr(skipVerifyDataPtr) + uintptr(i)))
				skipVerify = *boolPtr != 0
			}
		}

		// Set the global configuration
		SetDuckArrowConfig(uri, username, password, skipVerify)

		// Return a confirmation message
		duckdb.AssignStringToVector(duckdb.Vector{Ptr: unsafe.Pointer(output)}, int(i), "DuckArrow configured successfully")
	}
}

// extractString extracts a Go string from a duckdb_string_t at the given row index.
func extractString(dataPtr unsafe.Pointer, rowIdx C.idx_t) (string, error) {
	strPtr := (*C.duckdb_string_t)(unsafe.Pointer(uintptr(dataPtr) + uintptr(rowIdx)*duckdbStringTSize))
	strLen := C.duckdb_string_t_length(*strPtr)

	if strLen == 0 {
		return "", nil
	}

	strData := C.duckdb_string_t_data(strPtr)
	if strData == nil {
		return "", fmt.Errorf("null data pointer")
	}

	return C.GoStringN(strData, C.int(strLen)), nil
}

// setRowInvalid sets a row as invalid (NULL) in the validity mask.
// The chunkSize parameter is used for bounds validation to prevent memory corruption.
func setRowInvalid(validity *C.uint64_t, rowIdx uint64, chunkSize uint64) {
	if validity == nil {
		return
	}
	// Bounds check to prevent memory corruption
	if rowIdx >= chunkSize || rowIdx >= maxDuckDBChunkSize {
		return
	}
	entryIdx := rowIdx / 64
	idxInEntry := rowIdx % 64
	// Calculate required array size based on chunk size
	requiredEntries := (chunkSize + 63) / 64
	if entryIdx >= requiredEntries {
		return
	}
	validitySlice := (*[maxDuckDBChunkSize / 64]C.uint64_t)(unsafe.Pointer(validity))
	validitySlice[entryIdx] &^= (1 << idxInEntry)
}

// rowIsValid checks if a row is valid (not NULL) using the validity mask.
// The chunkSize parameter is used for bounds validation to prevent memory corruption.
func rowIsValid(validity *C.uint64_t, rowIdx uint64, chunkSize uint64) bool {
	if validity == nil {
		return true // No validity mask means all rows are valid
	}
	// Bounds check to prevent memory corruption
	if rowIdx >= chunkSize || rowIdx >= maxDuckDBChunkSize {
		return false // Treat out-of-bounds as invalid
	}
	entryIdx := rowIdx / 64
	idxInEntry := rowIdx % 64
	// Calculate required array size based on chunk size
	requiredEntries := (chunkSize + 63) / 64
	if entryIdx >= requiredEntries {
		return false
	}
	validitySlice := (*[maxDuckDBChunkSize / 64]C.uint64_t)(unsafe.Pointer(validity))
	return (validitySlice[entryIdx] & (1 << idxInEntry)) != 0
}

// validateURI delegates to the validation package for testability.
func validateURI(uri string) error {
	return validation.ValidateURI(uri)
}

// setScalarError is a helper to set an error on a scalar function with consistent formatting.
// All error messages are prefixed with "duckarrow_configure: " for clarity.
func setScalarError(info C.duckdb_function_info, msg string) {
	errMsg := C.CString("duckarrow_configure: " + msg)
	C.duckdb_scalar_function_set_error(info, errMsg)
	C.free(unsafe.Pointer(errMsg))
}

// RegisterDuckArrowConfigureFunction registers the duckarrow_configure(uri, username, password, [skip_verify]) scalar function.
// This function allows users to configure the gRPC endpoint and credentials for duckarrow.* table queries.
//
// Usage in SQL:
//
//	SELECT duckarrow_configure('grpc+tls://localhost:31337', 'username', 'password');
//	SELECT duckarrow_configure('grpc+tls://localhost:31337', 'username', 'password', true);  -- skip cert verification
//
// Parameters:
//   - conn: Active DuckDB connection for function registration
//
// Returns:
//   - duckdb.STATE_OK on success, duckdb.STATE_ERROR on failure
func RegisterDuckArrowConfigureFunction(conn duckdb.Connection) duckdb.State {
	// Create scalar function
	scalarFunc := C.duckdb_create_scalar_function()
	defer C.duckdb_destroy_scalar_function(&scalarFunc)

	// Set name
	name := C.CString("duckarrow_configure")
	defer C.free(unsafe.Pointer(name))
	C.duckdb_scalar_function_set_name(scalarFunc, name)

	// Add three required VARCHAR parameters (URI, username, password)
	varcharType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
	C.duckdb_scalar_function_add_parameter(scalarFunc, varcharType) // URI
	C.duckdb_scalar_function_add_parameter(scalarFunc, varcharType) // username
	C.duckdb_scalar_function_add_parameter(scalarFunc, varcharType) // password
	C.duckdb_destroy_logical_type(&varcharType)

	// Add optional BOOLEAN varargs for skip_verify (allows 0 or more boolean arguments)
	boolType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN)
	C.duckdb_scalar_function_set_varargs(scalarFunc, boolType)
	C.duckdb_destroy_logical_type(&boolType)

	// Set VARCHAR return type
	varcharRetType := C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
	C.duckdb_scalar_function_set_return_type(scalarFunc, varcharRetType)
	C.duckdb_destroy_logical_type(&varcharRetType)

	// Set the callback
	C.duckdb_scalar_function_set_function(scalarFunc,
		C.duckdb_scalar_function_t(C.duckarrow_configure_callback))

	// Register the function
	return duckdb.State(C.duckdb_register_scalar_function(
		C.duckdb_connection(conn.Ptr), scalarFunc))
}
