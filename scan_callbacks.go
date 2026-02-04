package main

/*
#cgo CFLAGS: -I${SRCDIR}/duckdb-go-api -I${SRCDIR}/cpp -DDUCKDB_API_EXCLUDE_FUNCTIONS=1
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include <duckdb_go_extension.h>
#include "go_callbacks.h"
*/
import "C"
import (
	"context"
	"fmt"
	"runtime"
	"runtime/cgo"
	"strings"
	"sync/atomic"
	"unsafe"

	"main/internal/flight"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScanBindData holds state for a table scan bound via GetScanFunction.
// This is similar to BindData but specifically for attached database scans.
type ScanBindData struct {
	// Connection info
	Client   *flight.Client
	Config   flight.Config
	IsPooled bool
	URI      string

	// Table identification
	SchemaName string
	TableName  string

	// Column metadata (populated during bind)
	ColumnNames []string
	ColumnTypes []string

	// Query state (populated during init)
	Stmt   adbc.Statement
	Reader array.RecordReader
	Schema *arrow.Schema

	// Scan state
	CurrentBatch  arrow.RecordBatch
	BatchPosition int64
	Done          int32

	// Projection (populated during init)
	ProjectedColumns []int // Indices of columns to fetch
}

// duckarrow_go_scan_table_bind binds a scan for a table in an attached database.
// This creates the connection and gets column metadata.
//
//export duckarrow_go_scan_table_bind
func duckarrow_go_scan_table_bind(uri *C.char, schemaName *C.char, tableName *C.char) C.DuckArrowScanBindResult {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowScanBindResult
	result.handle = nil
	result.error = nil
	result.column_count = 0

	// Validate parameters
	if uri == nil || tableName == nil {
		result.error = C.CString("duckarrow_go_scan_table_bind: uri and table_name are required")
		return result
	}

	uriStr := C.GoString(uri)
	tableStr := C.GoString(tableName)
	var schemaStr string
	if schemaName != nil {
		schemaStr = C.GoString(schemaName)
	}

	// Get credentials from global config
	_, configUsername, configPassword, configSkipVerify := GetDuckArrowConfig()

	// Build connection config
	cfg := flight.Config{
		URI:        uriStr,
		Username:   configUsername,
		Password:   configPassword,
		SkipVerify: configSkipVerify,
	}

	// Get connection from pool
	ctx := context.Background()
	connResult, err := flight.GetConnection(ctx, cfg)
	if err != nil {
		result.error = C.CString(fmt.Sprintf("duckarrow_go_scan_table_bind: connection failed: %v", err))
		return result
	}

	// Build schema query to get column metadata
	// Use WHERE 1=0 to avoid fetching any data
	var query string
	if schemaStr != "" {
		escapedSchema := strings.ReplaceAll(schemaStr, `"`, `""`)
		escapedTable := strings.ReplaceAll(tableStr, `"`, `""`)
		query = fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE 1=0`, escapedSchema, escapedTable)
	} else {
		escapedTable := strings.ReplaceAll(tableStr, `"`, `""`)
		query = fmt.Sprintf(`SELECT * FROM "%s" WHERE 1=0`, escapedTable)
	}

	// Execute schema query
	queryResult, err := connResult.Client.Query(ctx, query)
	if err != nil {
		if connResult.IsPooled {
			flight.ReleaseConnection(cfg)
		} else {
			connResult.Client.Close()
		}
		result.error = C.CString(fmt.Sprintf("duckarrow_go_scan_table_bind: schema query failed: %v", err))
		return result
	}

	// Get schema and column info
	schema := queryResult.Reader.Schema()
	columnNames := make([]string, len(schema.Fields()))
	columnTypes := make([]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columnNames[i] = field.Name
		columnTypes[i] = arrowTypeToString(field.Type)
	}

	// Release schema query resources - we'll re-execute with projection in init
	queryResult.Reader.Release()
	queryResult.Stmt.Close()

	// Create bind data
	bindData := &ScanBindData{
		Client:      connResult.Client,
		Config:      cfg,
		IsPooled:    connResult.IsPooled,
		URI:         uriStr,
		SchemaName:  schemaStr,
		TableName:   tableStr,
		ColumnNames: columnNames,
		ColumnTypes: columnTypes,
		Schema:      schema,
	}

	// Create cgo handle
	handle := cgo.NewHandle(bindData)
	result.handle = C.DuckArrowScanHandle(uintptr(handle))
	result.column_count = C.size_t(len(columnNames))

	return result
}

// arrowTypeToString converts an Arrow type to a string representation for C++
func arrowTypeToString(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR"
	case arrow.INT64:
		return "BIGINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT8:
		return "TINYINT"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIME32, arrow.TIME64:
		return "TIME"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "BLOB"
	case arrow.DECIMAL128:
		dt := t.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
	default:
		return "VARCHAR"
	}
}

// duckarrow_go_scan_get_column returns column information for a bound scan.
//
//export duckarrow_go_scan_get_column
func duckarrow_go_scan_get_column(scanHandle C.DuckArrowScanHandle, index C.size_t) C.DuckArrowScanColumn {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var result C.DuckArrowScanColumn
	result.name = nil
	result.type_name = nil

	if scanHandle == nil {
		return result
	}

	handle := cgo.Handle(uintptr(scanHandle))
	bindData, ok := handle.Value().(*ScanBindData)
	if !ok || bindData == nil {
		return result
	}

	idx := int(index)
	if idx < 0 || idx >= len(bindData.ColumnNames) {
		return result
	}

	// Note: These strings are allocated with C.CString and must be freed by the caller
	result.name = C.CString(bindData.ColumnNames[idx])
	result.type_name = C.CString(bindData.ColumnTypes[idx])

	return result
}

// duckarrow_go_scan_init initializes the scan with projection pushdown.
//
//export duckarrow_go_scan_init
func duckarrow_go_scan_init(scanHandle C.DuckArrowScanHandle, columnIDs *C.size_t, columnCount C.size_t) *C.char {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if scanHandle == nil {
		return C.CString("duckarrow_go_scan_init: nil scan handle")
	}

	handle := cgo.Handle(uintptr(scanHandle))
	bindData, ok := handle.Value().(*ScanBindData)
	if !ok || bindData == nil {
		return C.CString("duckarrow_go_scan_init: invalid scan handle")
	}

	// Extract projected column indices
	count := int(columnCount)
	projectedColumns := make([]int, count)
	if count > 0 && columnIDs != nil {
		ids := unsafe.Slice(columnIDs, count)
		for i := 0; i < count; i++ {
			projectedColumns[i] = int(ids[i])
		}
	}
	bindData.ProjectedColumns = projectedColumns

	// Build query with projected columns
	var columnList string
	if count == 0 || count == len(bindData.ColumnNames) {
		columnList = "*"
	} else {
		cols := make([]string, count)
		for i, colIdx := range projectedColumns {
			if colIdx >= 0 && colIdx < len(bindData.ColumnNames) {
				escapedCol := strings.ReplaceAll(bindData.ColumnNames[colIdx], `"`, `""`)
				cols[i] = fmt.Sprintf(`"%s"`, escapedCol)
			}
		}
		columnList = strings.Join(cols, ", ")
	}

	var query string
	if bindData.SchemaName != "" {
		escapedSchema := strings.ReplaceAll(bindData.SchemaName, `"`, `""`)
		escapedTable := strings.ReplaceAll(bindData.TableName, `"`, `""`)
		query = fmt.Sprintf(`SELECT %s FROM "%s"."%s"`, columnList, escapedSchema, escapedTable)
	} else {
		escapedTable := strings.ReplaceAll(bindData.TableName, `"`, `""`)
		query = fmt.Sprintf(`SELECT %s FROM "%s"`, columnList, escapedTable)
	}

	// Execute the actual query
	ctx := context.Background()
	queryResult, err := bindData.Client.Query(ctx, query)
	if err != nil {
		return C.CString(fmt.Sprintf("duckarrow_go_scan_init: query failed: %v", err))
	}

	bindData.Stmt = queryResult.Stmt
	bindData.Reader = queryResult.Reader

	return nil // Success
}

// duckarrow_go_scan_next scans the next chunk of data.
// Returns the number of rows, or negative on error.
//
//export duckarrow_go_scan_next
func duckarrow_go_scan_next(scanHandle C.DuckArrowScanHandle, output unsafe.Pointer) C.int64_t {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if scanHandle == nil {
		return -1
	}

	handle := cgo.Handle(uintptr(scanHandle))
	bindData, ok := handle.Value().(*ScanBindData)
	if !ok || bindData == nil {
		return -1
	}

	if atomic.LoadInt32(&bindData.Done) == 1 {
		return 0
	}

	if bindData.Reader == nil {
		return -1
	}

	// Get next batch if needed
	if bindData.CurrentBatch == nil || bindData.BatchPosition >= bindData.CurrentBatch.NumRows() {
		// Release previous batch
		if bindData.CurrentBatch != nil {
			bindData.CurrentBatch.Release()
			bindData.CurrentBatch = nil
		}

		if !bindData.Reader.Next() {
			if err := bindData.Reader.Err(); err != nil {
				// Error during scan
				return -1
			}
			atomic.StoreInt32(&bindData.Done, 1)
			return 0
		}

		// Get new batch and retain it
		bindData.CurrentBatch = bindData.Reader.RecordBatch()
		bindData.CurrentBatch.Retain()
		bindData.BatchPosition = 0
	}

	// Calculate rows to emit (max 2048 per DuckDB chunk)
	const maxChunkSize = 2048
	remaining := bindData.CurrentBatch.NumRows() - bindData.BatchPosition
	rowsToEmit := int(remaining)
	if rowsToEmit > maxChunkSize {
		rowsToEmit = maxChunkSize
	}

	// Convert the output pointer to duckdb_data_chunk
	chunk := C.duckdb_data_chunk(output)

	// Convert each column
	numCols := int(bindData.CurrentBatch.NumCols())
	for colIdx := 0; colIdx < numCols; colIdx++ {
		arrowCol := bindData.CurrentBatch.Column(colIdx)
		duckVec := C.duckdb_data_chunk_get_vector(chunk, C.idx_t(colIdx))

		if err := convertArrowToDuckDBVector(arrowCol, duckVec, int(bindData.BatchPosition), rowsToEmit); err != nil {
			return -1
		}
	}

	bindData.BatchPosition += int64(rowsToEmit)
	C.duckdb_data_chunk_set_size(chunk, C.idx_t(rowsToEmit))

	return C.int64_t(rowsToEmit)
}

// convertArrowToDuckDBVector converts Arrow column data to a DuckDB vector.
// This is a simplified version that handles common types.
func convertArrowToDuckDBVector(arrowCol arrow.Array, duckVec C.duckdb_vector, offset, count int) error {
	// Ensure validity mask is writable
	C.duckdb_vector_ensure_validity_writable(duckVec)
	validity := C.duckdb_vector_get_validity(duckVec)

	// Handle type-specific conversion
	switch col := arrowCol.(type) {
	case *array.String:
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if col.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			val := col.Value(srcIdx)
			cStr := C.CString(val)
			C.duckdb_vector_assign_string_element(duckVec, C.idx_t(i), cStr)
			C.free(unsafe.Pointer(cStr))
		}

	case *array.Int64:
		ptr := (*C.int64_t)(C.duckdb_vector_get_data(duckVec))
		data := unsafe.Slice(ptr, count)
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if col.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			data[i] = C.int64_t(col.Value(srcIdx))
		}

	case *array.Int32:
		ptr := (*C.int32_t)(C.duckdb_vector_get_data(duckVec))
		data := unsafe.Slice(ptr, count)
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if col.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			data[i] = C.int32_t(col.Value(srcIdx))
		}

	case *array.Float64:
		ptr := (*C.double)(C.duckdb_vector_get_data(duckVec))
		data := unsafe.Slice(ptr, count)
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if col.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			data[i] = C.double(col.Value(srcIdx))
		}

	case *array.Boolean:
		ptr := (*C.uint8_t)(C.duckdb_vector_get_data(duckVec))
		data := unsafe.Slice(ptr, count)
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if col.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			if col.Value(srcIdx) {
				data[i] = 1
			} else {
				data[i] = 0
			}
		}

	default:
		// Fallback: convert to string
		for i := 0; i < count; i++ {
			srcIdx := offset + i
			if arrowCol.IsNull(srcIdx) {
				C.duckdb_validity_set_row_invalid(validity, C.idx_t(i))
				continue
			}
			var val string
			if stringer, ok := arrowCol.(interface{ ValueStr(int) string }); ok {
				val = stringer.ValueStr(srcIdx)
			} else {
				val = fmt.Sprintf("%v", arrowCol.GetOneForMarshal(srcIdx))
			}
			cStr := C.CString(val)
			C.duckdb_vector_assign_string_element(duckVec, C.idx_t(i), cStr)
			C.free(unsafe.Pointer(cStr))
		}
	}

	return nil
}

// duckarrow_go_scan_free frees resources associated with a scan handle.
//
//export duckarrow_go_scan_free
func duckarrow_go_scan_free(scanHandle C.DuckArrowScanHandle) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if scanHandle == nil {
		return
	}

	handle := cgo.Handle(uintptr(scanHandle))
	bindData, ok := handle.Value().(*ScanBindData)
	if ok && bindData != nil {
		// Release current batch
		if bindData.CurrentBatch != nil {
			bindData.CurrentBatch.Release()
		}

		// Clean up query resources
		if bindData.Reader != nil {
			bindData.Reader.Release()
		}
		if bindData.Stmt != nil {
			bindData.Stmt.Close()
		}

		// Clean up connection
		if bindData.Client != nil {
			if bindData.IsPooled {
				flight.ReleaseConnection(bindData.Config)
			} else {
				bindData.Client.Close()
			}
		}
	}

	handle.Delete()
}
