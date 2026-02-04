//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_table.cpp
//
//===----------------------------------------------------------------------===//
//
// Implementation of DuckArrowTableEntry for representing Flight SQL tables.
//
//===----------------------------------------------------------------------===//

#include "duckarrow_table.hpp"
#include "duckarrow_catalog.hpp"

namespace duckarrow {

//===--------------------------------------------------------------------===//
// External Callback References
//===--------------------------------------------------------------------===//

extern duckarrow_scan_table_bind_fn g_scan_table_bind_callback;
extern duckarrow_scan_get_column_fn g_scan_get_column_callback;
extern duckarrow_scan_init_fn g_scan_init_callback;
extern duckarrow_scan_next_fn g_scan_next_callback;
extern duckarrow_scan_free_fn g_scan_free_callback;
extern duckarrow_free_fn g_free_callback;

//===--------------------------------------------------------------------===//
// DuckArrowScanBindData - FunctionData for table scans
//===--------------------------------------------------------------------===//

struct DuckArrowScanBindData : public duckdb::FunctionData {
	DuckArrowScanBindData(DuckArrowScanHandle handle_p, const duckdb::string &uri_p,
	                      const duckdb::string &schema_name_p, const duckdb::string &table_name_p)
	    : scan_handle(handle_p), uri(uri_p), schema_name(schema_name_p), table_name(table_name_p) {
	}

	~DuckArrowScanBindData() override {
		if (scan_handle && g_scan_free_callback) {
			g_scan_free_callback(scan_handle);
		}
	}

	duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
		// Re-bind for copy since scan_handle is owned
		if (!g_scan_table_bind_callback) {
			throw duckdb::IOException("Scan bind callback not registered");
		}
		auto result = g_scan_table_bind_callback(
		    uri.c_str(),
		    schema_name.empty() ? nullptr : schema_name.c_str(),
		    table_name.c_str());
		if (result.error) {
			duckdb::string err(result.error);
			if (g_free_callback) {
				g_free_callback((void *)result.error);
			}
			throw duckdb::IOException("Failed to copy scan bind data: %s", err);
		}
		return duckdb::make_uniq<DuckArrowScanBindData>(result.handle, uri, schema_name, table_name);
	}

	bool Equals(const duckdb::FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckArrowScanBindData>();
		return uri == other.uri && schema_name == other.schema_name && table_name == other.table_name;
	}

	DuckArrowScanHandle scan_handle;
	duckdb::string uri;
	duckdb::string schema_name;
	duckdb::string table_name;
};

//===--------------------------------------------------------------------===//
// DuckArrowScanGlobalState - Global state for table scans
//===--------------------------------------------------------------------===//

struct DuckArrowScanGlobalState : public duckdb::GlobalTableFunctionState {
	DuckArrowScanGlobalState() : initialized(false), finished(false) {
	}

	bool initialized;
	bool finished;
	duckdb::vector<duckdb::column_t> column_ids;

	idx_t MaxThreads() const override {
		// Single-threaded for now - Flight SQL queries are typically not parallelizable
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// DuckArrowScanLocalState - Local state for table scans
//===--------------------------------------------------------------------===//

struct DuckArrowScanLocalState : public duckdb::LocalTableFunctionState {
	// No local state needed for single-threaded scan
};

//===--------------------------------------------------------------------===//
// External Callback References
//===--------------------------------------------------------------------===//

// These are defined in duckarrow_catalog.cpp and set by Go
extern duckarrow_get_columns_fn g_get_columns_callback;

//===--------------------------------------------------------------------===//
// Type Conversion
//===--------------------------------------------------------------------===//

// Convert a Flight SQL column type string to a DuckDB LogicalType.
// Flight SQL returns type names like "VARCHAR", "BIGINT", "DOUBLE", etc.
// This function maps those strings to DuckDB logical types.
duckdb::LogicalType FlightSQLTypeToDuckDB(const duckdb::string &type_str) {
	// Convert to uppercase for case-insensitive comparison
	duckdb::string upper_type = duckdb::StringUtil::Upper(type_str);

	// String types
	if (upper_type == "VARCHAR" || upper_type == "STRING" || upper_type == "TEXT" ||
	    upper_type == "CHAR" || upper_type == "BPCHAR" || upper_type == "NAME") {
		return duckdb::LogicalType::VARCHAR;
	}

	// Integer types
	if (upper_type == "BIGINT" || upper_type == "INT8" || upper_type == "INT64" || upper_type == "LONG") {
		return duckdb::LogicalType::BIGINT;
	}
	if (upper_type == "INTEGER" || upper_type == "INT" || upper_type == "INT4" || upper_type == "INT32") {
		return duckdb::LogicalType::INTEGER;
	}
	if (upper_type == "SMALLINT" || upper_type == "INT2" || upper_type == "INT16" || upper_type == "SHORT") {
		return duckdb::LogicalType::SMALLINT;
	}
	if (upper_type == "TINYINT" || upper_type == "INT1" || upper_type == "INT8") {
		return duckdb::LogicalType::TINYINT;
	}

	// Unsigned integer types
	if (upper_type == "UBIGINT" || upper_type == "UINT8" || upper_type == "UINT64" || upper_type == "ULONG") {
		return duckdb::LogicalType::UBIGINT;
	}
	if (upper_type == "UINTEGER" || upper_type == "UINT" || upper_type == "UINT4" || upper_type == "UINT32") {
		return duckdb::LogicalType::UINTEGER;
	}
	if (upper_type == "USMALLINT" || upper_type == "UINT2" || upper_type == "UINT16" || upper_type == "USHORT") {
		return duckdb::LogicalType::USMALLINT;
	}
	if (upper_type == "UTINYINT" || upper_type == "UINT1") {
		return duckdb::LogicalType::UTINYINT;
	}

	// Floating point types
	if (upper_type == "DOUBLE" || upper_type == "FLOAT8" || upper_type == "DOUBLE PRECISION" ||
	    upper_type == "NUMERIC" || upper_type == "REAL8") {
		return duckdb::LogicalType::DOUBLE;
	}
	if (upper_type == "FLOAT" || upper_type == "FLOAT4" || upper_type == "REAL") {
		return duckdb::LogicalType::FLOAT;
	}

	// Boolean type
	if (upper_type == "BOOLEAN" || upper_type == "BOOL") {
		return duckdb::LogicalType::BOOLEAN;
	}

	// Date/time types
	if (upper_type == "DATE") {
		return duckdb::LogicalType::DATE;
	}
	if (upper_type == "TIME" || upper_type == "TIME WITHOUT TIME ZONE") {
		return duckdb::LogicalType::TIME;
	}
	if (upper_type == "TIMESTAMP" || upper_type == "DATETIME" ||
	    upper_type == "TIMESTAMP WITHOUT TIME ZONE") {
		return duckdb::LogicalType::TIMESTAMP;
	}
	if (upper_type == "TIMESTAMPTZ" || upper_type == "TIMESTAMP WITH TIME ZONE") {
		return duckdb::LogicalType::TIMESTAMP_TZ;
	}
	if (upper_type == "INTERVAL") {
		return duckdb::LogicalType::INTERVAL;
	}

	// Binary types
	if (upper_type == "BLOB" || upper_type == "BYTEA" || upper_type == "BINARY" ||
	    upper_type == "VARBINARY" || upper_type == "BYTES") {
		return duckdb::LogicalType::BLOB;
	}

	// UUID type
	if (upper_type == "UUID") {
		return duckdb::LogicalType::UUID;
	}

	// JSON type
	if (upper_type == "JSON" || upper_type == "JSONB") {
		return duckdb::LogicalType::JSON();
	}

	// Handle DECIMAL/NUMERIC with precision and scale
	// Format: DECIMAL(p,s) or NUMERIC(p,s)
	if (duckdb::StringUtil::StartsWith(upper_type, "DECIMAL") ||
	    duckdb::StringUtil::StartsWith(upper_type, "NUMERIC")) {
		// Try to parse precision and scale
		auto paren_start = upper_type.find('(');
		auto paren_end = upper_type.find(')');
		if (paren_start != duckdb::string::npos && paren_end != duckdb::string::npos) {
			auto params = upper_type.substr(paren_start + 1, paren_end - paren_start - 1);
			auto comma_pos = params.find(',');
			if (comma_pos != duckdb::string::npos) {
				try {
					auto precision = std::stoi(params.substr(0, comma_pos));
					auto scale = std::stoi(params.substr(comma_pos + 1));
					// DuckDB max precision is 38
					if (precision > 38) {
						precision = 38;
					}
					return duckdb::LogicalType::DECIMAL(precision, scale);
				} catch (...) {
					// Fall through to default DECIMAL
				}
			} else {
				// Just precision, no scale
				try {
					auto precision = std::stoi(params);
					if (precision > 38) {
						precision = 38;
					}
					return duckdb::LogicalType::DECIMAL(precision, 0);
				} catch (...) {
					// Fall through to default DECIMAL
				}
			}
		}
		// Default DECIMAL with reasonable precision
		return duckdb::LogicalType::DECIMAL(18, 3);
	}

	// HUGEINT for very large integers
	if (upper_type == "HUGEINT" || upper_type == "INT128") {
		return duckdb::LogicalType::HUGEINT;
	}

	// Default to VARCHAR for unknown types
	// This is safe because DuckDB will attempt implicit casts if needed
	return duckdb::LogicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// DuckArrowTableEntry Constructor
//===--------------------------------------------------------------------===//

DuckArrowTableEntry::DuckArrowTableEntry(duckdb::Catalog &catalog,
                                         duckdb::SchemaCatalogEntry &schema,
                                         duckdb::CreateTableInfo &info,
                                         DuckArrowConnectionHandle handle,
                                         const duckdb::string &schema_name,
                                         const duckdb::string &table_name)
    : duckdb::TableCatalogEntry(catalog, schema, info), connection_handle(handle),
      remote_schema_name(schema_name), remote_table_name(table_name) {
}

//===--------------------------------------------------------------------===//
// TableCatalogEntry API Implementation
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<duckdb::BaseStatistics>
DuckArrowTableEntry::GetStatistics(duckdb::ClientContext & /* context */,
                                   duckdb::column_t /* column_id */) {
	// Flight SQL doesn't provide column statistics
	// Return nullptr to indicate no statistics available
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Static C++ Callbacks for TableFunction
//===--------------------------------------------------------------------===//

// These static functions are called by DuckDB's C++ TableFunction mechanism
// and delegate to Go via the registered callbacks.

static duckdb::unique_ptr<duckdb::FunctionData>
DuckArrowScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                  duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
	// Get parameters from input
	auto &inputs = input.inputs;
	if (inputs.size() < 3) {
		throw duckdb::IOException("DuckArrowScan requires uri, schema, and table parameters");
	}

	auto uri = inputs[0].GetValue<duckdb::string>();
	auto schema_name = inputs[1].IsNull() ? "" : inputs[1].GetValue<duckdb::string>();
	auto table_name = inputs[2].GetValue<duckdb::string>();

	// Call Go to bind the scan
	if (!g_scan_table_bind_callback) {
		throw duckdb::IOException("DuckArrow scan bind callback not registered");
	}

	auto result = g_scan_table_bind_callback(
	    uri.c_str(),
	    schema_name.empty() ? nullptr : schema_name.c_str(),
	    table_name.c_str());

	if (result.error) {
		duckdb::string err(result.error);
		if (g_free_callback) {
			g_free_callback((void *)result.error);
		}
		throw duckdb::IOException("Failed to bind scan: %s", err);
	}

	// Get column information from Go
	if (!g_scan_get_column_callback) {
		if (g_scan_free_callback) {
			g_scan_free_callback(result.handle);
		}
		throw duckdb::IOException("DuckArrow scan get column callback not registered");
	}

	for (size_t i = 0; i < result.column_count; i++) {
		auto col = g_scan_get_column_callback(result.handle, i);
		names.push_back(col.name);
		return_types.push_back(FlightSQLTypeToDuckDB(col.type_name));
		// Note: col.name and col.type_name are owned by Go and freed when scan_handle is freed
	}

	return duckdb::make_uniq<DuckArrowScanBindData>(result.handle, uri, schema_name, table_name);
}

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
DuckArrowScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input) {
	auto result = duckdb::make_uniq<DuckArrowScanGlobalState>();

	// Store column IDs for projection pushdown
	for (auto &col_id : input.column_ids) {
		result->column_ids.push_back(col_id);
	}

	return std::move(result);
}

static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
DuckArrowScanInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                       duckdb::GlobalTableFunctionState *global_state) {
	return duckdb::make_uniq<DuckArrowScanLocalState>();
}

static void DuckArrowScanFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                                  duckdb::DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<DuckArrowScanBindData>();
	auto &global_state = data.global_state->Cast<DuckArrowScanGlobalState>();

	if (global_state.finished) {
		output.SetCardinality(0);
		return;
	}

	// Initialize on first call
	if (!global_state.initialized) {
		if (!g_scan_init_callback) {
			throw duckdb::IOException("DuckArrow scan init callback not registered");
		}

		// Convert column_t (uint64_t) to size_t for callback compatibility
		// On macOS, these are different types even though both are 64-bit
		std::vector<size_t> column_ids_converted(global_state.column_ids.begin(),
		                                         global_state.column_ids.end());

		// Pass column IDs for projection pushdown
		const char *err = g_scan_init_callback(
		    bind_data.scan_handle,
		    column_ids_converted.data(),
		    column_ids_converted.size());

		if (err) {
			duckdb::string error_msg(err);
			if (g_free_callback) {
				g_free_callback((void *)err);
			}
			throw duckdb::IOException("Failed to initialize scan: %s", error_msg);
		}
		global_state.initialized = true;
	}

	// Get next chunk from Go
	if (!g_scan_next_callback) {
		throw duckdb::IOException("DuckArrow scan next callback not registered");
	}

	// Note: We pass the raw DataChunk pointer to Go, which will fill it using the C API
	// This requires that Go can access duckdb_data_chunk functions
	int64_t row_count = g_scan_next_callback(bind_data.scan_handle, &output);

	if (row_count < 0) {
		throw duckdb::IOException("Error during scan");
	}

	if (row_count == 0) {
		global_state.finished = true;
	}

	output.SetCardinality(row_count);
}

duckdb::TableFunction
DuckArrowTableEntry::GetScanFunction(duckdb::ClientContext &context,
                                     duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	// Get the catalog to access the URI
	auto &duck_catalog = catalog.Cast<DuckArrowCatalog>();
	const auto &uri = duck_catalog.GetURI();

	// Check if scan callbacks are registered
	if (!g_scan_table_bind_callback) {
		throw duckdb::IOException("DuckArrow scan bind callback not registered");
	}

	// Bind the scan to the specific table
	auto result = g_scan_table_bind_callback(
	    uri.c_str(),
	    remote_schema_name.empty() ? nullptr : remote_schema_name.c_str(),
	    remote_table_name.c_str());

	if (result.error) {
		duckdb::string err(result.error);
		if (g_free_callback) {
			g_free_callback((void *)result.error);
		}
		throw duckdb::IOException("Failed to bind scan: %s", err);
	}

	// Create the bind data with the scan handle
	bind_data = duckdb::make_uniq<DuckArrowScanBindData>(result.handle, uri, remote_schema_name, remote_table_name);

	// Create and return the TableFunction
	// Note: We don't pass the bind callback here since bind_data is already set
	duckdb::TableFunction scan_func("duckarrow_attached_scan", {}, DuckArrowScanFunction,
	                                 nullptr, DuckArrowScanInitGlobal, DuckArrowScanInitLocal);

	scan_func.projection_pushdown = true;

	return scan_func;
}

duckdb::TableStorageInfo
DuckArrowTableEntry::GetStorageInfo(duckdb::ClientContext & /* context */) {
	// Flight SQL doesn't provide detailed storage information
	duckdb::TableStorageInfo result;
	// Leave index_info empty - no indexes for Flight SQL tables
	return result;
}

//===--------------------------------------------------------------------===//
// DuckArrow-specific Methods
//===--------------------------------------------------------------------===//

DuckArrowConnectionHandle DuckArrowTableEntry::GetConnectionHandle() const {
	return connection_handle;
}

const duckdb::string &DuckArrowTableEntry::GetRemoteSchemaName() const {
	return remote_schema_name;
}

const duckdb::string &DuckArrowTableEntry::GetRemoteTableName() const {
	return remote_table_name;
}

//===--------------------------------------------------------------------===//
// Static Factory Method
//===--------------------------------------------------------------------===//

duckdb::unique_ptr<DuckArrowTableEntry> DuckArrowTableEntry::CreateFromFlightSQL(
    duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
    DuckArrowConnectionHandle connection_handle, const duckdb::string &schema_name,
    const duckdb::string &table_name) {

	// Check if the get_columns callback is registered
	if (!g_get_columns_callback || !connection_handle) {
		return nullptr;
	}

	// Get column metadata from Flight SQL via Go callback
	DuckArrowColumnList column_list = g_get_columns_callback(
	    connection_handle,
	    nullptr,  // catalog (use default)
	    schema_name.empty() ? nullptr : schema_name.c_str(),
	    table_name.c_str());

	if (column_list.error) {
		duckdb::string error_msg(column_list.error);
		duckarrow_free_column_list(&column_list);
		throw duckdb::IOException("Failed to get columns from Flight SQL: %s", error_msg);
	}

	// If no columns returned, the table doesn't exist
	if (column_list.count == 0) {
		duckarrow_free_column_list(&column_list);
		return nullptr;
	}

	// Create the table info with columns
	duckdb::CreateTableInfo info;
	info.schema = schema.name;
	info.table = table_name;

	for (size_t i = 0; i < column_list.count; i++) {
		auto &col = column_list.columns[i];
		duckdb::string col_name(col.column_name);
		duckdb::LogicalType col_type = FlightSQLTypeToDuckDB(col.column_type);
		info.columns.AddColumn(duckdb::ColumnDefinition(col_name, col_type));
	}

	// Clean up the column list
	duckarrow_free_column_list(&column_list);

	// Create and return the table entry
	return duckdb::make_uniq<DuckArrowTableEntry>(
	    catalog, schema, info, connection_handle, schema_name, table_name);
}

} // namespace duckarrow
