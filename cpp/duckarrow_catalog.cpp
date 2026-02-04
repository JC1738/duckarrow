//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_catalog.cpp
//
//===----------------------------------------------------------------------===//
//
// Implementation of DuckArrowCatalog for accessing Flight SQL server metadata.
//
//===----------------------------------------------------------------------===//

#include "duckarrow_catalog.hpp"
#include "duckarrow_schema.hpp"
#include <mutex>

namespace duckarrow {

//===--------------------------------------------------------------------===//
// Global Callback Storage
//===--------------------------------------------------------------------===//

// These are set by Go via the duckarrow_register_* functions
// Not static so they can be accessed from duckarrow_schema.cpp, duckarrow_table.cpp, duckarrow_storage.cpp
duckarrow_connect_fn g_connect_callback = nullptr;
duckarrow_list_schemas_fn g_list_schemas_callback = nullptr;
duckarrow_list_tables_fn g_list_tables_callback = nullptr;
duckarrow_get_columns_fn g_get_columns_callback = nullptr;
duckarrow_free_fn g_free_callback = nullptr;

// Scan callbacks for GetScanFunction
duckarrow_scan_table_bind_fn g_scan_table_bind_callback = nullptr;
duckarrow_scan_get_column_fn g_scan_get_column_callback = nullptr;
duckarrow_scan_init_fn g_scan_init_callback = nullptr;
duckarrow_scan_next_fn g_scan_next_callback = nullptr;
duckarrow_scan_free_fn g_scan_free_callback = nullptr;

//===--------------------------------------------------------------------===//
// DuckArrowCatalog Constructor/Destructor
//===--------------------------------------------------------------------===//

DuckArrowCatalog::DuckArrowCatalog(duckdb::AttachedDatabase &db, DuckArrowOptions options_p)
    : duckdb::Catalog(db), options(std::move(options_p)), connection_handle(nullptr) {
	// Connection handle will be set by Go when the connection is established
}

DuckArrowCatalog::~DuckArrowCatalog() {
	// Connection cleanup is handled by Go
	connection_handle = nullptr;
}

//===--------------------------------------------------------------------===//
// Catalog Interface Implementation
//===--------------------------------------------------------------------===//

duckdb::string DuckArrowCatalog::GetCatalogType() {
	return "duckarrow";
}

void DuckArrowCatalog::Initialize(bool /* load_builtin */) {
	// No initialization needed - metadata is fetched on demand from Flight SQL
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowCatalog::CreateSchema(duckdb::CatalogTransaction /* transaction */,
                               duckdb::CreateSchemaInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CreateSchema - Flight SQL is read-only");
}

void DuckArrowCatalog::DropSchema(duckdb::ClientContext & /* context */,
                                  duckdb::DropInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support DropSchema - Flight SQL is read-only");
}

void DuckArrowCatalog::ScanSchemas(duckdb::ClientContext & /* context */,
                                   std::function<void(duckdb::SchemaCatalogEntry &)> /* callback */) {
	// If no callback registered, nothing to scan
	if (!g_list_schemas_callback || !connection_handle) {
		return;
	}

	// Get schemas from Flight SQL via Go callback
	DuckArrowSchemaList schema_list = g_list_schemas_callback(connection_handle, nullptr);

	if (schema_list.error) {
		duckdb::string error_msg(schema_list.error);
		duckarrow_free_schema_list(&schema_list);
		throw duckdb::IOException("Failed to list schemas from Flight SQL: %s", error_msg);
	}

	// Note: ScanSchemas requires SchemaCatalogEntry objects.
	// For full implementation, we would need DuckArrowSchemaEntry class.
	// For now, we clean up and return (schemas will be accessed via GetSchema).
	duckarrow_free_schema_list(&schema_list);
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
DuckArrowCatalog::GetSchema(duckdb::CatalogTransaction /* transaction */,
                            const duckdb::string &schema_name,
                            duckdb::OnEntryNotFound if_not_found,
                            duckdb::QueryErrorContext /* error_context */) {
	std::lock_guard<std::mutex> guard(schema_cache_lock);

	// Check if schema already exists in cache
	auto it = schema_cache.find(schema_name);
	if (it != schema_cache.end()) {
		return it->second.get();
	}

	// No connection handle means we can't verify schema existence
	if (!connection_handle) {
		if (if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
			throw duckdb::CatalogException("Schema '%s' not found (no connection)", schema_name);
		}
		return nullptr;
	}

	// Create a new schema entry for the requested schema
	// Note: Flight SQL doesn't require schemas to be pre-registered,
	// so we create schema entries on demand
	duckdb::CreateSchemaInfo info;
	info.schema = schema_name;
	info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

	auto schema_entry = duckdb::make_uniq<DuckArrowSchemaEntry>(*this, info, connection_handle);
	auto result = schema_entry.get();
	schema_cache[schema_name] = std::move(schema_entry);

	return result;
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
DuckArrowCatalog::PlanCreateTableAs(duckdb::ClientContext & /* context */,
                                    duckdb::LogicalCreateTable & /* op */,
                                    duckdb::unique_ptr<duckdb::PhysicalOperator> /* plan */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE TABLE AS - Flight SQL is read-only");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
DuckArrowCatalog::PlanInsert(duckdb::ClientContext & /* context */,
                             duckdb::LogicalInsert & /* op */,
                             duckdb::unique_ptr<duckdb::PhysicalOperator> /* plan */) {
	throw duckdb::NotImplementedException("DuckArrow does not support INSERT - Flight SQL is read-only");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
DuckArrowCatalog::PlanDelete(duckdb::ClientContext & /* context */,
                             duckdb::LogicalDelete & /* op */,
                             duckdb::unique_ptr<duckdb::PhysicalOperator> /* plan */) {
	throw duckdb::NotImplementedException("DuckArrow does not support DELETE - Flight SQL is read-only");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
DuckArrowCatalog::PlanUpdate(duckdb::ClientContext & /* context */,
                             duckdb::LogicalUpdate & /* op */,
                             duckdb::unique_ptr<duckdb::PhysicalOperator> /* plan */) {
	throw duckdb::NotImplementedException("DuckArrow does not support UPDATE - Flight SQL is read-only");
}

duckdb::unique_ptr<duckdb::LogicalOperator>
DuckArrowCatalog::BindCreateIndex(duckdb::Binder & /* binder */,
                                  duckdb::CreateStatement & /* stmt */,
                                  duckdb::TableCatalogEntry & /* table */,
                                  duckdb::unique_ptr<duckdb::LogicalOperator> /* plan */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE INDEX - Flight SQL is read-only");
}

duckdb::DatabaseSize DuckArrowCatalog::GetDatabaseSize(duckdb::ClientContext & /* context */) {
	// Flight SQL doesn't provide database size information
	duckdb::DatabaseSize result;
	result.free_blocks = 0;
	result.total_blocks = 0;
	result.used_blocks = 0;
	result.wal_size = 0;
	result.block_size = 0;
	result.bytes = 0;
	return result;
}

bool DuckArrowCatalog::InMemory() {
	// Flight SQL is a remote database, not in-memory
	return false;
}

duckdb::string DuckArrowCatalog::GetDBPath() {
	return options.uri;
}

//===--------------------------------------------------------------------===//
// DuckArrow-specific Methods
//===--------------------------------------------------------------------===//

const DuckArrowOptions &DuckArrowCatalog::GetOptions() const {
	return options;
}

const duckdb::string &DuckArrowCatalog::GetURI() const {
	return options.uri;
}

void DuckArrowCatalog::SetConnectionHandle(DuckArrowConnectionHandle handle) {
	std::lock_guard<std::mutex> guard(schema_cache_lock);
	connection_handle = handle;
}

DuckArrowConnectionHandle DuckArrowCatalog::GetConnectionHandle() const {
	std::lock_guard<std::mutex> guard(schema_cache_lock);
	return connection_handle;
}

} // namespace duckarrow

//===--------------------------------------------------------------------===//
// C API for Go Callback Registration
//===--------------------------------------------------------------------===//

extern "C" {

DUCKARROW_API void duckarrow_register_connect(duckarrow_connect_fn callback) {
	duckarrow::g_connect_callback = callback;
}

DUCKARROW_API void duckarrow_register_list_schemas(duckarrow_list_schemas_fn callback) {
	duckarrow::g_list_schemas_callback = callback;
}

DUCKARROW_API void duckarrow_register_list_tables(duckarrow_list_tables_fn callback) {
	duckarrow::g_list_tables_callback = callback;
}

DUCKARROW_API void duckarrow_register_get_columns(duckarrow_get_columns_fn callback) {
	duckarrow::g_get_columns_callback = callback;
}

DUCKARROW_API void duckarrow_register_free(duckarrow_free_fn callback) {
	duckarrow::g_free_callback = callback;
}

DUCKARROW_API void duckarrow_register_scan_table_bind(duckarrow_scan_table_bind_fn callback) {
	duckarrow::g_scan_table_bind_callback = callback;
}

DUCKARROW_API void duckarrow_register_scan_get_column(duckarrow_scan_get_column_fn callback) {
	duckarrow::g_scan_get_column_callback = callback;
}

DUCKARROW_API void duckarrow_register_scan_init(duckarrow_scan_init_fn callback) {
	duckarrow::g_scan_init_callback = callback;
}

DUCKARROW_API void duckarrow_register_scan_next(duckarrow_scan_next_fn callback) {
	duckarrow::g_scan_next_callback = callback;
}

DUCKARROW_API void duckarrow_register_scan_free(duckarrow_scan_free_fn callback) {
	duckarrow::g_scan_free_callback = callback;
}

DUCKARROW_API void duckarrow_free_schema_list(DuckArrowSchemaList *list) {
	if (!list) {
		return;
	}
	if (duckarrow::g_free_callback) {
		// Free each schema name
		for (size_t i = 0; i < list->count; i++) {
			if (list->schemas[i].schema_name) {
				duckarrow::g_free_callback((void *)list->schemas[i].schema_name);
			}
		}
		// Free the array
		if (list->schemas) {
			duckarrow::g_free_callback(list->schemas);
		}
		// Free the error message
		if (list->error) {
			duckarrow::g_free_callback((void *)list->error);
		}
	}
	// Zero out the struct
	list->schemas = nullptr;
	list->count = 0;
	list->error = nullptr;
}

DUCKARROW_API void duckarrow_free_table_list(DuckArrowTableList *list) {
	if (!list) {
		return;
	}
	if (duckarrow::g_free_callback) {
		// Free each table name and type
		for (size_t i = 0; i < list->count; i++) {
			if (list->tables[i].table_name) {
				duckarrow::g_free_callback((void *)list->tables[i].table_name);
			}
			if (list->tables[i].table_type) {
				duckarrow::g_free_callback((void *)list->tables[i].table_type);
			}
		}
		// Free the array
		if (list->tables) {
			duckarrow::g_free_callback(list->tables);
		}
		// Free the error message
		if (list->error) {
			duckarrow::g_free_callback((void *)list->error);
		}
	}
	// Zero out the struct
	list->tables = nullptr;
	list->count = 0;
	list->error = nullptr;
}

DUCKARROW_API void duckarrow_free_column_list(DuckArrowColumnList *list) {
	if (!list) {
		return;
	}
	if (duckarrow::g_free_callback) {
		// Free each column name and type
		for (size_t i = 0; i < list->count; i++) {
			if (list->columns[i].column_name) {
				duckarrow::g_free_callback((void *)list->columns[i].column_name);
			}
			if (list->columns[i].column_type) {
				duckarrow::g_free_callback((void *)list->columns[i].column_type);
			}
		}
		// Free the array
		if (list->columns) {
			duckarrow::g_free_callback(list->columns);
		}
		// Free the error message
		if (list->error) {
			duckarrow::g_free_callback((void *)list->error);
		}
	}
	// Zero out the struct
	list->columns = nullptr;
	list->count = 0;
	list->error = nullptr;
}

} // extern "C"
