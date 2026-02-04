//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_schema.cpp
//
//===----------------------------------------------------------------------===//
//
// Implementation of DuckArrowSchemaEntry for table discovery and lookup
// within a schema from a Flight SQL server.
//
//===----------------------------------------------------------------------===//

#include "duckarrow_schema.hpp"
#include "duckarrow_catalog.hpp"
#include "duckarrow_table.hpp"
#include <mutex>

namespace duckarrow {

//===--------------------------------------------------------------------===//
// External Callback References
//===--------------------------------------------------------------------===//

// These are defined in duckarrow_catalog.cpp and set by Go
extern duckarrow_list_tables_fn g_list_tables_callback;
extern duckarrow_get_columns_fn g_get_columns_callback;

//===--------------------------------------------------------------------===//
// DuckArrowSchemaEntry Constructor
//===--------------------------------------------------------------------===//

DuckArrowSchemaEntry::DuckArrowSchemaEntry(duckdb::Catalog &catalog,
                                           duckdb::CreateSchemaInfo &info,
                                           DuckArrowConnectionHandle handle)
    : duckdb::SchemaCatalogEntry(catalog, info), connection_handle(handle) {
}

//===--------------------------------------------------------------------===//
// Schema API Implementation
//===--------------------------------------------------------------------===//

void DuckArrowSchemaEntry::Scan(duckdb::ClientContext & /* context */,
                                duckdb::CatalogType type,
                                const std::function<void(duckdb::CatalogEntry &)> & /* callback */) {
	// Only support scanning tables for now
	if (type != duckdb::CatalogType::TABLE_ENTRY) {
		return;
	}

	if (!g_list_tables_callback || !connection_handle) {
		return;
	}

	// Get tables from Flight SQL via Go callback
	// name is the schema name (inherited from CatalogEntry)
	DuckArrowTableList table_list = g_list_tables_callback(
	    connection_handle,
	    nullptr,  // catalog (use default)
	    name.c_str()  // schema name
	);

	if (table_list.error) {
		duckdb::string error_msg(table_list.error);
		duckarrow_free_table_list(&table_list);
		throw duckdb::IOException("Failed to list tables from Flight SQL: %s", error_msg);
	}

	// Note: Full Scan requires DuckArrowTableEntry objects.
	// For now, we clean up and return (tables will be accessed via LookupEntry).
	// When DuckArrowTableEntry is available, we'll iterate and call callback for each.
	duckarrow_free_table_list(&table_list);
}

void DuckArrowSchemaEntry::Scan(duckdb::CatalogType /* type */,
                                const std::function<void(duckdb::CatalogEntry &)> & /* callback */) {
	throw duckdb::NotImplementedException("DuckArrowSchemaEntry::Scan without context not supported");
}

// Helper function to look up or create an entry (shared by GetEntry/LookupEntry)
duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::GetOrCreateEntry(duckdb::CatalogType type,
                                       const duckdb::string &entry_name) {
	// Only support looking up tables for now
	if (type != duckdb::CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	auto table_name = entry_name;

	// Check cache first (with lock)
	{
		std::lock_guard<std::mutex> guard(table_cache_lock);
		auto it = table_cache.find(table_name);
		if (it != table_cache.end()) {
			return it->second.get();
		}
	}

	// Not in cache - create from Flight SQL
	// name is the schema name (inherited from CatalogEntry)
	auto table_entry = DuckArrowTableEntry::CreateFromFlightSQL(
	    catalog, *this, connection_handle, name, table_name);

	if (!table_entry) {
		// Table doesn't exist or error occurred
		return nullptr;
	}

	// Cache the result (with lock)
	std::lock_guard<std::mutex> guard(table_cache_lock);
	auto *raw_ptr = table_entry.get();
	table_cache[table_name] = std::move(table_entry);
	return raw_ptr;
}

#if DUCKARROW_DUCKDB_VERSION_AT_LEAST(1, 4, 0)
// v1.4.0+: LookupEntry with EntryLookupInfo
duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::LookupEntry(duckdb::CatalogTransaction /* transaction */,
                                  const duckdb::EntryLookupInfo &lookup_info) {
	return GetOrCreateEntry(lookup_info.GetCatalogType(), lookup_info.GetEntryName());
}
#else
// v1.2.0: GetEntry with explicit type and name
duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::GetEntry(duckdb::CatalogTransaction /* transaction */,
                               duckdb::CatalogType type,
                               const duckdb::string &entry_name) {
	return GetOrCreateEntry(type, entry_name);
}
#endif

//===--------------------------------------------------------------------===//
// Create Operations (not supported - Flight SQL is read-only)
//===--------------------------------------------------------------------===//

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateTable(duckdb::CatalogTransaction /* transaction */,
                                  duckdb::BoundCreateTableInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE TABLE - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateView(duckdb::CatalogTransaction /* transaction */,
                                 duckdb::CreateViewInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE VIEW - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateIndex(duckdb::CatalogTransaction /* transaction */,
                                  duckdb::CreateIndexInfo & /* info */,
                                  duckdb::TableCatalogEntry & /* table */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE INDEX - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateFunction(duckdb::CatalogTransaction /* transaction */,
                                     duckdb::CreateFunctionInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE FUNCTION - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateSequence(duckdb::CatalogTransaction /* transaction */,
                                     duckdb::CreateSequenceInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE SEQUENCE - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction /* transaction */,
                                          duckdb::CreateTableFunctionInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE TABLE FUNCTION - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction /* transaction */,
                                         duckdb::CreateCopyFunctionInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE COPY FUNCTION - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreatePragmaFunction(duckdb::CatalogTransaction /* transaction */,
                                           duckdb::CreatePragmaFunctionInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE PRAGMA FUNCTION - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateCollation(duckdb::CatalogTransaction /* transaction */,
                                      duckdb::CreateCollationInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE COLLATION - Flight SQL is read-only");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
DuckArrowSchemaEntry::CreateType(duckdb::CatalogTransaction /* transaction */,
                                 duckdb::CreateTypeInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support CREATE TYPE - Flight SQL is read-only");
}

//===--------------------------------------------------------------------===//
// Modify Operations (not supported - Flight SQL is read-only)
//===--------------------------------------------------------------------===//

void DuckArrowSchemaEntry::DropEntry(duckdb::ClientContext & /* context */,
                                     duckdb::DropInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support DROP - Flight SQL is read-only");
}

void DuckArrowSchemaEntry::Alter(duckdb::CatalogTransaction /* transaction */,
                                 duckdb::AlterInfo & /* info */) {
	throw duckdb::NotImplementedException("DuckArrow does not support ALTER - Flight SQL is read-only");
}

//===--------------------------------------------------------------------===//
// DuckArrow-specific Methods
//===--------------------------------------------------------------------===//

DuckArrowConnectionHandle DuckArrowSchemaEntry::GetConnectionHandle() const {
	std::lock_guard<std::mutex> guard(table_cache_lock);
	return connection_handle;
}

} // namespace duckarrow
