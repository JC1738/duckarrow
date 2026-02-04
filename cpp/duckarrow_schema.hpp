//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_schema.hpp
//
//===----------------------------------------------------------------------===//
//
// This file defines the DuckArrowSchemaEntry class which handles table lookups
// and enumeration within a schema from a Flight SQL server.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckarrow_compat.hpp"
#include "go_callbacks.h"
#include <mutex>

namespace duckarrow {

// Forward declaration
class DuckArrowCatalog;

//===--------------------------------------------------------------------===//
// DuckArrowSchemaEntry
//===--------------------------------------------------------------------===//

// SchemaCatalogEntry implementation for Flight SQL schemas.
// Provides table discovery and lookup by querying Flight SQL metadata.
class DuckArrowSchemaEntry : public duckdb::SchemaCatalogEntry {
public:
	DuckArrowSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info,
	                     DuckArrowConnectionHandle connection_handle);
	~DuckArrowSchemaEntry() override = default;

	//===----------------------------------------------------------------===//
	// Schema API
	//===----------------------------------------------------------------===//

	// Scan all entries of a given type in this schema
	void Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
	          const std::function<void(duckdb::CatalogEntry &)> &callback) override;

	// Scan without context (throws NotImplementedException)
	void Scan(duckdb::CatalogType type,
	          const std::function<void(duckdb::CatalogEntry &)> &callback) override;

	// Get a specific entry (table, view, etc.) by name
	duckdb::optional_ptr<duckdb::CatalogEntry>
	GetEntry(duckdb::CatalogTransaction transaction,
	         duckdb::CatalogType type,
	         const duckdb::string &name) override;

	//===----------------------------------------------------------------===//
	// Create Operations (not supported - Flight SQL is read-only)
	//===----------------------------------------------------------------===//

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateTable(duckdb::CatalogTransaction transaction,
	            duckdb::BoundCreateTableInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateView(duckdb::CatalogTransaction transaction,
	           duckdb::CreateViewInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateIndex(duckdb::CatalogTransaction transaction,
	            duckdb::CreateIndexInfo &info,
	            duckdb::TableCatalogEntry &table) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateFunction(duckdb::CatalogTransaction transaction,
	               duckdb::CreateFunctionInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateSequence(duckdb::CatalogTransaction transaction,
	               duckdb::CreateSequenceInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateTableFunction(duckdb::CatalogTransaction transaction,
	                    duckdb::CreateTableFunctionInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateCopyFunction(duckdb::CatalogTransaction transaction,
	                   duckdb::CreateCopyFunctionInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreatePragmaFunction(duckdb::CatalogTransaction transaction,
	                     duckdb::CreatePragmaFunctionInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateCollation(duckdb::CatalogTransaction transaction,
	                duckdb::CreateCollationInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateType(duckdb::CatalogTransaction transaction,
	           duckdb::CreateTypeInfo &info) override;

	//===----------------------------------------------------------------===//
	// Modify Operations (not supported - Flight SQL is read-only)
	//===----------------------------------------------------------------===//

	void DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

	void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) override;

	//===----------------------------------------------------------------===//
	// DuckArrow-specific Methods
	//===----------------------------------------------------------------===//

	// Get the connection handle for this schema
	DuckArrowConnectionHandle GetConnectionHandle() const;

private:
	// Connection handle for Flight SQL queries
	DuckArrowConnectionHandle connection_handle;

	// Table cache with mutex protection for thread safety
	mutable std::mutex table_cache_lock;
	mutable duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> table_cache;

	// Disable copy
	DuckArrowSchemaEntry(const DuckArrowSchemaEntry &) = delete;
	DuckArrowSchemaEntry &operator=(const DuckArrowSchemaEntry &) = delete;
};

} // namespace duckarrow
