//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_catalog.hpp
//
//===----------------------------------------------------------------------===//
//
// This file defines the DuckArrowCatalog class which provides catalog metadata
// for an attached Flight SQL server.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckarrow_compat.hpp"
#include "go_callbacks.h"
#include <mutex>
#include <unordered_map>

namespace duckarrow {

// Forward declaration
class DuckArrowSchemaEntry;

//===--------------------------------------------------------------------===//
// DuckArrowOptions
//===--------------------------------------------------------------------===//

// Configuration options for DuckArrow catalog/connection
struct DuckArrowOptions {
	// Flight SQL server URI (e.g., grpc://host:port or grpc+tls://host:port)
	duckdb::string uri;

	// Access mode (READ_ONLY or READ_WRITE)
	duckdb::AccessMode access_mode = duckdb::AccessMode::READ_ONLY;

	// Optional authentication credentials
	duckdb::string username;
	duckdb::string password;
	duckdb::string token;
};

//===--------------------------------------------------------------------===//
// DuckArrowCatalog
//===--------------------------------------------------------------------===//

// Catalog implementation for Flight SQL servers.
// Provides schema, table, and column metadata by querying the Flight SQL
// metadata endpoints via Go callbacks.
class DuckArrowCatalog : public duckdb::Catalog {
public:
	DuckArrowCatalog(duckdb::AttachedDatabase &db, DuckArrowOptions options);
	~DuckArrowCatalog() override;

	//===----------------------------------------------------------------===//
	// Catalog Interface
	//===----------------------------------------------------------------===//

	duckdb::string GetCatalogType() override;

	void Initialize(bool load_builtin) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateSchema(duckdb::CatalogTransaction transaction,
	             duckdb::CreateSchemaInfo &info) override;

	void DropSchema(duckdb::ClientContext &context,
	                duckdb::DropInfo &info) override;

	void ScanSchemas(duckdb::ClientContext &context,
	                 std::function<void(duckdb::SchemaCatalogEntry &)> callback) override;

#if DUCKARROW_DUCKDB_VERSION_AT_LEAST(1, 4, 0)
	// v1.4.0+: LookupSchema replaces GetSchema with EntryLookupInfo parameter
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
	LookupSchema(duckdb::CatalogTransaction transaction,
	             const duckdb::EntryLookupInfo &schema_lookup,
	             duckdb::OnEntryNotFound if_not_found) override;
#else
	// v1.2.0: GetSchema with string schema name
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
	GetSchema(duckdb::CatalogTransaction transaction,
	          const duckdb::string &schema_name,
	          duckdb::OnEntryNotFound if_not_found,
	          duckdb::QueryErrorContext error_context) override;
#endif

#if DUCKARROW_DUCKDB_VERSION_AT_LEAST(1, 4, 0)
	// v1.4.0+: Plan* methods return PhysicalOperator& and take PhysicalPlanGenerator
	duckdb::PhysicalOperator &
	PlanCreateTableAs(duckdb::ClientContext &context,
	                  duckdb::PhysicalPlanGenerator &planner,
	                  duckdb::LogicalCreateTable &op,
	                  duckdb::PhysicalOperator &plan) override;

	duckdb::PhysicalOperator &
	PlanInsert(duckdb::ClientContext &context,
	           duckdb::PhysicalPlanGenerator &planner,
	           duckdb::LogicalInsert &op,
	           duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;

	duckdb::PhysicalOperator &
	PlanDelete(duckdb::ClientContext &context,
	           duckdb::PhysicalPlanGenerator &planner,
	           duckdb::LogicalDelete &op,
	           duckdb::PhysicalOperator &plan) override;

	duckdb::PhysicalOperator &
	PlanUpdate(duckdb::ClientContext &context,
	           duckdb::PhysicalPlanGenerator &planner,
	           duckdb::LogicalUpdate &op,
	           duckdb::PhysicalOperator &plan) override;
#else
	// v1.2.0: Plan* methods return unique_ptr and don't take PhysicalPlanGenerator
	duckdb::unique_ptr<duckdb::PhysicalOperator>
	PlanCreateTableAs(duckdb::ClientContext &context,
	                  duckdb::LogicalCreateTable &op,
	                  duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

	duckdb::unique_ptr<duckdb::PhysicalOperator>
	PlanInsert(duckdb::ClientContext &context,
	           duckdb::LogicalInsert &op,
	           duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

	duckdb::unique_ptr<duckdb::PhysicalOperator>
	PlanDelete(duckdb::ClientContext &context,
	           duckdb::LogicalDelete &op,
	           duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

	duckdb::unique_ptr<duckdb::PhysicalOperator>
	PlanUpdate(duckdb::ClientContext &context,
	           duckdb::LogicalUpdate &op,
	           duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;
#endif

	duckdb::unique_ptr<duckdb::LogicalOperator>
	BindCreateIndex(duckdb::Binder &binder,
	                duckdb::CreateStatement &stmt,
	                duckdb::TableCatalogEntry &table,
	                duckdb::unique_ptr<duckdb::LogicalOperator> plan) override;

	duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext &context) override;

	bool InMemory() override;

	duckdb::string GetDBPath() override;

	//===----------------------------------------------------------------===//
	// DuckArrow-specific Methods
	//===----------------------------------------------------------------===//

	// Get the connection options
	const DuckArrowOptions &GetOptions() const;

	// Get the Flight SQL URI
	const duckdb::string &GetURI() const;

	// Set the connection handle (called by Go after establishing connection)
	void SetConnectionHandle(DuckArrowConnectionHandle handle);

	// Get the connection handle
	DuckArrowConnectionHandle GetConnectionHandle() const;

private:
	// Helper function to get or create schema entry (used by GetSchema/LookupSchema)
	duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
	GetOrCreateSchemaEntry(const duckdb::string &schema_name,
	                       duckdb::OnEntryNotFound if_not_found);

	DuckArrowOptions options;
	DuckArrowConnectionHandle connection_handle;

	// Schema cache with mutex protection for thread safety
	mutable std::mutex schema_cache_lock;
	std::unordered_map<duckdb::string, duckdb::unique_ptr<DuckArrowSchemaEntry>> schema_cache;
};

} // namespace duckarrow
