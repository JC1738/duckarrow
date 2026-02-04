//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_compat.hpp
//
//===----------------------------------------------------------------------===//
//
// DuckDB v1.2.0 compatibility definitions.
// These structs are forward-declared in the bundled duckdb.hpp but not defined.
// We provide minimal definitions here for storage extension compatibility.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// DatabaseSize - storage size information
//===--------------------------------------------------------------------===//
// Forward declared in duckdb.hpp but not defined in the bundled header.
// Required for Catalog::GetDatabaseSize() override.

struct DatabaseSize {
	idx_t total_blocks = 0;
	idx_t block_size = 0;
	idx_t free_blocks = 0;
	idx_t used_blocks = 0;
	idx_t bytes = 0;
	idx_t wal_size = 0;
};

//===--------------------------------------------------------------------===//
// CreateSchemaInfo - schema creation metadata
//===--------------------------------------------------------------------===//
// Forward declared in duckdb.hpp but not defined in the bundled header.
// Required for SchemaCatalogEntry constructor.
// Inherits from CreateInfo which IS defined in duckdb.hpp.

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA_ENTRY) {
	}

	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_uniq<CreateSchemaInfo>();
		CopyProperties(*result);
		return std::move(result);
	}

	string ToString() const override {
		return "CREATE SCHEMA " + schema;
	}
};

//===--------------------------------------------------------------------===//
// CreateTableInfo - table creation metadata
//===--------------------------------------------------------------------===//
// Forward declared in duckdb.hpp but not defined in the bundled header.

struct CreateTableInfo : public CreateInfo {
	CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY) {
	}

	CreateTableInfo(string catalog_p, string schema_p, string name_p)
	    : CreateInfo(CatalogType::TABLE_ENTRY, std::move(schema_p), std::move(catalog_p)), table(std::move(name_p)) {
	}

	string table;
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;

	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_uniq<CreateTableInfo>();
		CopyProperties(*result);
		result->table = table;
		result->columns = columns.Copy();
		for (auto &constraint : constraints) {
			result->constraints.push_back(constraint->Copy());
		}
		return std::move(result);
	}

	string ToString() const override {
		return "CREATE TABLE " + table;
	}
};

//===--------------------------------------------------------------------===//
// IndexInfo - index metadata
//===--------------------------------------------------------------------===//
// Not defined in bundled header but needed for TableStorageInfo.

struct IndexInfo {
	bool is_unique = false;
	bool is_primary = false;
	bool is_foreign = false;
	string index_name;
};

//===--------------------------------------------------------------------===//
// TableStorageInfo - table storage metadata
//===--------------------------------------------------------------------===//
// Forward declared in duckdb.hpp but not defined in the bundled header.

class TableStorageInfo {
public:
	optional_idx cardinality;
	vector<IndexInfo> index_info;
};

//===--------------------------------------------------------------------===//
// TableCatalogEntry - base class for table entries
//===--------------------------------------------------------------------===//
// Forward declared in duckdb.hpp but not defined in the bundled header.
// Required for subclassing to create custom table entries.

class TableCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TABLE_ENTRY;
	static constexpr const char *Name = "table";

	TableCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateTableInfo &info)
	    : StandardEntry(CatalogType::TABLE_ENTRY, schema_p, catalog_p, info.table),
	      columns(info.columns.Copy()) {
		for (auto &constraint : info.constraints) {
			constraints.push_back(constraint->Copy());
		}
	}

	~TableCatalogEntry() override = default;

	const ColumnList &GetColumns() const {
		return columns;
	}

	const vector<unique_ptr<Constraint>> &GetConstraints() const {
		return constraints;
	}

	vector<LogicalType> GetTypes() const {
		return columns.GetColumnTypes();
	}

	// Pure virtual methods that must be implemented by subclasses
	virtual unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) = 0;
	virtual TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) = 0;
	virtual TableStorageInfo GetStorageInfo(ClientContext &context) = 0;

protected:
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;
};

//===--------------------------------------------------------------------===//
// NOTE: AttachInfo IS defined in duckdb.hpp, no need to redefine it here.
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// StorageExtensionInfo - storage extension static info
//===--------------------------------------------------------------------===//
// Forward declared but not defined in the bundled header.

struct StorageExtensionInfo {
	virtual ~StorageExtensionInfo() {
	}
};

//===--------------------------------------------------------------------===//
// CheckpointOptions - checkpoint configuration
//===--------------------------------------------------------------------===//
// Used by StorageExtension checkpoint callbacks.

struct CheckpointOptions {
	bool force = false;
};

//===--------------------------------------------------------------------===//
// StorageExtension - custom storage backend
//===--------------------------------------------------------------------===//
// Forward declared but not defined in the bundled header.
// Required for implementing custom storage extensions.

class StorageExtension {
public:
	// Function pointer types
	using attach_function_t = unique_ptr<Catalog> (*)(StorageExtensionInfo *storage_info,
	                                                  ClientContext &context,
	                                                  AttachedDatabase &db,
	                                                  const string &name,
	                                                  AttachInfo &info,
	                                                  AccessMode access_mode);

	using create_transaction_manager_t = unique_ptr<TransactionManager> (*)(StorageExtensionInfo *storage_info,
	                                                                        AttachedDatabase &db,
	                                                                        Catalog &catalog);

	// Callback function pointers
	attach_function_t attach = nullptr;
	create_transaction_manager_t create_transaction_manager = nullptr;

	// Additional info passed to the storage functions
	shared_ptr<StorageExtensionInfo> storage_info;

	virtual ~StorageExtension() {
	}

	// Checkpoint lifecycle hooks (optional overrides)
	virtual void OnCheckpointStart(AttachedDatabase & /* db */, CheckpointOptions /* options */) {
	}
	virtual void OnCheckpointEnd(AttachedDatabase & /* db */, CheckpointOptions /* options */) {
	}
};

} // namespace duckdb
