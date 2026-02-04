//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_table.hpp
//
//===----------------------------------------------------------------------===//
//
// This file defines the DuckArrowTableEntry class which represents a table
// from a Flight SQL server within an attached DuckArrow database.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckarrow_compat.hpp"
#include "go_callbacks.h"

namespace duckarrow {

//===--------------------------------------------------------------------===//
// DuckArrowTableEntry
//===--------------------------------------------------------------------===//

// TableCatalogEntry implementation for Flight SQL tables.
// Represents a table from a remote Flight SQL server that can be queried
// via the duckarrow_query table function.
class DuckArrowTableEntry : public duckdb::TableCatalogEntry {
public:
	DuckArrowTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	                    duckdb::CreateTableInfo &info, DuckArrowConnectionHandle connection_handle,
	                    const duckdb::string &schema_name, const duckdb::string &table_name);
	~DuckArrowTableEntry() override = default;

	//===----------------------------------------------------------------===//
	// TableCatalogEntry API
	//===----------------------------------------------------------------===//

	// Get statistics for a column (not supported for Flight SQL)
	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                          duckdb::column_t column_id) override;

	// Get the table function for scanning this table
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                       duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;

	// Get storage information (minimal for Flight SQL)
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	//===----------------------------------------------------------------===//
	// DuckArrow-specific Methods
	//===----------------------------------------------------------------===//

	// Get the connection handle
	DuckArrowConnectionHandle GetConnectionHandle() const;

	// Get the schema name on the remote server
	const duckdb::string &GetRemoteSchemaName() const;

	// Get the table name on the remote server
	const duckdb::string &GetRemoteTableName() const;

	//===----------------------------------------------------------------===//
	// Static Factory Method
	//===----------------------------------------------------------------===//

	// Create a DuckArrowTableEntry by querying column metadata from Flight SQL
	// Returns nullptr if the table doesn't exist or an error occurs
	static duckdb::unique_ptr<DuckArrowTableEntry> CreateFromFlightSQL(
	    duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema,
	    DuckArrowConnectionHandle connection_handle, const duckdb::string &schema_name,
	    const duckdb::string &table_name);

private:
	// Connection handle for Flight SQL queries
	DuckArrowConnectionHandle connection_handle;

	// Schema and table name on the remote server
	duckdb::string remote_schema_name;
	duckdb::string remote_table_name;

	// Disable copy
	DuckArrowTableEntry(const DuckArrowTableEntry &) = delete;
	DuckArrowTableEntry &operator=(const DuckArrowTableEntry &) = delete;
};

//===--------------------------------------------------------------------===//
// Type Conversion Utilities
//===--------------------------------------------------------------------===//

// Convert a Flight SQL column type string to a DuckDB LogicalType
duckdb::LogicalType FlightSQLTypeToDuckDB(const duckdb::string &type_str);

} // namespace duckarrow
