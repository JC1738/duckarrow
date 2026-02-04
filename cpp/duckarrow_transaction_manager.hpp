//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_transaction_manager.hpp
//
//===----------------------------------------------------------------------===//
//
// This file defines the DuckArrowTransactionManager class which handles
// transactions for the DuckArrow catalog.
//
// Note: Flight SQL is stateless, so transaction support is minimal.
// Each query is essentially auto-committed on the server side.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckarrow_catalog.hpp"

namespace duckarrow {

class DuckArrowTransactionManager;

//===--------------------------------------------------------------------===//
// DuckArrowTransaction
//===--------------------------------------------------------------------===//

// Transaction object for DuckArrow catalogs.
// Since Flight SQL is stateless (each query is auto-committed on the server),
// this is a minimal implementation that satisfies DuckDB's transaction interface.
class DuckArrowTransaction : public duckdb::Transaction {
public:
	DuckArrowTransaction(DuckArrowTransactionManager &manager,
	                     duckdb::ClientContext &context,
	                     DuckArrowCatalog &catalog);
	~DuckArrowTransaction() override;

	// Get the catalog associated with this transaction
	DuckArrowCatalog &GetCatalog();

private:
	DuckArrowCatalog &catalog;
};

//===--------------------------------------------------------------------===//
// DuckArrowTransactionManager
//===--------------------------------------------------------------------===//

// Transaction manager for DuckArrow catalogs.
// Since Flight SQL is stateless, this provides minimal transaction support
// that allows DuckDB's query execution to proceed normally.
class DuckArrowTransactionManager : public duckdb::TransactionManager {
public:
	DuckArrowTransactionManager(duckdb::AttachedDatabase &db, DuckArrowCatalog &catalog);
	~DuckArrowTransactionManager() override;

	//===----------------------------------------------------------------===//
	// TransactionManager Interface
	//===----------------------------------------------------------------===//

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;

	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context,
	                                    duckdb::Transaction &transaction) override;

	void RollbackTransaction(duckdb::Transaction &transaction) override;

	void Checkpoint(duckdb::ClientContext &context, bool force) override;

private:
	DuckArrowCatalog &catalog;
	duckdb::mutex transaction_lock;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<DuckArrowTransaction>> transactions;
};

} // namespace duckarrow
