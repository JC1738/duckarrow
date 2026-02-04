//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_transaction_manager.cpp
//
//===----------------------------------------------------------------------===//
//
// Implementation of DuckArrowTransactionManager and DuckArrowTransaction.
//
// Note: Flight SQL is stateless, so transaction support is minimal.
// Each query is essentially auto-committed on the server side.
//
//===----------------------------------------------------------------------===//

#include "duckarrow_transaction_manager.hpp"

namespace duckarrow {

//===--------------------------------------------------------------------===//
// DuckArrowTransaction
//===--------------------------------------------------------------------===//

DuckArrowTransaction::DuckArrowTransaction(DuckArrowTransactionManager &manager,
                                           duckdb::ClientContext &context,
                                           DuckArrowCatalog &catalog)
    : duckdb::Transaction(manager, context), catalog(catalog) {
}

DuckArrowTransaction::~DuckArrowTransaction() {
}

DuckArrowCatalog &DuckArrowTransaction::GetCatalog() {
	return catalog;
}

//===--------------------------------------------------------------------===//
// DuckArrowTransactionManager
//===--------------------------------------------------------------------===//

DuckArrowTransactionManager::DuckArrowTransactionManager(duckdb::AttachedDatabase &db,
                                                         DuckArrowCatalog &catalog)
    : duckdb::TransactionManager(db), catalog(catalog), transaction_lock(), transactions() {
}

DuckArrowTransactionManager::~DuckArrowTransactionManager() {
}

duckdb::Transaction &
DuckArrowTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	auto transaction = duckdb::make_uniq<DuckArrowTransaction>(*this, context, catalog);
	auto &result = *transaction;
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

duckdb::ErrorData
DuckArrowTransactionManager::CommitTransaction(duckdb::ClientContext & /*context*/,
                                               duckdb::Transaction &transaction) {
	// Flight SQL is stateless - queries are auto-committed on the server.
	// We just need to clean up our transaction tracking.
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions.erase(transaction);
	return duckdb::ErrorData();
}

void
DuckArrowTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	// Flight SQL is stateless - there's nothing to roll back on the server.
	// We just need to clean up our transaction tracking.
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void
DuckArrowTransactionManager::Checkpoint(duckdb::ClientContext & /*context*/, bool /*force*/) {
	// No-op for DuckArrow - Flight SQL is read-only and stateless.
	// There is no local state to checkpoint.
}

} // namespace duckarrow
