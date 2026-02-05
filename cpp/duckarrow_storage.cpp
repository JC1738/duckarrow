//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_storage.cpp
//
//===----------------------------------------------------------------------===//
//
// Implementation of DuckArrowStorageExtension for attaching Flight SQL servers.
//
//===----------------------------------------------------------------------===//

#include "duckarrow_storage.hpp"
#include "duckarrow_catalog.hpp"
#include "duckarrow_transaction_manager.hpp"

namespace duckarrow {

// Declared in duckarrow_catalog.cpp
extern duckarrow_connect_fn g_connect_callback;
extern duckarrow_free_fn g_free_callback;

//===--------------------------------------------------------------------===//
// Attach Callback
//===--------------------------------------------------------------------===//

// Called when user executes: ATTACH 'grpc://host:port' AS db (TYPE duckarrow)
// Creates a DuckArrowCatalog for the attached Flight SQL database.
// Establishes a Flight SQL connection via Go callback and associates it with the catalog.
static duckdb::unique_ptr<duckdb::Catalog>
DuckArrowAttach(duckdb::StorageExtensionInfo * /* storage_info */,
                duckdb::ClientContext & /* context */,
                duckdb::AttachedDatabase &db,
                const duckdb::string & /* name */,
                duckdb::AttachInfo &info,
                duckdb::AccessMode access_mode) {
	// Extract the URI from AttachInfo.path
	// Expected format: grpc://host:port or grpc+tls://host:port
	duckdb::string uri = info.path;

	// Create catalog options from attach info
	DuckArrowOptions options;
	options.uri = uri;  // Copy, don't move - we need it later
	options.access_mode = access_mode;

	// Parse any additional options from AttachInfo
	for (auto &entry : info.options) {
		auto key = duckdb::StringUtil::Lower(entry.first);
		if (key == "username" || key == "user") {
			options.username = entry.second.GetValue<duckdb::string>();
		} else if (key == "password") {
			options.password = entry.second.GetValue<duckdb::string>();
		} else if (key == "token") {
			options.token = entry.second.GetValue<duckdb::string>();
		}
	}

	// Establish connection via Go callback before creating catalog
	// (so we can fail early if connection fails)
	DuckArrowConnectionHandle connection_handle = nullptr;
	if (g_connect_callback) {
		const char *uri_cstr = options.uri.c_str();
		const char *username_cstr = options.username.empty() ? nullptr : options.username.c_str();
		const char *password_cstr = options.password.empty() ? nullptr : options.password.c_str();
		const char *token_cstr = options.token.empty() ? nullptr : options.token.c_str();

		DuckArrowConnectResult result = g_connect_callback(uri_cstr, username_cstr, password_cstr, token_cstr);

		if (result.error) {
			duckdb::string error_msg(result.error);
			// Free the error string allocated by Go
			if (g_free_callback) {
				g_free_callback((void *)result.error);
			}
			throw duckdb::IOException("Failed to connect to Flight SQL server '%s': %s",
			                          options.uri, error_msg);
		}

		connection_handle = result.handle;
	}

	// Create the catalog and set the connection handle
	auto catalog = duckdb::make_uniq<DuckArrowCatalog>(db, options);
	if (connection_handle) {
		catalog->SetConnectionHandle(connection_handle);
	}

	return catalog;
}

//===--------------------------------------------------------------------===//
// Transaction Manager Callback
//===--------------------------------------------------------------------===//

// Creates the transaction manager for the DuckArrow catalog.
// Flight SQL is stateless, so transactions are minimal.
static duckdb::unique_ptr<duckdb::TransactionManager>
DuckArrowCreateTransactionManager(duckdb::StorageExtensionInfo * /* storage_info */,
                                  duckdb::AttachedDatabase &db,
                                  duckdb::Catalog &catalog) {
	auto &duckarrow_catalog = catalog.Cast<DuckArrowCatalog>();
	return duckdb::make_uniq<DuckArrowTransactionManager>(db, duckarrow_catalog);
}

//===--------------------------------------------------------------------===//
// DuckArrowStorageExtension Constructor
//===--------------------------------------------------------------------===//

DuckArrowStorageExtension::DuckArrowStorageExtension() {
	attach = DuckArrowAttach;
	create_transaction_manager = DuckArrowCreateTransactionManager;
}

} // namespace duckarrow

//===----------------------------------------------------------------------===//
// C API Registration Function
//===----------------------------------------------------------------------===//
//
// This function registers the DuckArrow storage extension with DuckDB.
// It must be called from Go during extension initialization.
//
// The duckdb_database handle is an opaque pointer to DuckDB's internal
// DatabaseData structure. We need to access the underlying DuckDB instance
// to register the storage extension.
//
//===----------------------------------------------------------------------===//

// Internal structure that mirrors DuckDB's C API database wrapper.
// This structure must match the layout in DuckDB's capi/capi_internal.hpp.
// Note: This is accessing DuckDB internals and may need updates for new versions.
struct DatabaseData {
	duckdb::unique_ptr<duckdb::DuckDB> database;
};

extern "C" {

// duckarrow_register_storage_extension registers the DuckArrow storage extension
// with a DuckDB database instance. This enables the ATTACH ... (TYPE duckarrow) syntax.
//
// Parameters:
//   db_handle: The DuckDB database handle from the C API (duckdb_database)
//
// Returns:
//   true if registration succeeded, false otherwise
//
// Thread safety: This function should be called once during extension initialization.
// It is not safe to call concurrently from multiple threads.
DUCKARROW_API bool duckarrow_register_storage_extension(void *db_handle) {
	// Storage extension registration requires DBConfig::GetConfig which is an internal
	// DuckDB API not exported from DuckDB's binary. This functionality requires either:
	// 1. Building DuckDB from source with the extension compiled in
	// 2. DuckDB adding a C API for registering storage extensions
	//
	// For now, this always returns false. The extension still works via table functions
	// like duckarrow_query(), duckarrow_schemas(), duckarrow_tables(), etc.
	//
	// TODO: Re-enable when DuckDB exports storage extension registration API
	(void)db_handle; // Suppress unused parameter warning
	return false;

	// Original implementation (requires internal DuckDB API):
	//
	// if (db_handle == nullptr) {
	//     return false;
	// }
	// try {
	//     auto *db_data = static_cast<DatabaseData *>(db_handle);
	//     if (!db_data->database) {
	//         return false;
	//     }
	//     auto &db = *db_data->database;
	//     auto &config = duckdb::DBConfig::GetConfig(*db.instance);
	//     config.storage_extensions["duckarrow"] =
	//         duckdb::make_uniq<duckarrow::DuckArrowStorageExtension>();
	//     return true;
	// } catch (...) {
	//     return false;
	// }
}

} // extern "C"
