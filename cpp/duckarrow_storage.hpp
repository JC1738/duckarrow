//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckarrow_storage.hpp
//
//===----------------------------------------------------------------------===//
//
// This file defines the DuckArrowStorageExtension class which allows DuckDB
// to attach Flight SQL servers using:
//   ATTACH 'grpc://host:port' AS db (TYPE duckarrow)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckarrow_compat.hpp"
#include "go_callbacks.h"

namespace duckarrow {

//===--------------------------------------------------------------------===//
// DuckArrowStorageExtension
//===--------------------------------------------------------------------===//

// Storage extension that enables attaching Arrow Flight SQL servers as
// external databases in DuckDB. This allows querying remote Flight SQL
// endpoints using standard DuckDB SQL syntax.
class DuckArrowStorageExtension : public duckdb::StorageExtension {
public:
	DuckArrowStorageExtension();
};

} // namespace duckarrow
