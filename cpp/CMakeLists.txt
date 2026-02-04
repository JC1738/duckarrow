cmake_minimum_required(VERSION 3.15)
project(duckarrow_storage LANGUAGES CXX)

# C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find DuckDB headers from duckdb-go-api submodule (relative to this directory)
set(DUCKDB_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../duckdb-go-api")

# Verify DuckDB headers exist
if(NOT EXISTS "${DUCKDB_INCLUDE_DIR}/duckdb.h")
    message(FATAL_ERROR "DuckDB headers not found at ${DUCKDB_INCLUDE_DIR}. "
                        "Ensure duckdb-go-api submodule is initialized: "
                        "git submodule update --init")
endif()

# Include directories
include_directories(${DUCKDB_INCLUDE_DIR})

#===----------------------------------------------------------------------===//
# DuckDB Version Configuration
#===----------------------------------------------------------------------===//
# Parse DUCKDB_VERSION (e.g., "v1.4.3") and convert to numeric format:
# MAJOR*1000000 + MINOR*1000 + PATCH (e.g., v1.4.3 = 1004003)
# Default to v1.4.3 if not specified.
#===----------------------------------------------------------------------===//

if(NOT DEFINED DUCKDB_VERSION)
    set(DUCKDB_VERSION "v1.4.3")
    message(STATUS "DUCKDB_VERSION not specified, defaulting to ${DUCKDB_VERSION}")
endif()

# Strip leading 'v' if present and parse version components
string(REGEX REPLACE "^v" "" DUCKDB_VERSION_CLEAN "${DUCKDB_VERSION}")
string(REPLACE "." ";" DUCKDB_VERSION_LIST "${DUCKDB_VERSION_CLEAN}")

list(LENGTH DUCKDB_VERSION_LIST DUCKDB_VERSION_LEN)
if(DUCKDB_VERSION_LEN LESS 3)
    message(FATAL_ERROR "Invalid DUCKDB_VERSION format: ${DUCKDB_VERSION}. Expected vX.Y.Z or X.Y.Z")
endif()

list(GET DUCKDB_VERSION_LIST 0 DUCKDB_VERSION_MAJOR)
list(GET DUCKDB_VERSION_LIST 1 DUCKDB_VERSION_MINOR)
list(GET DUCKDB_VERSION_LIST 2 DUCKDB_VERSION_PATCH)

# Calculate numeric version: MAJOR*1000000 + MINOR*1000 + PATCH
math(EXPR DUCKARROW_DUCKDB_VERSION_NUM
    "${DUCKDB_VERSION_MAJOR} * 1000000 + ${DUCKDB_VERSION_MINOR} * 1000 + ${DUCKDB_VERSION_PATCH}")

message(STATUS "DuckDB version: ${DUCKDB_VERSION} -> ${DUCKARROW_DUCKDB_VERSION_NUM}")

# Collect source files
file(GLOB_RECURSE SOURCES "*.cpp")

# Create static library for linking into Go extension
add_library(duckarrow_storage STATIC ${SOURCES})

# Enable Position Independent Code (required for linking into shared library)
set_target_properties(duckarrow_storage PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Define DuckDB version for conditional compilation
target_compile_definitions(duckarrow_storage PRIVATE
    DUCKARROW_DUCKDB_VERSION=${DUCKARROW_DUCKDB_VERSION_NUM}
)

# Export symbols for Go callback registration
# On Unix, use visibility=default for exported symbols
if(UNIX)
    target_compile_options(duckarrow_storage PRIVATE -fvisibility=hidden)
    set_target_properties(duckarrow_storage PROPERTIES
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
endif()

# On Windows, use dllexport
if(WIN32)
    target_compile_definitions(duckarrow_storage PRIVATE DUCKARROW_STORAGE_EXPORTS)
endif()

# Static library output - no platform-specific settings needed
# The static library will be linked into the Go shared extension

# Install target
install(TARGETS duckarrow_storage
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
