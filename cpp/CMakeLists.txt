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

# Collect source files
file(GLOB_RECURSE SOURCES "*.cpp")

# Create shared library for the storage extension layer
add_library(duckarrow_storage SHARED ${SOURCES})

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

# Platform-specific output settings
if(APPLE)
    set_target_properties(duckarrow_storage PROPERTIES
        SUFFIX ".dylib"
        INSTALL_RPATH "@loader_path"
    )
elseif(UNIX)
    set_target_properties(duckarrow_storage PROPERTIES
        SUFFIX ".so"
        INSTALL_RPATH "$ORIGIN"
    )
endif()

# Install target
install(TARGETS duckarrow_storage
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
