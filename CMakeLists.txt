cmake_minimum_required(VERSION 3.12)
project(duckarrow)

set(EXTENSION_NAME duckarrow)

# Check for required tools
find_program(GO_EXECUTABLE go REQUIRED)
find_program(MAKE_EXECUTABLE make REQUIRED)

if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR "Go compiler not found. Please install Go: https://go.dev/dl/")
endif()

if(NOT MAKE_EXECUTABLE)
    message(FATAL_ERROR "Make not found. Please install make.")
endif()

message(STATUS "Found Go: ${GO_EXECUTABLE}")
message(STATUS "Found Make: ${MAKE_EXECUTABLE}")

# Platform detection for DuckDB
if(WIN32)
    # Windows ARM64 detection
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "ARM64|aarch64|arm64")
        set(DUCKDB_PLATFORM "windows_arm64")
        set(MAKE_GOARCH "arm64")
    else()
        set(DUCKDB_PLATFORM "windows_amd64")
        set(MAKE_GOARCH "amd64")
    endif()
    set(MAKE_GOOS "windows")
elseif(APPLE)
    # macOS ARM64 detection (Apple Silicon)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64|ARM64")
        set(DUCKDB_PLATFORM "osx_arm64")
        set(MAKE_GOARCH "arm64")
    else()
        set(DUCKDB_PLATFORM "osx_amd64")
        set(MAKE_GOARCH "amd64")
    endif()
    set(MAKE_GOOS "darwin")
else()
    # Linux/Unix ARM64 detection (covers aarch64, arm64, armv8, ARM64)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|armv8|ARM64")
        set(DUCKDB_PLATFORM "linux_arm64")
        set(MAKE_GOARCH "arm64")
    else()
        set(DUCKDB_PLATFORM "linux_amd64")
        set(MAKE_GOARCH "amd64")
    endif()
    set(MAKE_GOOS "linux")
endif()

message(STATUS "Building for platform: ${DUCKDB_PLATFORM}")
message(STATUS "GOOS=${MAKE_GOOS}, GOARCH=${MAKE_GOARCH}")

# Custom target that invokes the Go/Make build
add_custom_target(${EXTENSION_NAME}_extension ALL
    COMMAND ${CMAKE_COMMAND} -E env
        GOOS=${MAKE_GOOS}
        GOARCH=${MAKE_GOARCH}
        ${MAKE_EXECUTABLE} build
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building Go-based DuckDB extension for ${DUCKDB_PLATFORM}"
)
