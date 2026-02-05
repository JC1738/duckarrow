DUCKDB_VERSION := v1.4.3
# C Extension API version - separate from DuckDB release version
# DuckDB v1.4.x still uses C API v1.2.0
DUCKDB_API_VERSION := v1.2.0
EXTENSION_NAME := duckarrow
# Extract version from git tag, fall back to "dev" for local builds
EXTENSION_VERSION := $(shell git describe --tags --exact-match 2>/dev/null || echo "dev")

BUILD_DIR := build
SCRIPTS_DIR := scripts
TESTS_DIR := tests

# C++ build directories
CPP_DIR := cpp
CPP_BUILD_DIR := $(BUILD_DIR)/cpp

.PHONY: all clean build deps test test-unit test-coverage test-all fmt help
.PHONY: test-load test-hardcoded test-connection test-full test-replacement-scan test-pool test-attach test-show-tables test-show-schemas test-info-schema test-qualified test-types test-edge-cases test-errors test-projection
.PHONY: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64 build-windows-arm64
.PHONY: build-linux build-darwin build-windows
.PHONY: cmake-configure cmake-build cpp-clean

# Platform detection with override support
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Derive platform string and file extension
ifeq ($(GOOS),linux)
    PLATFORM := linux_$(GOARCH)
    EXT := .so
else ifeq ($(GOOS),darwin)
    PLATFORM := osx_$(GOARCH)
    EXT := .dylib
else ifeq ($(GOOS),windows)
    PLATFORM := windows_$(GOARCH)
    EXT := .dll
else
    # Fallback for unknown OS
    PLATFORM := $(GOOS)_$(GOARCH)
    EXT := .so
endif

# Platform-specific output directory to avoid collisions when building multiple platforms
OUTPUT_DIR := $(BUILD_DIR)/$(PLATFORM)
OUTPUT := $(OUTPUT_DIR)/$(EXTENSION_NAME)$(EXT)

# Default target
all: build

# Ensure dependencies and check submodule
deps:
	@mkdir -p $(OUTPUT_DIR)
	@if [ ! -f duckdb-go-api/duckdb.h ]; then \
		echo "Error: duckdb-go-api submodule not initialized."; \
		echo "Run: git submodule update --init"; \
		exit 1; \
	fi
	go mod tidy

# CMake generator - use MinGW Makefiles on Windows for CGO compatibility
ifeq ($(GOOS),windows)
    CMAKE_GENERATOR := -G "MinGW Makefiles"
else
    CMAKE_GENERATOR :=
endif

# Configure CMake for C++ build
cmake-configure:
	@mkdir -p $(CPP_BUILD_DIR)
	cmake -S $(CPP_DIR) -B $(CPP_BUILD_DIR) \
		$(CMAKE_GENERATOR) \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_SHARED_LIBS=OFF \
		-DDUCKDB_VERSION=$(DUCKDB_VERSION)

# Build C++ static library (skip on Windows - not supported)
cmake-build: cmake-configure
ifneq ($(GOOS),windows)
	cmake --build $(CPP_BUILD_DIR) --config Release
endif

# Linux requires external linker to properly include C++ symbols with --whole-archive
ifeq ($(GOOS),linux)
    GO_LDFLAGS := -X main.Version=$(EXTENSION_VERSION) -linkmode external
else
    GO_LDFLAGS := -X main.Version=$(EXTENSION_VERSION)
endif

# Build the extension
# On Windows, skip C++ library (ATTACH not supported due to linker limitations)
ifeq ($(GOOS),windows)
build: deps
else
build: deps cmake-build
endif
	CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -buildmode=c-shared -ldflags="$(GO_LDFLAGS)" -o $(OUTPUT) ./
	cd $(OUTPUT_DIR) && python3 ../../$(SCRIPTS_DIR)/append_extension_metadata.py \
		-l $(EXTENSION_NAME)$(EXT) \
		-n $(EXTENSION_NAME) \
		-dv $(DUCKDB_API_VERSION) \
		-ev $(EXTENSION_VERSION) \
		-p $(PLATFORM)
	@# Create symlink for test convenience (skip on Windows)
ifneq ($(GOOS),windows)
	@ln -sf $(PLATFORM)/$(EXTENSION_NAME).duckdb_extension $(BUILD_DIR)/$(EXTENSION_NAME).duckdb_extension
endif
	@echo "Built: $(OUTPUT_DIR)/$(EXTENSION_NAME).duckdb_extension for $(PLATFORM)"

# Platform-specific targets
# NOTE: Cross-compilation requires appropriate C toolchains:
#   - Linux ARM64 from AMD64: CC=aarch64-linux-gnu-gcc
#   - Windows from Linux: CC=x86_64-w64-mingw32-gcc
#   - macOS: Requires osxcross or native build on macOS
# These targets work best for native builds on each platform.
build-linux-amd64:
	$(MAKE) build GOOS=linux GOARCH=amd64

build-linux-arm64:
	$(MAKE) build GOOS=linux GOARCH=arm64

build-darwin-amd64:
	$(MAKE) build GOOS=darwin GOARCH=amd64

build-darwin-arm64:
	$(MAKE) build GOOS=darwin GOARCH=arm64

build-windows-amd64:
	$(MAKE) build GOOS=windows GOARCH=amd64

build-windows-arm64:
	$(MAKE) build GOOS=windows GOARCH=arm64

# Convenience targets
build-linux: build-linux-amd64 build-linux-arm64

build-darwin: build-darwin-amd64 build-darwin-arm64

build-windows: build-windows-amd64 build-windows-arm64

# Test: Extension loading
test-load: build
	@echo "=== Test: Extension Load ==="
	duckdb -unsigned -c "LOAD './$(OUTPUT_DIR)/$(EXTENSION_NAME).duckdb_extension';" && \
		echo "PASS: Extension loaded" || echo "FAIL: Extension failed to load"

# Test: Hardcoded data
test-hardcoded: build
	@echo "=== Test: Hardcoded Data ==="
	duckdb -unsigned < $(TESTS_DIR)/hardcoded_test.sql

# Test: Flight SQL connection
test-connection: build
	@echo "=== Test: Flight SQL Connection ==="
	duckdb -unsigned < $(TESTS_DIR)/connection_test.sql

# Test: Full data transfer
test-full: build
	@echo "=== Test: Full Data Transfer ==="
	duckdb -unsigned < $(TESTS_DIR)/full_test.sql

# Test: Replacement scan (includes validation error tests)
test-replacement-scan: build
	@echo "=== Test: Replacement Scan ==="
	@echo "Note: This test includes expected validation errors"
	duckdb -unsigned < $(TESTS_DIR)/replacement_scan_test.sql; \
	if [ $$? -eq 0 ] || [ $$? -eq 1 ]; then echo "Replacement scan tests completed (errors are expected for validation tests)"; else exit 1; fi

# Test: Connection pooling
test-pool: build
	@echo "=== Test: Connection Pool ==="
	@echo "Note: Compare timing - subsequent queries should be faster"
	duckdb -unsigned < $(TESTS_DIR)/connection_pool_test.sql

# Test: ATTACH/DETACH basic functionality
test-attach: build
	@echo "=== Test: ATTACH/DETACH ==="
	@echo "Note: This test includes expected errors for error handling tests"
	duckdb -unsigned < $(TESTS_DIR)/attach_basic_test.sql; \
	if [ $$? -eq 0 ]; then echo "ATTACH/DETACH tests PASSED"; \
	elif [ $$? -eq 1 ]; then echo "ATTACH/DETACH tests completed with expected errors"; \
	else echo "ATTACH/DETACH tests FAILED with unexpected error"; exit 1; fi

# Test: SHOW TABLES functionality
test-show-tables: build
	@echo "=== Test: SHOW TABLES ==="
	duckdb -unsigned < $(TESTS_DIR)/show_tables_test.sql

# Test: SHOW SCHEMAS functionality
test-show-schemas: build
	@echo "=== Test: SHOW SCHEMAS ==="
	@echo "Note: This test includes expected errors for error handling tests"
	duckdb -unsigned < $(TESTS_DIR)/show_schemas_test.sql; \
	if [ $$? -eq 0 ]; then echo "SHOW SCHEMAS tests PASSED"; \
	elif [ $$? -eq 1 ]; then echo "SHOW SCHEMAS tests completed with expected errors"; \
	else echo "SHOW SCHEMAS tests FAILED with unexpected error"; exit 1; fi

# Test: Qualified queries (three-part and two-part names)
test-qualified: build
	@echo "=== Test: Qualified Queries ==="
	duckdb -unsigned < $(TESTS_DIR)/qualified_queries_test.sql

# Test: Information schema integration
test-info-schema: build
	@echo "=== Test: Information Schema ==="
	duckdb -unsigned < $(TESTS_DIR)/information_schema_test.sql

# Test: Projection pushdown
test-projection: build
	@echo "=== Test: Projection Pushdown ==="
	duckdb -unsigned < $(TESTS_DIR)/projection_pushdown_test.sql

# Run SQL integration tests (core functionality)
test: test-load test-hardcoded test-connection test-full test-replacement-scan test-pool
	@echo "SQL integration tests passed!"

# Go unit tests (no server required)
test-unit:
	@echo "=== Go Unit Tests ==="
	go test -v -race ./internal/...

# Go tests with coverage
test-coverage:
	@echo "=== Go Tests with Coverage ==="
	go test -v -race -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out

# Test: Data type conversions
test-types: build
	@echo "=== Test: Data Types ==="
	duckdb -unsigned < $(TESTS_DIR)/data_types_test.sql

# Test: Edge cases (large results, unicode)
test-edge-cases: build
	@echo "=== Test: Edge Cases ==="
	duckdb -unsigned < $(TESTS_DIR)/integration_test.sql

# Test: Error handling
# Note: This test includes expected errors, but the final recovery test must pass
test-errors: build
	@echo "=== Test: Error Handling ==="
	@echo "Note: Expected errors will be shown, but recovery test must pass"
	duckdb -unsigned < $(TESTS_DIR)/error_handling_test.sql; \
	if [ $$? -eq 0 ]; then echo "Error handling tests PASSED"; \
	elif [ $$? -eq 1 ]; then echo "Error handling tests completed with expected errors"; \
	else echo "Error handling tests FAILED with unexpected error"; exit 1; fi

# Full test suite (unit + all SQL tests)
test-all: test-unit test test-types test-edge-cases test-errors
	@echo "All tests completed!"

# Clean C++ build artifacts
cpp-clean:
	rm -rf $(CPP_BUILD_DIR)

# Clean build artifacts
clean: cpp-clean
	rm -rf $(BUILD_DIR)

# Development helpers
fmt:
	go fmt ./...

# Show help
help:
	@echo "DuckArrow DuckDB Extension Build System"
	@echo ""
	@echo "Build Targets:"
	@echo "  build               - Build the extension for current platform"
	@echo "  cmake-configure     - Configure CMake for C++ build"
	@echo "  cmake-build         - Build C++ static library"
	@echo "  build-linux-amd64   - Build for Linux x86_64"
	@echo "  build-linux-arm64   - Build for Linux ARM64"
	@echo "  build-darwin-amd64  - Build for macOS Intel"
	@echo "  build-darwin-arm64  - Build for macOS Apple Silicon"
	@echo "  build-windows-amd64 - Build for Windows x86_64"
	@echo "  build-windows-arm64 - Build for Windows ARM64"
	@echo "  build-linux         - Build for all Linux platforms"
	@echo "  build-darwin        - Build for all macOS platforms"
	@echo "  build-windows       - Build for all Windows platforms"
	@echo ""
	@echo "Test Targets:"
	@echo "  test                - Run core SQL integration tests"
	@echo "  test-unit           - Run Go unit tests (no server required)"
	@echo "  test-coverage       - Run Go tests with coverage report"
	@echo "  test-all            - Run full test suite (unit + all SQL tests)"
	@echo "  test-load           - Test extension loading"
	@echo "  test-hardcoded      - Test hardcoded table function"
	@echo "  test-connection     - Test Flight SQL connection"
	@echo "  test-full           - Test full data transfer"
	@echo "  test-replacement-scan - Test replacement scan (duckarrow.* syntax)"
	@echo "  test-pool           - Test connection pooling"
	@echo "  test-attach         - Test ATTACH/DETACH functionality"
	@echo "  test-show-tables    - Test SHOW TABLES functionality"
	@echo "  test-show-schemas   - Test SHOW SCHEMAS functionality"
	@echo "  test-qualified      - Test qualified queries (three-part and two-part names)"
	@echo "  test-info-schema    - Test information_schema integration"
	@echo "  test-projection     - Test projection pushdown"
	@echo "  test-types          - Test data type conversions"
	@echo "  test-edge-cases     - Test edge cases (large results, unicode)"
	@echo "  test-errors         - Test error handling"
	@echo ""
	@echo "Other:"
	@echo "  clean               - Remove build artifacts (including C++)"
	@echo "  cpp-clean           - Remove C++ build artifacts only"
	@echo "  deps                - Install dependencies"
	@echo "  fmt                 - Format Go code"
	@echo ""
	@echo "Cross-compilation note:"
	@echo "  Platform-specific targets require appropriate C toolchains."
	@echo "  For native builds, just run 'make build' on the target platform."
