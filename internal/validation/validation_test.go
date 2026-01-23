package validation

import (
	"strings"
	"sync"
	"testing"
)

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid cases
		{name: "valid simple name", input: "Order", wantErr: false},
		{name: "valid with underscore", input: "my_table", wantErr: false},
		{name: "valid unicode", input: "表名", wantErr: false},
		{name: "valid with numbers", input: "table123", wantErr: false},
		{name: "valid single char", input: "a", wantErr: false},
		{name: "valid 255 chars", input: strings.Repeat("a", 255), wantErr: false},
		{name: "valid with backslash", input: "test\\name", wantErr: false},
		{name: "valid with space", input: "my table", wantErr: false},
		{name: "valid with single quote", input: "table'name", wantErr: false},
		{name: "valid with double quote", input: `table"name`, wantErr: false},

		// Invalid cases - empty/length
		{name: "empty string", input: "", wantErr: true},
		{name: "too long 256 chars", input: strings.Repeat("a", 256), wantErr: true},

		// Invalid cases - SQL injection patterns
		{name: "SQL injection semicolon", input: "test;DROP", wantErr: true},
		{name: "SQL injection line comment", input: "test--evil", wantErr: true},
		{name: "SQL injection block comment start", input: "test/*evil", wantErr: true},
		{name: "SQL injection block comment end", input: "evil*/test", wantErr: true},

		// Invalid cases - control characters
		{name: "null byte", input: "test\x00name", wantErr: true},
		{name: "newline", input: "test\nname", wantErr: true},
		{name: "carriage return", input: "test\rname", wantErr: true},
		{name: "tab character", input: "test\tname", wantErr: true},

		// Edge cases - patterns at start/end
		{name: "semicolon at start", input: ";table", wantErr: true},
		{name: "semicolon at end", input: "table;", wantErr: true},
		{name: "comment at start", input: "--table", wantErr: true},
		{name: "comment at end", input: "table--", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTableName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateURI(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		// Valid cases
		{name: "valid grpc", input: "grpc://localhost:31337", wantErr: false},
		{name: "valid grpc+tls", input: "grpc+tls://localhost:31337", wantErr: false},
		{name: "valid grpc+tls with domain", input: "grpc+tls://server.example.com:443", wantErr: false},
		{name: "valid grpc localhost only", input: "grpc://localhost", wantErr: false},
		{name: "valid grpc with IP", input: "grpc://192.168.1.1:8080", wantErr: false},
		{name: "valid grpc+tls with IPv6", input: "grpc+tls://[::1]:31337", wantErr: false},

		// Invalid - empty/whitespace
		{name: "empty URI", input: "", wantErr: true, errMsg: "cannot be empty"},
		{name: "whitespace only", input: "   ", wantErr: true, errMsg: "cannot be empty"},
		{name: "tabs only", input: "\t\t", wantErr: true, errMsg: "cannot be empty"},

		// Invalid - wrong scheme
		{name: "http scheme", input: "http://localhost:31337", wantErr: true, errMsg: "must start with grpc://"},
		{name: "https scheme", input: "https://localhost:31337", wantErr: true, errMsg: "must start with grpc://"},
		{name: "no scheme", input: "localhost:31337", wantErr: true, errMsg: "must start with grpc://"},
		{name: "ftp scheme", input: "ftp://localhost:31337", wantErr: true, errMsg: "must start with grpc://"},

		// Invalid - missing host
		{name: "no host grpc", input: "grpc://", wantErr: true, errMsg: "must include a host"},
		{name: "no host grpc+tls", input: "grpc+tls://", wantErr: true, errMsg: "must include a host"},

		// Invalid - length
		{name: "too long URI", input: "grpc://localhost:31337/" + strings.Repeat("a", 2049), wantErr: true, errMsg: "exceeds maximum length"},

		// Edge cases with leading/trailing whitespace (should be trimmed)
		{name: "leading whitespace", input: "  grpc://localhost:31337", wantErr: false},
		{name: "trailing whitespace", input: "grpc://localhost:31337  ", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURI(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateURI(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateURI(%q) error = %q, want error containing %q", tt.input, err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func TestValidateURILengthBoundary(t *testing.T) {
	// Test exactly at the 2048 character boundary
	baseURI := "grpc://localhost:31337/"

	// 2048 chars should pass
	uri2048 := baseURI + strings.Repeat("a", 2048-len(baseURI))
	if len(uri2048) != 2048 {
		t.Fatalf("test setup error: uri2048 length = %d, want 2048", len(uri2048))
	}
	if err := ValidateURI(uri2048); err != nil {
		t.Errorf("ValidateURI with 2048 chars should pass, got error: %v", err)
	}

	// 2049 chars should fail
	uri2049 := baseURI + strings.Repeat("a", 2049-len(baseURI))
	if len(uri2049) != 2049 {
		t.Fatalf("test setup error: uri2049 length = %d, want 2049", len(uri2049))
	}
	if err := ValidateURI(uri2049); err == nil {
		t.Error("ValidateURI with 2049 chars should fail, got nil error")
	}
}

func TestValidateTableNameConcurrent(t *testing.T) {
	// Test concurrent access is safe (no shared mutable state)
	const numGoroutines = 100
	const numIterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				_ = ValidateTableName("test_table")
				_ = ValidateTableName("test;DROP")
				_ = ValidateTableName("")
			}
		}()
	}

	wg.Wait()
}

func TestShouldSkipTable(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		want      bool
	}{
		// Tables that should be skipped
		{"pg_catalog", "pg_catalog", true},
		{"pg_type", "pg_type", true},
		{"sqlite_master", "sqlite_master", true},
		{"sqlite_sequence", "sqlite_sequence", true},
		{"duckdb internal __", "__duckdb_internal", true},
		{"duckdb internal __duckarrow", "__duckarrow_cache", true},
		{"information_schema", "information_schema", true},
		{"MotherDuck cache table", "mdClientCache_KgH$9x4WdYvV_3", true},
		{"MotherDuck cache lowercase", "mdclientcache_abc123", true},
		{"MotherDuck cache uppercase", "MDCLIENTCACHE_XYZ", true},
		{"MotherDuck cache mixed case", "MdClientCache_Test", true},

		// Tables that should NOT be skipped (regular user tables)
		{"regular table", "users", false},
		{"table with underscore", "my_table", false},
		{"table starting with md", "mdtable", false},
		{"table starting with pg but not pg_", "pgtable", false},
		{"empty string", "", false},
		{"table with sqlite in name", "my_sqlite_backup", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldSkipTable(tt.tableName)
			if got != tt.want {
				t.Errorf("ShouldSkipTable(%q) = %v, want %v", tt.tableName, got, tt.want)
			}
		})
	}
}
