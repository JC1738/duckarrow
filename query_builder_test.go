package main

import (
	"testing"
)

func TestExtractTableName(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "simple table name",
			query:    `SELECT * FROM "Order"`,
			expected: "Order",
		},
		{
			name:     "table with lowercase",
			query:    `SELECT * FROM "users"`,
			expected: "users",
		},
		{
			name:     "table with escaped quotes",
			query:    `SELECT * FROM "table""name"`,
			expected: `table"name`,
		},
		{
			name:     "table with multiple escaped quotes",
			query:    `SELECT * FROM "a""b""c"`,
			expected: `a"b"c`,
		},
		{
			name:     "case insensitive SELECT",
			query:    `select * from "MyTable"`,
			expected: "MyTable",
		},
		{
			name:     "extra whitespace",
			query:    `SELECT  *  FROM  "TestTable"`,
			expected: "TestTable",
		},
		{
			name:     "empty query",
			query:    "",
			expected: "",
		},
		{
			name:     "invalid query format",
			query:    "INSERT INTO table VALUES (1)",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTableName(tt.query)
			if result != tt.expected {
				t.Errorf("extractTableName(%q) = %q, want %q", tt.query, result, tt.expected)
			}
		})
	}
}

func TestBuildProjectedQuery(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		columns   []string
		expected  string
	}{
		{
			name:      "single column",
			tableName: "Order",
			columns:   []string{"id"},
			expected:  `SELECT "id" FROM "Order"`,
		},
		{
			name:      "multiple columns",
			tableName: "Order",
			columns:   []string{"id", "name", "status"},
			expected:  `SELECT "id", "name", "status" FROM "Order"`,
		},
		{
			name:      "empty columns - SELECT *",
			tableName: "Order",
			columns:   []string{},
			expected:  `SELECT * FROM "Order"`,
		},
		{
			name:      "nil columns - SELECT *",
			tableName: "Order",
			columns:   nil,
			expected:  `SELECT * FROM "Order"`,
		},
		{
			name:      "table name with quotes",
			tableName: `My"Table`,
			columns:   []string{"col1"},
			expected:  `SELECT "col1" FROM "My""Table"`,
		},
		{
			name:      "column name with quotes",
			tableName: "Order",
			columns:   []string{`col"1`, "col2"},
			expected:  `SELECT "col""1", "col2" FROM "Order"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildProjectedQuery(tt.tableName, tt.columns)
			if result != tt.expected {
				t.Errorf("buildProjectedQuery(%q, %v) = %q, want %q", tt.tableName, tt.columns, result, tt.expected)
			}
		})
	}
}

func TestBuildSchemaQuery(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		expected  string
	}{
		{
			name:      "simple table",
			tableName: "Order",
			expected:  `SELECT * FROM "Order" WHERE 1=0`,
		},
		{
			name:      "table with quotes",
			tableName: `My"Table`,
			expected:  `SELECT * FROM "My""Table" WHERE 1=0`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildSchemaQuery(tt.tableName)
			if result != tt.expected {
				t.Errorf("buildSchemaQuery(%q) = %q, want %q", tt.tableName, result, tt.expected)
			}
		})
	}
}
