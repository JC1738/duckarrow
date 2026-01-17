package main

import (
	"fmt"
	"regexp"
	"strings"
)

// extractTableName extracts the table name from a query of the form:
// SELECT * FROM "tablename" or SELECT * FROM "table""name" (with escaped quotes)
// Returns the unescaped table name.
func extractTableName(query string) string {
	// Match SELECT * FROM "tablename" pattern
	// The table name may contain escaped double quotes (doubled)
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+"([^"]*(?:""[^"]*)*)"`)
	matches := re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		// Unescape doubled quotes
		return strings.ReplaceAll(matches[1], `""`, `"`)
	}
	// Fallback: try without quotes (shouldn't happen from replacement scan)
	re = regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+(\S+)`)
	matches = re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// buildProjectedQuery constructs a SQL query with specific columns.
// If columns is empty, uses SELECT *.
// tableName should be unescaped; this function handles escaping.
func buildProjectedQuery(tableName string, columns []string) string {
	escapedTable := strings.ReplaceAll(tableName, `"`, `""`)

	var columnList string
	if len(columns) == 0 {
		columnList = "*"
	} else {
		escapedCols := make([]string, len(columns))
		for i, col := range columns {
			escapedCols[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(col, `"`, `""`))
		}
		columnList = strings.Join(escapedCols, ", ")
	}

	return fmt.Sprintf(`SELECT %s FROM "%s"`, columnList, escapedTable)
}

// buildSchemaQuery constructs a query that returns only the schema (no rows).
// Uses WHERE 1=0 to avoid fetching any data.
func buildSchemaQuery(tableName string) string {
	escapedTable := strings.ReplaceAll(tableName, `"`, `""`)
	return fmt.Sprintf(`SELECT * FROM "%s" WHERE 1=0`, escapedTable)
}
