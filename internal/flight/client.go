package flight

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config for DuckArrow Flight SQL connection
type Config struct {
	URI        string // e.g., "grpc+tls://localhost:31337"
	Username   string
	Password   string
	SkipVerify bool
}

// Client wraps ADBC Flight SQL connection
type Client struct {
	db   adbc.Database
	conn adbc.Connection
}

// Connect establishes connection to Flight SQL server
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	drv := flightsql.NewDriver(nil)

	opts := map[string]string{
		adbc.OptionKeyURI:      cfg.URI,
		adbc.OptionKeyUsername: cfg.Username,
		adbc.OptionKeyPassword: cfg.Password,
	}

	if cfg.SkipVerify {
		opts[flightsql.OptionSSLSkipVerify] = "true"
	}

	// Increase gRPC message size from 16MB to 256MB for large result sets
	maxMsgSize := 256 * 1024 * 1024
	dialOpts := grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(maxMsgSize),
		grpc.MaxCallSendMsgSize(maxMsgSize),
	)

	// Add gRPC keepalive to prevent stale connections
	// Use conservative settings to avoid server's ENHANCE_YOUR_CALM/too_many_pings
	keepaliveOpts := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                2 * time.Minute,  // Ping interval during active streams
		Timeout:             20 * time.Second, // Wait 20s for ping response
		PermitWithoutStream: false,            // Only ping with active streams
	})

	db, err := drv.NewDatabaseWithOptions(opts, dialOpts, keepaliveOpts)
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open connection: %w", err)
	}

	return &Client{db: db, conn: conn}, nil
}

// QueryResult holds the reader and statement for cleanup
type QueryResult struct {
	Reader array.RecordReader
	Stmt   adbc.Statement
}

// Query executes SQL and returns Arrow RecordReader
// Note: Caller must call result.Reader.Release() and result.Stmt.Close() when done
func (c *Client) Query(ctx context.Context, sql string) (*QueryResult, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("create statement: %w", err)
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("execute query: %w", err)
	}

	return &QueryResult{
		Reader: reader,
		Stmt:   stmt,
	}, nil
}

// Execute executes a non-query SQL statement (DDL/DML) and returns affected row count.
// Use this for CREATE, DROP, INSERT, UPDATE, DELETE statements.
// Returns -1 if the server doesn't provide affected row count.
func (c *Client) Execute(ctx context.Context, sql string) (int64, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return 0, fmt.Errorf("set query: %w", err)
	}

	affected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return 0, fmt.Errorf("execute update: %w", err)
	}

	return affected, nil
}

// IsHealthy checks if the connection is still valid
func (c *Client) IsHealthy() bool {
	return c.conn != nil && c.db != nil
}

// GetSchemas returns a list of schema names from the Flight SQL server.
// It first tries ADBC GetObjects; if that fails, it falls back to SQL query.
func (c *Client) GetSchemas(ctx context.Context) ([]string, error) {
	schemas, err := c.getSchemasViaADBC(ctx)
	if err == nil {
		return schemas, nil
	}

	// Fall back to SQL query
	return c.getSchemasViaSQL(ctx)
}

// getSchemasViaADBC uses ADBC GetObjects to retrieve schema names
func (c *Client) getSchemasViaADBC(ctx context.Context) ([]string, error) {
	reader, err := c.conn.GetObjects(ctx, adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get objects: %w", err)
	}
	defer reader.Release()

	var schemas []string

	for reader.Next() {
		rec := reader.RecordBatch()

		// GetObjects returns schema with catalog_name (utf8) and catalog_db_schemas (list<struct>)
		// The db_schema_name is inside the nested struct
		if rec.NumCols() < 2 {
			continue
		}

		// Column 1 is catalog_db_schemas which is a list of structs
		dbSchemasCol := rec.Column(1)
		listArr, ok := dbSchemasCol.(*array.List)
		if !ok {
			continue
		}

		// The values inside the list are structs with db_schema_name field
		structArr, ok := listArr.ListValues().(*array.Struct)
		if !ok {
			continue
		}

		// Find the db_schema_name field (should be field 0)
		if structArr.NumField() < 1 {
			continue
		}

		schemaNameField := structArr.Field(0)
		stringArr, ok := schemaNameField.(*array.String)
		if !ok {
			continue
		}

		// Iterate through each row in the record
		for i := 0; i < int(rec.NumRows()); i++ {
			if listArr.IsNull(i) {
				continue
			}
			start, end := listArr.ValueOffsets(i)
			for j := int(start); j < int(end); j++ {
				if !stringArr.IsNull(j) {
					schemas = append(schemas, stringArr.Value(j))
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read objects: %w", err)
	}

	return schemas, nil
}

// getSchemasViaSQL uses a SQL query to retrieve schema names
func (c *Client) getSchemasViaSQL(ctx context.Context) ([]string, error) {
	result, err := c.Query(ctx, "SELECT schema_name FROM information_schema.schemata")
	if err != nil {
		return nil, fmt.Errorf("query schemas: %w", err)
	}
	defer result.Reader.Release()
	defer result.Stmt.Close()

	var schemas []string

	for result.Reader.Next() {
		rec := result.Reader.RecordBatch()
		if rec.NumCols() < 1 {
			continue
		}

		col := rec.Column(0)
		stringArr, ok := col.(*array.String)
		if !ok {
			continue
		}

		for i := 0; i < stringArr.Len(); i++ {
			if !stringArr.IsNull(i) {
				schemas = append(schemas, stringArr.Value(i))
			}
		}
	}

	if err := result.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read schemas: %w", err)
	}

	return schemas, nil
}

// TableInfo represents a table with its schema
type TableInfo struct {
	Schema string
	Name   string
}

// GetTables returns a list of tables from the Flight SQL server for a given schema.
// It first tries ADBC GetObjects; if that fails, it falls back to SQL query.
func (c *Client) GetTables(ctx context.Context, schema string) ([]TableInfo, error) {
	tables, err := c.getTablesViaADBC(ctx, schema)
	if err == nil {
		return tables, nil
	}

	// Fall back to SQL query
	return c.getTablesViaSQL(ctx, schema)
}

// getTablesViaADBC uses ADBC GetObjects to retrieve table names for a schema
func (c *Client) getTablesViaADBC(ctx context.Context, schema string) ([]TableInfo, error) {
	reader, err := c.conn.GetObjects(ctx, adbc.ObjectDepthTables, nil, &schema, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get objects: %w", err)
	}
	defer reader.Release()

	var tables []TableInfo

	for reader.Next() {
		rec := reader.RecordBatch()

		// GetObjects returns schema with catalog_name (utf8) and catalog_db_schemas (list<struct>)
		// The db_schema contains db_schema_name and db_schema_tables (list<struct>)
		if rec.NumCols() < 2 {
			continue
		}

		// Column 1 is catalog_db_schemas which is a list of structs
		dbSchemasCol := rec.Column(1)
		schemasListArr, ok := dbSchemasCol.(*array.List)
		if !ok {
			continue
		}

		// The values inside the list are structs with db_schema_name and db_schema_tables
		schemasStructArr, ok := schemasListArr.ListValues().(*array.Struct)
		if !ok {
			continue
		}

		// Need at least 2 fields: db_schema_name and db_schema_tables
		if schemasStructArr.NumField() < 2 {
			continue
		}

		schemaNameField := schemasStructArr.Field(0)
		schemaNameArr, ok := schemaNameField.(*array.String)
		if !ok {
			continue
		}

		tablesField := schemasStructArr.Field(1)
		tablesListArr, ok := tablesField.(*array.List)
		if !ok {
			continue
		}

		tablesStructArr, ok := tablesListArr.ListValues().(*array.Struct)
		if !ok {
			continue
		}

		// First field of tables struct is table_name
		if tablesStructArr.NumField() < 1 {
			continue
		}

		tableNameField := tablesStructArr.Field(0)
		tableNameArr, ok := tableNameField.(*array.String)
		if !ok {
			continue
		}

		// Iterate through each row in the record
		for i := 0; i < int(rec.NumRows()); i++ {
			if schemasListArr.IsNull(i) {
				continue
			}
			schemaStart, schemaEnd := schemasListArr.ValueOffsets(i)
			for j := int(schemaStart); j < int(schemaEnd); j++ {
				if schemaNameArr.IsNull(j) {
					continue
				}
				schemaName := schemaNameArr.Value(j)

				if tablesListArr.IsNull(j) {
					continue
				}
				tableStart, tableEnd := tablesListArr.ValueOffsets(j)
				for k := int(tableStart); k < int(tableEnd); k++ {
					if !tableNameArr.IsNull(k) {
						tables = append(tables, TableInfo{
							Schema: schemaName,
							Name:   tableNameArr.Value(k),
						})
					}
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read objects: %w", err)
	}

	return tables, nil
}

// getTablesViaSQL uses a SQL query to retrieve table names for a schema
func (c *Client) getTablesViaSQL(ctx context.Context, schema string) ([]TableInfo, error) {
	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema)
	result, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer result.Reader.Release()
	defer result.Stmt.Close()

	var tables []TableInfo

	for result.Reader.Next() {
		rec := result.Reader.RecordBatch()
		if rec.NumCols() < 1 {
			continue
		}

		col := rec.Column(0)
		stringArr, ok := col.(*array.String)
		if !ok {
			continue
		}

		for i := 0; i < stringArr.Len(); i++ {
			if !stringArr.IsNull(i) {
				tables = append(tables, TableInfo{
					Schema: schema,
					Name:   stringArr.Value(i),
				})
			}
		}
	}

	if err := result.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read tables: %w", err)
	}

	return tables, nil
}

// ColumnInfo describes a column in a table
type ColumnInfo struct {
	Name            string
	TypeName        string
	Nullable        bool
	OrdinalPosition int
}

// GetColumns returns column information for a table.
// It first tries ADBC GetObjects; if that fails, it falls back to SQL query.
func (c *Client) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	columns, err := c.getColumnsViaADBC(ctx, schema, table)
	if err == nil {
		return columns, nil
	}

	// Fall back to SQL query
	return c.getColumnsViaSQL(ctx, schema, table)
}

// getColumnsViaADBC uses ADBC GetObjects to retrieve column information
func (c *Client) getColumnsViaADBC(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	reader, err := c.conn.GetObjects(ctx, adbc.ObjectDepthColumns, nil, &schema, nil, &table, nil)
	if err != nil {
		return nil, fmt.Errorf("get objects: %w", err)
	}
	defer reader.Release()

	var columns []ColumnInfo

	for reader.Next() {
		rec := reader.RecordBatch()

		// GetObjects returns: catalog_name (utf8), catalog_db_schemas (list<struct>)
		// catalog_db_schemas contains: db_schema_name (utf8), db_schema_tables (list<struct>)
		// db_schema_tables contains: table_name (utf8), table_type (utf8), table_columns (list<struct>), ...
		// table_columns contains: column_name (utf8), ordinal_position (int32), remarks, xdbc_* fields
		if rec.NumCols() < 2 {
			continue
		}

		// Column 1 is catalog_db_schemas
		dbSchemasCol := rec.Column(1)
		dbSchemasListArr, ok := dbSchemasCol.(*array.List)
		if !ok {
			continue
		}

		dbSchemasStructArr, ok := dbSchemasListArr.ListValues().(*array.Struct)
		if !ok || dbSchemasStructArr.NumField() < 2 {
			continue
		}

		// Field 1 of db_schema struct is db_schema_tables (list<struct>)
		tablesListArr, ok := dbSchemasStructArr.Field(1).(*array.List)
		if !ok {
			continue
		}

		tablesStructArr, ok := tablesListArr.ListValues().(*array.Struct)
		if !ok || tablesStructArr.NumField() < 3 {
			continue
		}

		// Field 2 of table struct is table_columns (list<struct>)
		columnsListArr, ok := tablesStructArr.Field(2).(*array.List)
		if !ok {
			continue
		}

		columnsStructArr, ok := columnsListArr.ListValues().(*array.Struct)
		if !ok {
			continue
		}

		// Extract column fields by name from the struct schema
		var colNameArr *array.String
		var ordinalArr *array.Int32
		var typeNameArr *array.String
		var nullableArr *array.Int16

		// Look up field indices in the columns struct
		structType := columnsStructArr.DataType().(*arrow.StructType)
		for fieldIdx := 0; fieldIdx < columnsStructArr.NumField(); fieldIdx++ {
			fieldName := structType.Field(fieldIdx).Name
			switch fieldName {
			case "column_name":
				if arr, ok := columnsStructArr.Field(fieldIdx).(*array.String); ok {
					colNameArr = arr
				}
			case "ordinal_position":
				if arr, ok := columnsStructArr.Field(fieldIdx).(*array.Int32); ok {
					ordinalArr = arr
				}
			case "xdbc_type_name":
				if arr, ok := columnsStructArr.Field(fieldIdx).(*array.String); ok {
					typeNameArr = arr
				}
			case "xdbc_nullable":
				if arr, ok := columnsStructArr.Field(fieldIdx).(*array.Int16); ok {
					nullableArr = arr
				}
			}
		}

		if colNameArr == nil {
			continue
		}

		// Iterate through catalog rows
		for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
			if dbSchemasListArr.IsNull(rowIdx) {
				continue
			}

			schemaStart, schemaEnd := dbSchemasListArr.ValueOffsets(rowIdx)
			for schemaIdx := int(schemaStart); schemaIdx < int(schemaEnd); schemaIdx++ {
				if tablesListArr.IsNull(schemaIdx) {
					continue
				}

				tableStart, tableEnd := tablesListArr.ValueOffsets(schemaIdx)
				for tableIdx := int(tableStart); tableIdx < int(tableEnd); tableIdx++ {
					if columnsListArr.IsNull(tableIdx) {
						continue
					}

					colStart, colEnd := columnsListArr.ValueOffsets(tableIdx)
					for colIdx := int(colStart); colIdx < int(colEnd); colIdx++ {
						if colNameArr.IsNull(colIdx) {
							continue
						}

						col := ColumnInfo{
							Name: colNameArr.Value(colIdx),
						}

						if ordinalArr != nil && !ordinalArr.IsNull(colIdx) {
							col.OrdinalPosition = int(ordinalArr.Value(colIdx))
						}

						if typeNameArr != nil && !typeNameArr.IsNull(colIdx) {
							col.TypeName = typeNameArr.Value(colIdx)
						}

						// xdbc_nullable: 0 = not nullable, 1 = nullable, 2 = unknown
						if nullableArr != nil && !nullableArr.IsNull(colIdx) {
							col.Nullable = nullableArr.Value(colIdx) == 1
						}

						columns = append(columns, col)
					}
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read objects: %w", err)
	}

	return columns, nil
}

// getColumnsViaSQL uses a SQL query to retrieve column information
func (c *Client) getColumnsViaSQL(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf(
		"SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
		schema, table,
	)
	result, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer result.Reader.Release()
	defer result.Stmt.Close()

	var columns []ColumnInfo

	for result.Reader.Next() {
		rec := result.Reader.RecordBatch()
		if rec.NumCols() < 4 {
			continue
		}

		colNameArr, ok := rec.Column(0).(*array.String)
		if !ok {
			continue
		}

		dataTypeArr, ok := rec.Column(1).(*array.String)
		if !ok {
			continue
		}

		nullableArr, ok := rec.Column(2).(*array.String)
		if !ok {
			continue
		}

		// ordinal_position could be int32 or int64 depending on server
		var getOrdinal func(i int) int
		switch ordArr := rec.Column(3).(type) {
		case *array.Int32:
			getOrdinal = func(i int) int { return int(ordArr.Value(i)) }
		case *array.Int64:
			getOrdinal = func(i int) int { return int(ordArr.Value(i)) }
		default:
			getOrdinal = func(i int) int { return i + 1 }
		}

		for i := 0; i < colNameArr.Len(); i++ {
			if colNameArr.IsNull(i) {
				continue
			}

			col := ColumnInfo{
				Name:            colNameArr.Value(i),
				OrdinalPosition: getOrdinal(i),
			}

			if !dataTypeArr.IsNull(i) {
				col.TypeName = dataTypeArr.Value(i)
			}

			if !nullableArr.IsNull(i) {
				col.Nullable = nullableArr.Value(i) == "YES"
			}

			columns = append(columns, col)
		}
	}

	if err := result.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}

	return columns, nil
}

// Close closes connection and database
func (c *Client) Close() error {
	var errs []error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
