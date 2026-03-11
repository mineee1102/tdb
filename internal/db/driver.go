package db

import (
	"context"
	"fmt"
)

// DriverType represents the type of database driver.
type DriverType string

const (
	Postgres  DriverType = "postgres"
	MySQL     DriverType = "mysql"
	SQLite    DriverType = "sqlite"
	SQLServer DriverType = "mssql"
)

// TableInfo holds basic information about a table or view.
type TableInfo struct {
	Schema string
	Name   string
	Type   string // "table", "view"
}

// ColumnInfo holds information about a column.
type ColumnInfo struct {
	Name         string
	DataType     string
	Nullable     bool
	DefaultValue *string
	IsPrimaryKey bool
}

// IndexInfo holds information about an index.
type IndexInfo struct {
	Name     string
	Columns  []string
	IsUnique bool
}

// ForeignKeyInfo holds information about a foreign key.
type ForeignKeyInfo struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
}

// TableDescription holds the full description of a table including columns, indexes, and foreign keys.
type TableDescription struct {
	Schema      string
	Name        string
	Columns     []ColumnInfo
	PrimaryKeys []string
	Indexes     []IndexInfo
	ForeignKeys []ForeignKeyInfo
}

// QueryResult holds the result of a query execution.
type QueryResult struct {
	Columns      []string
	ColumnTypes  []string
	Rows         [][]interface{}
	RowsAffected int64
}

// GetRowsOptions specifies options for fetching rows from a table.
type GetRowsOptions struct {
	Limit    int
	Offset   int
	OrderBy  string
	OrderDir string // "ASC" or "DESC"
	Where    string
	// WhereArgs holds parameterized query arguments for the Where clause.
	// When set, Where should use placeholder syntax (e.g. "col = ?") and
	// WhereArgs supplies the corresponding values.
	// For user-typed free-form WHERE (e.g. via the '/' key in Data View),
	// WhereArgs is nil and the raw SQL is passed through as-is — this is
	// intentional, as the user is effectively writing SQL like in the SQL editor.
	WhereArgs []interface{}
}

// Transaction represents a database transaction.
type Transaction interface {
	Commit() error
	Rollback() error
	Exec(ctx context.Context, query string, args ...interface{}) (int64, error)
	Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error)
}

// Driver is the interface that all database drivers must implement.
type Driver interface {
	Connect(ctx context.Context, dsn string) error
	Close() error
	DriverName() DriverType

	// Schema and table introspection
	ListSchemas(ctx context.Context) ([]string, error)
	ListTables(ctx context.Context, schema string) ([]TableInfo, error)
	DescribeTable(ctx context.Context, schema, table string) (*TableDescription, error)

	// Data retrieval
	GetRows(ctx context.Context, schema, table string, opts GetRowsOptions) (*QueryResult, error)

	// Query execution
	Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error)
	Exec(ctx context.Context, query string, args ...interface{}) (int64, error)

	// CRUD operations
	InsertRow(ctx context.Context, schema, table string, values map[string]interface{}) error
	UpdateRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}, values map[string]interface{}) error
	DeleteRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}) error

	// Transaction support
	Begin(ctx context.Context) (Transaction, error)

	// Database switching
	ListDatabases(ctx context.Context) ([]string, error)
	SwitchDatabase(ctx context.Context, dbName string) error
	CurrentDatabase(ctx context.Context) (string, error)
}

// NewDriver creates a new driver instance based on the specified driver type.
func NewDriver(dt DriverType) (Driver, error) {
	switch dt {
	case Postgres:
		return &PostgresDriver{}, nil
	case MySQL:
		return &MySQLDriver{}, nil
	case SQLite:
		return &SQLiteDriver{}, nil
	case SQLServer:
		return &MSSQLDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported driver type: %s", dt)
	}
}
