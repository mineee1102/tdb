package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
)

// MSSQLDriver implements the Driver interface for SQL Server.
type MSSQLDriver struct {
	db  *sql.DB
	dsn string
}

// mssqlTx wraps sql.Tx to implement the Transaction interface.
type mssqlTx struct {
	tx *sql.Tx
}

func (t *mssqlTx) Commit() error {
	return t.tx.Commit()
}

func (t *mssqlTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *mssqlTx) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("transaction exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (t *mssqlTx) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("transaction query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *MSSQLDriver) Connect(ctx context.Context, dsn string) error {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mssql connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping mssql: %w", err)
	}
	d.db = db
	d.dsn = dsn
	return nil
}

func (d *MSSQLDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *MSSQLDriver) DriverName() DriverType {
	return SQLServer
}

func (d *MSSQLDriver) ListSchemas(ctx context.Context) ([]string, error) {
	query := `SELECT name FROM sys.schemas 
		WHERE name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 
			'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
			'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
		ORDER BY name`
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan schema: %w", err)
		}
		schemas = append(schemas, name)
	}
	return schemas, rows.Err()
}

func (d *MSSQLDriver) ListTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `SELECT table_schema, table_name, 
		CASE table_type WHEN 'BASE TABLE' THEN 'table' WHEN 'VIEW' THEN 'view' ELSE table_type END as type
		FROM information_schema.tables 
		WHERE table_schema = @p1
		ORDER BY type, table_name`
	rows, err := d.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.Name, &t.Type); err != nil {
			return nil, fmt.Errorf("failed to scan table info: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (d *MSSQLDriver) DescribeTable(ctx context.Context, schema, table string) (*TableDescription, error) {
	desc := &TableDescription{
		Schema: schema,
		Name:   table,
	}

	// Get columns
	colQuery := `SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns 
		WHERE table_schema = @p1 AND table_name = @p2 
		ORDER BY ordinal_position`
	colRows, err := d.db.QueryContext(ctx, colQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var col ColumnInfo
		var nullable string
		var defaultVal sql.NullString
		if err := colRows.Scan(&col.Name, &col.DataType, &nullable, &defaultVal); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.Nullable = nullable == "YES"
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		desc.Columns = append(desc.Columns, col)
	}
	if err := colRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Get primary keys
	pkQuery := `SELECT col.name
		FROM sys.indexes idx
		JOIN sys.index_columns ic ON idx.object_id = ic.object_id AND idx.index_id = ic.index_id
		JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
		JOIN sys.tables t ON idx.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2 AND idx.is_primary_key = 1
		ORDER BY ic.key_ordinal`
	pkRows, err := d.db.QueryContext(ctx, pkQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary keys: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		desc.PrimaryKeys = append(desc.PrimaryKeys, colName)
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary keys: %w", err)
	}

	// Mark primary key columns
	for i, col := range desc.Columns {
		for _, pk := range desc.PrimaryKeys {
			if col.Name == pk {
				desc.Columns[i].IsPrimaryKey = true
				break
			}
		}
	}

	// Get indexes
	idxQuery := `SELECT idx.name, col.name, idx.is_unique
		FROM sys.indexes idx
		JOIN sys.index_columns ic ON idx.object_id = ic.object_id AND idx.index_id = ic.index_id
		JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
		JOIN sys.tables t ON idx.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2 AND idx.name IS NOT NULL
		ORDER BY idx.name, ic.key_ordinal`
	idxRows, err := d.db.QueryContext(ctx, idxQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer idxRows.Close()

	idxMap := make(map[string]*IndexInfo)
	for idxRows.Next() {
		var idxName, colName string
		var isUnique bool
		if err := idxRows.Scan(&idxName, &colName, &isUnique); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}
		if idx, ok := idxMap[idxName]; ok {
			idx.Columns = append(idx.Columns, colName)
		} else {
			idxMap[idxName] = &IndexInfo{
				Name:     idxName,
				Columns:  []string{colName},
				IsUnique: isUnique,
			}
		}
	}
	if err := idxRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}
	for _, idx := range idxMap {
		desc.Indexes = append(desc.Indexes, *idx)
	}

	// Get foreign keys
	fkQuery := `SELECT fk.name, col.name, ref_t.name, ref_col.name
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.columns col ON fkc.parent_object_id = col.object_id AND fkc.parent_column_id = col.column_id
		JOIN sys.tables ref_t ON fkc.referenced_object_id = ref_t.object_id
		JOIN sys.columns ref_col ON fkc.referenced_object_id = ref_col.object_id AND fkc.referenced_column_id = ref_col.column_id
		JOIN sys.tables t ON fk.parent_object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2
		ORDER BY fk.name, fkc.constraint_column_id`
	fkRows, err := d.db.QueryContext(ctx, fkQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer fkRows.Close()

	fkMap := make(map[string]*ForeignKeyInfo)
	for fkRows.Next() {
		var name, col, refTable, refCol string
		if err := fkRows.Scan(&name, &col, &refTable, &refCol); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}
		if fk, ok := fkMap[name]; ok {
			fk.Columns = append(fk.Columns, col)
			fk.RefColumns = append(fk.RefColumns, refCol)
		} else {
			fkMap[name] = &ForeignKeyInfo{
				Name:       name,
				Columns:    []string{col},
				RefTable:   refTable,
				RefColumns: []string{refCol},
			}
		}
	}
	if err := fkRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating foreign keys: %w", err)
	}
	for _, fk := range fkMap {
		desc.ForeignKeys = append(desc.ForeignKeys, *fk)
	}

	return desc, nil
}

func (d *MSSQLDriver) GetRows(ctx context.Context, schema, table string, opts GetRowsOptions) (*QueryResult, error) {
	// SQL Server requires ORDER BY for OFFSET/FETCH NEXT pagination
	orderClause := ""
	if opts.OrderBy != "" {
		dir := "ASC"
		if strings.ToUpper(opts.OrderDir) == "DESC" {
			dir = "DESC"
		}
		orderClause = fmt.Sprintf(" ORDER BY %s %s", mssqlQuoteIdentifier(opts.OrderBy), dir)
	} else if opts.Offset > 0 || opts.Limit > 0 {
		// SQL Server requires ORDER BY with OFFSET/FETCH; use (SELECT NULL) as a fallback
		orderClause = " ORDER BY (SELECT NULL)"
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s", mssqlQuoteIdentifier(schema), mssqlQuoteIdentifier(table))

	if opts.Where != "" {
		query += " WHERE " + opts.Where
	}
	query += orderClause

	if opts.Offset > 0 || opts.Limit > 0 {
		query += fmt.Sprintf(" OFFSET %d ROWS", opts.Offset)
		if opts.Limit > 0 {
			query += fmt.Sprintf(" FETCH NEXT %d ROWS ONLY", opts.Limit)
		}
	}

	return d.Query(ctx, query, opts.WhereArgs...)
}

func (d *MSSQLDriver) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *MSSQLDriver) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (d *MSSQLDriver) InsertRow(ctx context.Context, schema, table string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholders := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	i := 1
	for col, val := range values {
		columns = append(columns, mssqlQuoteIdentifier(col))
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i))
		args = append(args, val)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		mssqlQuoteIdentifier(schema), mssqlQuoteIdentifier(table),
		strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

func (d *MSSQLDriver) UpdateRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}, values map[string]interface{}) error {
	setClauses := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values)+len(primaryKeys))
	idx := 1
	for col, val := range values {
		setClauses = append(setClauses, fmt.Sprintf("%s = @p%d", mssqlQuoteIdentifier(col), idx))
		args = append(args, val)
		idx++
	}

	whereClause, whereArgs := buildWhereClause(primaryKeys, mssqlQuoteIdentifier, mssqlPlaceholder, idx)
	args = append(args, whereArgs...)

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		mssqlQuoteIdentifier(schema), mssqlQuoteIdentifier(table),
		strings.Join(setClauses, ", "), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

func (d *MSSQLDriver) DeleteRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}) error {
	whereClause, args := buildWhereClause(primaryKeys, mssqlQuoteIdentifier, mssqlPlaceholder, 1)

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		mssqlQuoteIdentifier(schema), mssqlQuoteIdentifier(table), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

func (d *MSSQLDriver) Begin(ctx context.Context) (Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &mssqlTx{tx: tx}, nil
}

func (d *MSSQLDriver) ListDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name`
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan database: %w", err)
		}
		databases = append(databases, name)
	}
	return databases, rows.Err()
}

func (d *MSSQLDriver) CurrentDatabase(ctx context.Context) (string, error) {
	var name string
	err := d.db.QueryRowContext(ctx, "SELECT DB_NAME()").Scan(&name)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	return name, nil
}

func (d *MSSQLDriver) SwitchDatabase(ctx context.Context, dbName string) error {
	// Reconnect with the new database name in the DSN.
	// Using USE statement is not safe with connection pools because it only
	// affects a single connection, not the entire pool.
	newDSN := mssqlReplaceDatabaseInDSN(d.dsn, dbName)
	newDB, err := sql.Open("sqlserver", newDSN)
	if err != nil {
		return fmt.Errorf("failed to open new connection: %w", err)
	}
	if err := newDB.PingContext(ctx); err != nil {
		newDB.Close()
		return fmt.Errorf("failed to connect to database %q: %w", dbName, err)
	}
	d.db.Close()
	d.db = newDB
	d.dsn = newDSN
	return nil
}

// mssqlReplaceDatabaseInDSN replaces the database name in a SQL Server DSN.
// MSSQL DSN format: sqlserver://user:pass@host:port?database=dbname&param=value
// The database name is specified as the "database" query parameter.
func mssqlReplaceDatabaseInDSN(dsn, newDB string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		// Fallback: try simple string replacement
		return dsn
	}
	q := u.Query()
	q.Set("database", newDB)
	u.RawQuery = q.Encode()
	return u.String()
}

// mssqlQuoteIdentifier quotes an identifier with square brackets for SQL Server.
func mssqlQuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "]", "]]")
	return "[" + escaped + "]"
}

// mssqlPlaceholder returns a SQL Server positional parameter placeholder.
func mssqlPlaceholder(idx int) string {
	return fmt.Sprintf("@p%d", idx)
}
