package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLDriver implements the Driver interface for MySQL.
type MySQLDriver struct {
	db  *sql.DB
	dsn string
}

// mysqlTx wraps sql.Tx to implement the Transaction interface.
type mysqlTx struct {
	tx *sql.Tx
}

func (t *mysqlTx) Commit() error {
	return t.tx.Commit()
}

func (t *mysqlTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *mysqlTx) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("transaction exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (t *mysqlTx) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("transaction query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *MySQLDriver) Connect(ctx context.Context, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping mysql: %w", err)
	}
	d.db = db
	d.dsn = dsn
	return nil
}

func (d *MySQLDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *MySQLDriver) DriverName() DriverType {
	return MySQL
}

func (d *MySQLDriver) ListSchemas(ctx context.Context) ([]string, error) {
	// In MySQL, schemas are equivalent to databases.
	// Return only the current database so the table list shows only its tables.
	currentDB, err := d.CurrentDatabase(ctx)
	if err != nil {
		return nil, err
	}
	if currentDB == "" {
		return nil, nil
	}
	return []string{currentDB}, nil
}

func (d *MySQLDriver) ListTables(ctx context.Context, schema string) ([]TableInfo, error) {
	query := `SELECT table_schema, table_name, 
		CASE table_type WHEN 'BASE TABLE' THEN 'table' WHEN 'VIEW' THEN 'view' ELSE table_type END as type
		FROM information_schema.tables 
		WHERE table_schema = ?
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

func (d *MySQLDriver) DescribeTable(ctx context.Context, schema, table string) (*TableDescription, error) {
	desc := &TableDescription{
		Schema: schema,
		Name:   table,
	}

	// Get columns
	colQuery := `SELECT column_name, column_type, is_nullable, column_default, column_key
		FROM information_schema.columns 
		WHERE table_schema = ? AND table_name = ? 
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
		var columnKey string
		if err := colRows.Scan(&col.Name, &col.DataType, &nullable, &defaultVal, &columnKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.Nullable = nullable == "YES"
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		col.IsPrimaryKey = columnKey == "PRI"
		if col.IsPrimaryKey {
			desc.PrimaryKeys = append(desc.PrimaryKeys, col.Name)
		}
		desc.Columns = append(desc.Columns, col)
	}
	if err := colRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Get indexes
	idxQuery := fmt.Sprintf("SHOW INDEX FROM %s.%s", mysqlQuoteIdentifier(schema), mysqlQuoteIdentifier(table))
	idxRows, err := d.db.QueryContext(ctx, idxQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer idxRows.Close()

	idxMap := make(map[string]*IndexInfo)
	for idxRows.Next() {
		cols, err := idxRows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get index columns: %w", err)
		}
		// Allocate a slice for all columns; we only extract the ones we need
		vals := make([]interface{}, len(cols))
		for i := range vals {
			vals[i] = new(interface{})
		}
		if err := idxRows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}
		// SHOW INDEX columns: Table(0), Non_unique(1), Key_name(2), Seq_in_index(3), Column_name(4), ...
		nonUnique := formatInterfaceVal(*vals[1].(*interface{}))
		keyName := formatInterfaceVal(*vals[2].(*interface{}))
		colName := formatInterfaceVal(*vals[4].(*interface{}))
		if idx, ok := idxMap[keyName]; ok {
			idx.Columns = append(idx.Columns, colName)
		} else {
			idxMap[keyName] = &IndexInfo{
				Name:     keyName,
				Columns:  []string{colName},
				IsUnique: nonUnique == "0",
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
	fkQuery := `SELECT constraint_name, column_name, referenced_table_name, referenced_column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND referenced_table_name IS NOT NULL
		ORDER BY constraint_name, ordinal_position`
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

func (d *MySQLDriver) GetRows(ctx context.Context, schema, table string, opts GetRowsOptions) (*QueryResult, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s", mysqlQuoteIdentifier(schema), mysqlQuoteIdentifier(table))

	if opts.Where != "" {
		query += " WHERE " + opts.Where
	}
	if opts.OrderBy != "" {
		dir := "ASC"
		if strings.ToUpper(opts.OrderDir) == "DESC" {
			dir = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", mysqlQuoteIdentifier(opts.OrderBy), dir)
	}
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	return d.Query(ctx, query, opts.WhereArgs...)
}

func (d *MySQLDriver) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *MySQLDriver) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (d *MySQLDriver) InsertRow(ctx context.Context, schema, table string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholders := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	for col, val := range values {
		columns = append(columns, mysqlQuoteIdentifier(col))
		placeholders = append(placeholders, "?")
		args = append(args, val)
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		mysqlQuoteIdentifier(schema), mysqlQuoteIdentifier(table),
		strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

func (d *MySQLDriver) UpdateRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}, values map[string]interface{}) error {
	setClauses := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values)+len(primaryKeys))
	for col, val := range values {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", mysqlQuoteIdentifier(col)))
		args = append(args, val)
	}

	whereClause, whereArgs := buildWhereClause(primaryKeys, mysqlQuoteIdentifier, mysqlPlaceholder, 0)
	args = append(args, whereArgs...)

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		mysqlQuoteIdentifier(schema), mysqlQuoteIdentifier(table),
		strings.Join(setClauses, ", "), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

func (d *MySQLDriver) DeleteRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}) error {
	whereClause, args := buildWhereClause(primaryKeys, mysqlQuoteIdentifier, mysqlPlaceholder, 0)

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		mysqlQuoteIdentifier(schema), mysqlQuoteIdentifier(table), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

func (d *MySQLDriver) Begin(ctx context.Context) (Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &mysqlTx{tx: tx}, nil
}

func (d *MySQLDriver) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, "SHOW DATABASES")
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
		// Skip system databases
		if name == "information_schema" || name == "performance_schema" || name == "mysql" || name == "sys" {
			continue
		}
		databases = append(databases, name)
	}
	return databases, rows.Err()
}

func (d *MySQLDriver) CurrentDatabase(ctx context.Context) (string, error) {
	var name sql.NullString
	err := d.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&name)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	if !name.Valid {
		return "", nil
	}
	return name.String, nil
}

func (d *MySQLDriver) SwitchDatabase(ctx context.Context, dbName string) error {
	// Reconnect with the new database name in the DSN.
	// Using USE statement is not safe with connection pools because it only
	// affects a single connection, not the entire pool.
	newDSN := mysqlReplaceDatabaseInDSN(d.dsn, dbName)
	newDB, err := sql.Open("mysql", newDSN)
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

// mysqlReplaceDatabaseInDSN replaces the database name in a MySQL DSN.
// MySQL DSN format: [user[:password]@][net[(addr)]]/dbname[?param1=value1&...]
// The database name is between the first "/" after the address part and the "?" (or end of string).
func mysqlReplaceDatabaseInDSN(dsn, newDB string) string {
	// Find the slash that separates the address from the database name.
	// In the MySQL DSN format, this is the slash after the ')' of tcp(...) or after '@'.
	slashIdx := strings.LastIndex(dsn, "/")
	if slashIdx < 0 {
		// No slash found; append /dbname
		return dsn + "/" + newDB
	}

	prefix := dsn[:slashIdx+1] // everything up to and including '/'

	rest := dsn[slashIdx+1:] // dbname?params...
	if qIdx := strings.Index(rest, "?"); qIdx >= 0 {
		// Preserve query parameters
		return prefix + newDB + rest[qIdx:]
	}
	return prefix + newDB
}

// formatInterfaceVal converts an interface{} value to string,
// handling []byte which MySQL driver returns for some columns.
func formatInterfaceVal(v interface{}) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

// mysqlQuoteIdentifier quotes an identifier with backticks for MySQL.
func mysqlQuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

// mysqlPlaceholder returns a MySQL placeholder (always "?").
func mysqlPlaceholder(_ int) string {
	return "?"
}
