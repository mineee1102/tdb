package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresDriver implements the Driver interface for PostgreSQL.
type PostgresDriver struct {
	db  *sql.DB
	dsn string
}

// pgTx wraps sql.Tx to implement the Transaction interface.
type pgTx struct {
	tx *sql.Tx
}

func (t *pgTx) Commit() error {
	return t.tx.Commit()
}

func (t *pgTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *pgTx) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("transaction exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (t *pgTx) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("transaction query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *PostgresDriver) Connect(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}
	d.db = db
	d.dsn = dsn
	return nil
}

func (d *PostgresDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *PostgresDriver) DriverName() DriverType {
	return Postgres
}

func (d *PostgresDriver) ListSchemas(ctx context.Context) ([]string, error) {
	query := `SELECT schema_name FROM information_schema.schemata 
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast') 
		ORDER BY schema_name`
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

func (d *PostgresDriver) ListTables(ctx context.Context, schema string) ([]TableInfo, error) {
	// Get tables and views from information_schema
	query := `SELECT table_schema, table_name, 
		CASE table_type WHEN 'BASE TABLE' THEN 'table' WHEN 'VIEW' THEN 'view' ELSE table_type END as type
		FROM information_schema.tables 
		WHERE table_schema = $1
		UNION ALL
		SELECT schemaname, matviewname, 'view'
		FROM pg_matviews
		WHERE schemaname = $1
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

func (d *PostgresDriver) DescribeTable(ctx context.Context, schema, table string) (*TableDescription, error) {
	desc := &TableDescription{
		Schema: schema,
		Name:   table,
	}

	// Get columns
	colQuery := `SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 
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
	pkQuery := `SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position`
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
	idxQuery := `SELECT indexname, indexdef
		FROM pg_indexes 
		WHERE schemaname = $1 AND tablename = $2`
	idxRows, err := d.db.QueryContext(ctx, idxQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer idxRows.Close()

	for idxRows.Next() {
		var idx IndexInfo
		var indexDef string
		if err := idxRows.Scan(&idx.Name, &indexDef); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}
		idx.IsUnique = strings.Contains(strings.ToUpper(indexDef), "UNIQUE")
		// Parse columns from index definition
		idx.Columns = parseIndexColumns(indexDef)
		desc.Indexes = append(desc.Indexes, idx)
	}
	if err := idxRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	// Get foreign keys
	fkQuery := `SELECT 
		tc.constraint_name,
		kcu.column_name,
		ccu.table_name AS ref_table,
		ccu.column_name AS ref_column
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu 
			ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
		WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type = 'FOREIGN KEY'
		ORDER BY tc.constraint_name, kcu.ordinal_position`
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

func (d *PostgresDriver) GetRows(ctx context.Context, schema, table string, opts GetRowsOptions) (*QueryResult, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s", pgQuoteIdentifier(schema), pgQuoteIdentifier(table))

	if opts.Where != "" {
		query += " WHERE " + opts.Where
	}
	if opts.OrderBy != "" {
		dir := "ASC"
		if strings.ToUpper(opts.OrderDir) == "DESC" {
			dir = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", pgQuoteIdentifier(opts.OrderBy), dir)
	}
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	return d.Query(ctx, query, opts.WhereArgs...)
}

func (d *PostgresDriver) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *PostgresDriver) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (d *PostgresDriver) InsertRow(ctx context.Context, schema, table string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholders := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	i := 1
	for col, val := range values {
		columns = append(columns, pgQuoteIdentifier(col))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, val)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		pgQuoteIdentifier(schema), pgQuoteIdentifier(table),
		strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

func (d *PostgresDriver) UpdateRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}, values map[string]interface{}) error {
	setClauses := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values)+len(primaryKeys))
	idx := 1
	for col, val := range values {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", pgQuoteIdentifier(col), idx))
		args = append(args, val)
		idx++
	}

	whereClause, whereArgs := buildWhereClause(primaryKeys, pgQuoteIdentifier, pgPlaceholder, idx)
	args = append(args, whereArgs...)

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		pgQuoteIdentifier(schema), pgQuoteIdentifier(table),
		strings.Join(setClauses, ", "), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

func (d *PostgresDriver) DeleteRow(ctx context.Context, schema, table string, primaryKeys map[string]interface{}) error {
	whereClause, args := buildWhereClause(primaryKeys, pgQuoteIdentifier, pgPlaceholder, 1)

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		pgQuoteIdentifier(schema), pgQuoteIdentifier(table), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

func (d *PostgresDriver) Begin(ctx context.Context) (Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &pgTx{tx: tx}, nil
}

func (d *PostgresDriver) ListDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname`
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

func (d *PostgresDriver) CurrentDatabase(ctx context.Context) (string, error) {
	var name string
	err := d.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	return name, nil
}

func (d *PostgresDriver) SwitchDatabase(ctx context.Context, dbName string) error {
	// PostgreSQL requires a new connection to switch databases.
	// Build a new DSN with the target database name.
	newDSN := pgReplaceDatabaseInDSN(d.dsn, dbName)

	newDB, err := sql.Open("pgx", newDSN)
	if err != nil {
		return fmt.Errorf("failed to open new postgres connection: %w", err)
	}
	if err := newDB.PingContext(ctx); err != nil {
		newDB.Close()
		return fmt.Errorf("failed to ping new database %q: %w", dbName, err)
	}

	// Close old connection and swap
	d.db.Close()
	d.db = newDB
	d.dsn = newDSN
	return nil
}

// pgQuoteIdentifier quotes an identifier to prevent SQL injection.
func pgQuoteIdentifier(name string) string {
	// Replace any double quotes with two double quotes (standard SQL escaping)
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// pgPlaceholder returns a PostgreSQL positional parameter placeholder.
func pgPlaceholder(idx int) string {
	return fmt.Sprintf("$%d", idx)
}

// parseIndexColumns extracts column names from a PostgreSQL index definition.
func parseIndexColumns(indexDef string) []string {
	// Index definitions look like: CREATE [UNIQUE] INDEX name ON schema.table USING btree (col1, col2)
	start := strings.LastIndex(indexDef, "(")
	end := strings.LastIndex(indexDef, ")")
	if start < 0 || end < 0 || end <= start {
		return nil
	}
	colStr := indexDef[start+1 : end]
	parts := strings.Split(colStr, ",")
	columns := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		// Remove sort direction and other modifiers
		col = strings.Fields(col)[0]
		// Remove quotes if present
		col = strings.Trim(col, `"`)
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

// pgReplaceDatabaseInDSN replaces the database name in a PostgreSQL DSN.
// Supports both URI format (postgres://user:pass@host:port/dbname) and
// keyword=value format (host=localhost dbname=mydb).
func pgReplaceDatabaseInDSN(dsn, newDB string) string {
	// Try URI format first
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err == nil {
			u.Path = "/" + newDB
			return u.String()
		}
	}

	// keyword=value format: replace dbname=xxx
	if strings.Contains(dsn, "dbname=") {
		// Handle both quoted and unquoted values
		parts := strings.Fields(dsn)
		for i, p := range parts {
			if strings.HasPrefix(p, "dbname=") {
				parts[i] = "dbname=" + newDB
				return strings.Join(parts, " ")
			}
		}
	}

	// If no dbname found, append it
	return dsn + " dbname=" + newDB
}
