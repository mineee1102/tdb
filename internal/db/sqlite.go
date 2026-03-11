package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// SQLiteDriver implements the Driver interface for SQLite.
type SQLiteDriver struct {
	db  *sql.DB
	dsn string
}

// sqliteTx wraps sql.Tx to implement the Transaction interface.
type sqliteTx struct {
	tx *sql.Tx
}

func (t *sqliteTx) Commit() error {
	return t.tx.Commit()
}

func (t *sqliteTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *sqliteTx) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("transaction exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (t *sqliteTx) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("transaction query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *SQLiteDriver) Connect(ctx context.Context, dsn string) error {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping sqlite: %w", err)
	}
	d.db = db
	d.dsn = dsn
	return nil
}

func (d *SQLiteDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *SQLiteDriver) DriverName() DriverType {
	return SQLite
}

func (d *SQLiteDriver) ListSchemas(_ context.Context) ([]string, error) {
	// SQLite does not have a schema concept; return "main" as default
	return []string{"main"}, nil
}

func (d *SQLiteDriver) ListTables(ctx context.Context, _ string) ([]TableInfo, error) {
	query := `SELECT name, type FROM sqlite_master 
		WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
		ORDER BY type, name`
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		t.Schema = "main"
		if err := rows.Scan(&t.Name, &t.Type); err != nil {
			return nil, fmt.Errorf("failed to scan table info: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (d *SQLiteDriver) DescribeTable(ctx context.Context, _, table string) (*TableDescription, error) {
	desc := &TableDescription{
		Schema: "main",
		Name:   table,
	}

	// Get columns using PRAGMA table_info
	pragmaQuery := fmt.Sprintf("PRAGMA table_info(%s)", sqliteQuoteIdentifier(table))
	colRows, err := d.db.QueryContext(ctx, pragmaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultVal sql.NullString
		if err := colRows.Scan(&cid, &name, &dataType, &notNull, &defaultVal, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col := ColumnInfo{
			Name:         name,
			DataType:     dataType,
			Nullable:     notNull == 0,
			IsPrimaryKey: pk > 0,
		}
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		if col.IsPrimaryKey {
			desc.PrimaryKeys = append(desc.PrimaryKeys, name)
		}
		desc.Columns = append(desc.Columns, col)
	}
	if err := colRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Get indexes using PRAGMA index_list
	idxListQuery := fmt.Sprintf("PRAGMA index_list(%s)", sqliteQuoteIdentifier(table))
	idxRows, err := d.db.QueryContext(ctx, idxListQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer idxRows.Close()

	for idxRows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		if err := idxRows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		idx := IndexInfo{
			Name:     name,
			IsUnique: unique == 1,
		}

		// Get index columns using PRAGMA index_info
		idxInfoQuery := fmt.Sprintf("PRAGMA index_info(%s)", sqliteQuoteIdentifier(name))
		infoRows, err := d.db.QueryContext(ctx, idxInfoQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to get index info: %w", err)
		}

		for infoRows.Next() {
			var seqNo, cid int
			var colName string
			if err := infoRows.Scan(&seqNo, &cid, &colName); err != nil {
				infoRows.Close()
				return nil, fmt.Errorf("failed to scan index column: %w", err)
			}
			idx.Columns = append(idx.Columns, colName)
		}
		infoRows.Close()

		desc.Indexes = append(desc.Indexes, idx)
	}
	if err := idxRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	// Get foreign keys using PRAGMA foreign_key_list
	fkQuery := fmt.Sprintf("PRAGMA foreign_key_list(%s)", sqliteQuoteIdentifier(table))
	fkRows, err := d.db.QueryContext(ctx, fkQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer fkRows.Close()

	fkMap := make(map[int]*ForeignKeyInfo)
	for fkRows.Next() {
		var id, seq int
		var refTable, from, to, onUpdate, onDelete, match string
		if err := fkRows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}
		if fk, ok := fkMap[id]; ok {
			fk.Columns = append(fk.Columns, from)
			fk.RefColumns = append(fk.RefColumns, to)
		} else {
			fkMap[id] = &ForeignKeyInfo{
				Name:       fmt.Sprintf("fk_%s_%d", table, id),
				Columns:    []string{from},
				RefTable:   refTable,
				RefColumns: []string{to},
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

func (d *SQLiteDriver) GetRows(ctx context.Context, _, table string, opts GetRowsOptions) (*QueryResult, error) {
	query := fmt.Sprintf("SELECT * FROM %s", sqliteQuoteIdentifier(table))

	if opts.Where != "" {
		query += " WHERE " + opts.Where
	}
	if opts.OrderBy != "" {
		dir := "ASC"
		if strings.ToUpper(opts.OrderDir) == "DESC" {
			dir = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", sqliteQuoteIdentifier(opts.OrderBy), dir)
	}
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	return d.Query(ctx, query, opts.WhereArgs...)
}

func (d *SQLiteDriver) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (d *SQLiteDriver) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec failed: %w", err)
	}
	return result.RowsAffected()
}

func (d *SQLiteDriver) InsertRow(ctx context.Context, _, table string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholders := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	for col, val := range values {
		columns = append(columns, sqliteQuoteIdentifier(col))
		placeholders = append(placeholders, "?")
		args = append(args, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		sqliteQuoteIdentifier(table),
		strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

func (d *SQLiteDriver) UpdateRow(ctx context.Context, _, table string, primaryKeys map[string]interface{}, values map[string]interface{}) error {
	setClauses := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values)+len(primaryKeys))
	for col, val := range values {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", sqliteQuoteIdentifier(col)))
		args = append(args, val)
	}

	whereClause, whereArgs := buildWhereClause(primaryKeys, sqliteQuoteIdentifier, sqlitePlaceholder, 0)
	args = append(args, whereArgs...)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		sqliteQuoteIdentifier(table),
		strings.Join(setClauses, ", "), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

func (d *SQLiteDriver) DeleteRow(ctx context.Context, _, table string, primaryKeys map[string]interface{}) error {
	whereClause, args := buildWhereClause(primaryKeys, sqliteQuoteIdentifier, sqlitePlaceholder, 0)

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		sqliteQuoteIdentifier(table), whereClause)

	_, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

func (d *SQLiteDriver) Begin(ctx context.Context) (Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &sqliteTx{tx: tx}, nil
}

func (d *SQLiteDriver) ListDatabases(_ context.Context) ([]string, error) {
	// SQLite is file-based; there is no concept of multiple databases.
	return []string{}, nil
}

func (d *SQLiteDriver) CurrentDatabase(_ context.Context) (string, error) {
	return d.dsn, nil
}

func (d *SQLiteDriver) SwitchDatabase(_ context.Context, _ string) error {
	return fmt.Errorf("sqlite does not support database switching")
}

// sqliteQuoteIdentifier quotes an identifier with double quotes for SQLite.
func sqliteQuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// sqlitePlaceholder returns a SQLite placeholder (always "?").
func sqlitePlaceholder(_ int) string {
	return "?"
}
