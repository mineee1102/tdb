package db

import (
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// scanRows reads all rows from a *sql.Rows and returns a QueryResult.
// This is a common helper used by all database drivers.
func scanRows(rows *sql.Rows) (*QueryResult, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	columns := make([]string, len(colTypes))
	typeNames := make([]string, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = ct.Name()
		typeNames[i] = ct.DatabaseTypeName()
	}

	var resultRows [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} pointers for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to user-friendly types
		row := make([]interface{}, len(columns))
		for i, v := range values {
			row[i] = convertValue(v)
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &QueryResult{
		Columns:     columns,
		ColumnTypes: typeNames,
		Rows:        resultRows,
	}, nil
}

// convertValue converts database driver-specific types to standard Go types.
func convertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return val
	}
}

// buildWhereClause builds a WHERE clause from primary key values with parameterized placeholders.
// placeholderFunc generates the placeholder for each parameter index (e.g., "$1" for Postgres, "?" for MySQL).
// Keys are sorted to ensure deterministic clause ordering.
func buildWhereClause(primaryKeys map[string]interface{}, quoteFunc func(string) string, placeholderFunc func(int) string, startIdx int) (string, []interface{}) {
	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(primaryKeys))
	for col := range primaryKeys {
		keys = append(keys, col)
	}
	sort.Strings(keys)

	clause := ""
	args := make([]interface{}, 0, len(primaryKeys))
	idx := startIdx
	for _, col := range keys {
		if clause != "" {
			clause += " AND "
		}
		clause += quoteFunc(col) + " = " + placeholderFunc(idx)
		args = append(args, primaryKeys[col])
		idx++
	}
	return clause, args
}
