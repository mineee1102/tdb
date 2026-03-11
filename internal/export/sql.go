package export

import (
	"fmt"
	"io"
	"strings"

	"github.com/minee/tdb/internal/db"
)

// ExportSQL writes query results as INSERT INTO statements to the given writer.
// String values are properly escaped. NULL values use the NULL keyword.
func ExportSQL(result *db.QueryResult, tableName string, writer io.Writer) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}
	if tableName == "" {
		return fmt.Errorf("table name is empty")
	}

	// Quote column names
	quotedCols := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		quotedCols[i] = quoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	for rowIdx, row := range result.Rows {
		values := make([]string, len(result.Columns))
		for i := 0; i < len(result.Columns); i++ {
			if i < len(row) {
				values[i] = formatSQLValue(row[i])
			} else {
				values[i] = "NULL"
			}
		}
		valList := strings.Join(values, ", ")

		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n",
			quoteIdentifier(tableName), colList, valList)

		if _, err := io.WriteString(writer, stmt); err != nil {
			return fmt.Errorf("failed to write row %d: %w", rowIdx, err)
		}
	}

	return nil
}

// quoteIdentifier quotes a SQL identifier with double quotes.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// formatSQLValue formats a value for use in a SQL INSERT statement.
func formatSQLValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", v)
	default:
		// Treat as string
		s := fmt.Sprintf("%v", v)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}
