package export

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/minee/tdb/internal/db"
)

// ExportCSV writes query results as CSV to the given writer.
// Header row followed by data rows. NULL values are exported as empty strings.
func ExportCSV(result *db.QueryResult, writer io.Writer) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}

	w := csv.NewWriter(writer)
	defer w.Flush()

	// Write header row
	if err := w.Write(result.Columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data rows
	for rowIdx, row := range result.Rows {
		record := make([]string, len(result.Columns))
		for i := 0; i < len(result.Columns); i++ {
			if i < len(row) {
				record[i] = formatCSVValue(row[i])
			}
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("failed to write row %d: %w", rowIdx, err)
		}
	}

	return nil
}

// formatCSVValue formats a value for CSV output.
// NULL values become empty strings.
func formatCSVValue(val interface{}) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}
