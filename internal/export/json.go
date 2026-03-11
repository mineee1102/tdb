package export

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/minee/tdb/internal/db"
)

// ExportJSON writes query results as a JSON array of objects to the given writer.
// Each row is an object with column names as keys. Output is indented for readability.
func ExportJSON(result *db.QueryResult, writer io.Writer) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}

	// Build array of row objects
	rows := make([]map[string]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		obj := make(map[string]interface{}, len(result.Columns))
		for i, col := range result.Columns {
			if i < len(row) {
				obj[col] = row[i]
			} else {
				obj[col] = nil
			}
		}
		rows = append(rows, obj)
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(rows); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}
