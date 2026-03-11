package export

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/minee/tdb/internal/db"
)

// ── CSV Tests ─────────────────────────────────────────────────────

func TestExportCSV_Basic(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{1, "Alice", 30},
			{2, "Bob", 25},
		},
	}

	var buf bytes.Buffer
	err := ExportCSV(result, &buf)
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines (header + 2 rows), got %d", len(lines))
	}
	if lines[0] != "id,name,age" {
		t.Errorf("expected header 'id,name,age', got '%s'", lines[0])
	}
	if lines[1] != "1,Alice,30" {
		t.Errorf("expected '1,Alice,30', got '%s'", lines[1])
	}
}

func TestExportCSV_NullValues(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, nil},
		},
	}

	var buf bytes.Buffer
	err := ExportCSV(result, &buf)
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	output := buf.String()
	// NULL should be exported as empty string
	if !strings.Contains(output, "1,") {
		t.Errorf("expected NULL to be empty string, got: %s", output)
	}
}

func TestExportCSV_NilResult(t *testing.T) {
	var buf bytes.Buffer
	err := ExportCSV(nil, &buf)
	if err == nil {
		t.Error("expected error for nil result")
	}
}

func TestExportCSV_EmptyResult(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name"},
		Rows:    [][]interface{}{},
	}

	var buf bytes.Buffer
	err := ExportCSV(result, &buf)
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line (header only), got %d", len(lines))
	}
}

func TestExportCSV_SpecialCharacters(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "text"},
		Rows: [][]interface{}{
			{1, `value with "quotes" and, commas`},
		},
	}

	var buf bytes.Buffer
	err := ExportCSV(result, &buf)
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	output := buf.String()
	// CSV should properly quote fields with commas and double quotes
	if !strings.Contains(output, `"value with ""quotes"" and, commas"`) {
		t.Errorf("expected properly escaped CSV, got: %s", output)
	}
}

// ── JSON Tests ────────────────────────────────────────────────────

func TestExportJSON_Basic(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{float64(1), "Alice", float64(30)},
			{float64(2), "Bob", float64(25)},
		},
	}

	var buf bytes.Buffer
	err := ExportJSON(result, &buf)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", rows[0]["name"])
	}
	if rows[1]["age"] != float64(25) {
		t.Errorf("expected 25, got %v", rows[1]["age"])
	}
}

func TestExportJSON_NullValues(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{float64(1), nil},
		},
	}

	var buf bytes.Buffer
	err := ExportJSON(result, &buf)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if rows[0]["name"] != nil {
		t.Errorf("expected nil for NULL value, got %v", rows[0]["name"])
	}
}

func TestExportJSON_NilResult(t *testing.T) {
	var buf bytes.Buffer
	err := ExportJSON(nil, &buf)
	if err == nil {
		t.Error("expected error for nil result")
	}
}

func TestExportJSON_EmptyResult(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id"},
		Rows:    [][]interface{}{},
	}

	var buf bytes.Buffer
	err := ExportJSON(result, &buf)
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &rows); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected empty array, got %d items", len(rows))
	}
}

// ── SQL Tests ─────────────────────────────────────────────────────

func TestExportSQL_Basic(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{1, "Alice", 30},
			{2, "Bob", 25},
		},
	}

	var buf bytes.Buffer
	err := ExportSQL(result, "users", &buf)
	if err != nil {
		t.Fatalf("ExportSQL failed: %v", err)
	}

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 INSERT statements, got %d", len(lines))
	}
	if !strings.Contains(lines[0], `INSERT INTO "users"`) {
		t.Errorf("expected INSERT INTO statement, got: %s", lines[0])
	}
	if !strings.Contains(lines[0], "'Alice'") {
		t.Errorf("expected quoted string value, got: %s", lines[0])
	}
}

func TestExportSQL_NullValues(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, nil},
		},
	}

	var buf bytes.Buffer
	err := ExportSQL(result, "users", &buf)
	if err != nil {
		t.Fatalf("ExportSQL failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "NULL") {
		t.Errorf("expected NULL keyword, got: %s", output)
	}
}

func TestExportSQL_EscapedStrings(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "text"},
		Rows: [][]interface{}{
			{1, "it's a test"},
		},
	}

	var buf bytes.Buffer
	err := ExportSQL(result, "data", &buf)
	if err != nil {
		t.Fatalf("ExportSQL failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "'it''s a test'") {
		t.Errorf("expected escaped single quote, got: %s", output)
	}
}

func TestExportSQL_NilResult(t *testing.T) {
	var buf bytes.Buffer
	err := ExportSQL(nil, "users", &buf)
	if err == nil {
		t.Error("expected error for nil result")
	}
}

func TestExportSQL_EmptyTableName(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id"},
		Rows:    [][]interface{}{},
	}

	var buf bytes.Buffer
	err := ExportSQL(result, "", &buf)
	if err == nil {
		t.Error("expected error for empty table name")
	}
}

func TestExportSQL_BoolValues(t *testing.T) {
	result := &db.QueryResult{
		Columns: []string{"id", "active"},
		Rows: [][]interface{}{
			{1, true},
			{2, false},
		},
	}

	var buf bytes.Buffer
	err := ExportSQL(result, "users", &buf)
	if err != nil {
		t.Fatalf("ExportSQL failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "TRUE") {
		t.Errorf("expected TRUE, got: %s", output)
	}
	if !strings.Contains(output, "FALSE") {
		t.Errorf("expected FALSE, got: %s", output)
	}
}
