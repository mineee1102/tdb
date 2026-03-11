package ui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minee/tdb/internal/db"
)

func TestStructureViewModel_TabSwitching(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.SetSize(80, 30)

	if m.activeTab != TabColumns {
		t.Error("Expected initial tab to be TabColumns")
	}

	// Switch to Indexes tab
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})
	if m.activeTab != TabIndexes {
		t.Errorf("Expected TabIndexes, got %d", m.activeTab)
	}

	// Switch to Foreign Keys tab
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")})
	if m.activeTab != TabForeignKeys {
		t.Errorf("Expected TabForeignKeys, got %d", m.activeTab)
	}

	// Tab cycles back to Columns
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.activeTab != TabColumns {
		t.Errorf("Expected TabColumns after tab cycle, got %d", m.activeTab)
	}
}

func TestStructureViewModel_Navigation(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.SetSize(80, 30)

	defVal := "0"
	m.desc = &db.TableDescription{
		Schema: "public",
		Name:   "users",
		Columns: []db.ColumnInfo{
			{Name: "id", DataType: "integer", Nullable: false, IsPrimaryKey: true},
			{Name: "name", DataType: "varchar(255)", Nullable: false},
			{Name: "email", DataType: "varchar(255)", Nullable: true},
			{Name: "age", DataType: "integer", Nullable: true, DefaultValue: &defVal},
		},
	}

	if m.cursorRow != 0 {
		t.Error("Expected initial cursor at row 0")
	}

	// Navigate down
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.cursorRow != 1 {
		t.Errorf("Expected cursor at row 1, got %d", m.cursorRow)
	}

	// Navigate up
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	if m.cursorRow != 0 {
		t.Errorf("Expected cursor at row 0, got %d", m.cursorRow)
	}
}

func TestStructureViewModel_AddColumnDDL(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.SetSize(80, 30)
	m.schema = "public"
	m.table = "users"
	m.desc = &db.TableDescription{
		Schema: "public",
		Name:   "users",
		Columns: []db.ColumnInfo{
			{Name: "id", DataType: "integer", Nullable: false, IsPrimaryKey: true},
		},
	}

	// Start add column mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})
	if m.ddlMode != ddlAddColumn {
		t.Error("Expected ddlAddColumn mode")
	}
	if len(m.ddlFields) != 4 {
		t.Errorf("Expected 4 DDL fields, got %d", len(m.ddlFields))
	}
}

func TestStructureViewModel_DropColumnDDL(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.SetSize(80, 30)
	m.schema = "public"
	m.table = "users"
	m.desc = &db.TableDescription{
		Schema: "public",
		Name:   "users",
		Columns: []db.ColumnInfo{
			{Name: "id", DataType: "integer", Nullable: false, IsPrimaryKey: true},
			{Name: "name", DataType: "varchar(255)", Nullable: false},
		},
	}

	// Select second column (name)
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})

	// Press d to drop
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if m.ddlMode != ddlDropColumn {
		t.Error("Expected ddlDropColumn mode")
	}
	if !m.ddlConfirming {
		t.Error("Expected confirming state")
	}
	if m.ddlPreview == "" {
		t.Error("Expected DDL preview to be set")
	}
}

func TestStructureViewModel_BuildDDLStatements(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.schema = "public"
	m.table = "users"

	// Test drop column DDL
	ddl := m.buildDropColumnDDL("email")
	expected := `ALTER TABLE "public"."users" DROP COLUMN "email"`
	if ddl != expected {
		t.Errorf("Expected '%s', got '%s'", expected, ddl)
	}

	// Test drop table DDL
	ddl = m.buildDropTableDDL()
	expected = `DROP TABLE "public"."users"`
	if ddl != expected {
		t.Errorf("Expected '%s', got '%s'", expected, ddl)
	}

	// Test create index DDL
	m.ddlFields = []ddlField{
		{label: "Index Name", value: "idx_users_email"},
		{label: "Columns", value: "email, name"},
		{label: "Unique", value: "y"},
	}
	m.ddlMode = ddlCreateIndex
	ddl = m.buildCreateIndexDDL()
	if ddl == "" {
		t.Error("Expected non-empty DDL for create index")
	}
}

func TestStructureViewModel_View(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.SetSize(80, 30)
	m.SetFocused(true)

	// View with no data
	view := m.View()
	if view == "" {
		t.Error("Expected non-empty view")
	}

	defVal := "hello"
	// View with data
	m.desc = &db.TableDescription{
		Schema: "public",
		Name:   "users",
		Columns: []db.ColumnInfo{
			{Name: "id", DataType: "integer", Nullable: false, IsPrimaryKey: true},
			{Name: "name", DataType: "varchar(255)", Nullable: true, DefaultValue: &defVal},
		},
		Indexes: []db.IndexInfo{
			{Name: "pk_users", Columns: []string{"id"}, IsUnique: true},
		},
		ForeignKeys: []db.ForeignKeyInfo{
			{Name: "fk_order_user", Columns: []string{"user_id"}, RefTable: "users", RefColumns: []string{"id"}},
		},
	}

	// Columns tab
	view = m.View()
	if view == "" {
		t.Error("Expected non-empty view with columns")
	}

	// Indexes tab
	m.activeTab = TabIndexes
	view = m.View()
	if view == "" {
		t.Error("Expected non-empty view with indexes")
	}

	// Foreign keys tab
	m.activeTab = TabForeignKeys
	view = m.View()
	if view == "" {
		t.Error("Expected non-empty view with foreign keys")
	}
}

func TestStructureViewModel_CreateTableDDL(t *testing.T) {
	theme := DarkTheme()
	m := NewStructureViewModel(nil, theme)
	m.schema = "public"
	m.createTableName = "orders"
	m.createTableColumns = []createColumnDef{
		{name: "id", dataType: "INTEGER", nullable: false},
		{name: "user_id", dataType: "INTEGER", nullable: false},
		{name: "total", dataType: "DECIMAL(10,2)", nullable: true, defVal: "0"},
	}

	ddl := m.buildCreateTableDDL()
	if ddl == "" {
		t.Error("Expected non-empty DDL")
	}

	// Should contain CREATE TABLE
	if !stringContains(ddl, "CREATE TABLE") {
		t.Errorf("Expected CREATE TABLE in DDL: %s", ddl)
	}

	// Should contain column definitions
	if !stringContains(ddl, `"id"`) {
		t.Errorf("Expected column 'id' in DDL: %s", ddl)
	}
	if !stringContains(ddl, "NOT NULL") {
		t.Errorf("Expected NOT NULL in DDL: %s", ddl)
	}
	if !stringContains(ddl, "DEFAULT 0") {
		t.Errorf("Expected DEFAULT 0 in DDL: %s", ddl)
	}
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
