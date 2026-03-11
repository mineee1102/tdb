package ui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minee/tdb/internal/db"
)

// ── Theme tests ───────────────────────────────────────────────────

func TestDarkThemeNotNil(t *testing.T) {
	theme := DarkTheme()
	if theme == nil {
		t.Fatal("DarkTheme() should not return nil")
	}
}

// ── Help tests ────────────────────────────────────────────────────

func TestHelpToggle(t *testing.T) {
	theme := DarkTheme()
	h := NewHelpModel(theme)

	if h.IsVisible() {
		t.Error("Help should be hidden by default")
	}

	h.Toggle()
	if !h.IsVisible() {
		t.Error("Help should be visible after toggle")
	}

	h.Toggle()
	if h.IsVisible() {
		t.Error("Help should be hidden after second toggle")
	}
}

func TestHelpViewContextual(t *testing.T) {
	theme := DarkTheme()
	h := NewHelpModel(theme)

	// Should return empty when not visible
	view := h.View(PaneTableList, 80, 24)
	if view != "" {
		t.Error("Help.View() should return empty when not visible")
	}

	// Should return content when visible
	h.Toggle()
	view = h.View(PaneTableList, 80, 24)
	if view == "" {
		t.Error("Help.View() should return content when visible")
	}
}

// ── StatusBar tests ───────────────────────────────────────────────

func TestStatusBarView(t *testing.T) {
	theme := DarkTheme()
	sb := NewStatusBarModel(theme)
	sb.SetWidth(100)
	sb.SetConnectionInfo("postgres@localhost:5432/mydb")
	sb.SetRowCount(42)

	view := sb.View()
	if view == "" {
		t.Error("StatusBar.View() should not be empty")
	}
}

func TestStatusBarMessage(t *testing.T) {
	theme := DarkTheme()
	sb := NewStatusBarModel(theme)
	sb.SetWidth(100)

	sb.SetMessage("Error occurred", "error")
	view := sb.View()
	if view == "" {
		t.Error("StatusBar should render with error message")
	}
}

// ── TableList tests ───────────────────────────────────────────────

func TestTableListLoadTables(t *testing.T) {
	theme := DarkTheme()
	m := NewTableListModel(nil, theme)

	msg := TablesLoadedMsg{
		Schemas: []string{"public"},
		Tables: map[string][]db.TableInfo{
			"public": {
				{Schema: "public", Name: "users", Type: "table"},
				{Schema: "public", Name: "posts", Type: "table"},
				{Schema: "public", Name: "user_view", Type: "view"},
			},
		},
	}

	m, _ = m.Update(msg)

	if len(m.schemas) != 1 {
		t.Errorf("Expected 1 schema, got %d", len(m.schemas))
	}
	if len(m.tables["public"]) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(m.tables["public"]))
	}
}

func TestTableListNavigation(t *testing.T) {
	theme := DarkTheme()
	m := NewTableListModel(nil, theme)

	// Load tables
	m, _ = m.Update(TablesLoadedMsg{
		Schemas: []string{"public"},
		Tables: map[string][]db.TableInfo{
			"public": {
				{Schema: "public", Name: "a_table", Type: "table"},
				{Schema: "public", Name: "b_table", Type: "table"},
				{Schema: "public", Name: "c_table", Type: "table"},
			},
		},
	})
	m.SetFocused(true)

	// Move down
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.selectedTable != 1 {
		t.Errorf("Expected selectedTable=1 after j, got %d", m.selectedTable)
	}

	// Move up
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	if m.selectedTable != 0 {
		t.Errorf("Expected selectedTable=0 after k, got %d", m.selectedTable)
	}

	// Shouldn't go below 0
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	if m.selectedTable != 0 {
		t.Errorf("Expected selectedTable=0, got %d", m.selectedTable)
	}
}

func TestTableListFilter(t *testing.T) {
	theme := DarkTheme()
	m := NewTableListModel(nil, theme)

	m, _ = m.Update(TablesLoadedMsg{
		Schemas: []string{"public"},
		Tables: map[string][]db.TableInfo{
			"public": {
				{Schema: "public", Name: "users", Type: "table"},
				{Schema: "public", Name: "posts", Type: "table"},
				{Schema: "public", Name: "user_roles", Type: "table"},
			},
		},
	})

	// Enter filter mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	if !m.filtering {
		t.Error("Expected filtering=true after /")
	}

	// Type "user"
	for _, r := range "user" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	filtered := m.filteredTables()
	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered tables for 'user', got %d", len(filtered))
	}

	// Escape clears filter
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if m.filtering {
		t.Error("Expected filtering=false after Esc")
	}
	if m.filterInput != "" {
		t.Error("Expected filterInput to be cleared after Esc")
	}
}

func TestTableListView(t *testing.T) {
	theme := DarkTheme()
	m := NewTableListModel(nil, theme)
	m.SetSize(30, 20)

	m, _ = m.Update(TablesLoadedMsg{
		Schemas: []string{"public"},
		Tables: map[string][]db.TableInfo{
			"public": {
				{Schema: "public", Name: "users", Type: "table"},
				{Schema: "public", Name: "views_data", Type: "view"},
			},
		},
	})

	view := m.View()
	if view == "" {
		t.Error("TableList.View() should not be empty")
	}
}

// ── DataView tests ────────────────────────────────────────────────

func TestDataViewLoadData(t *testing.T) {
	theme := DarkTheme()
	m := NewDataViewModel(nil, theme)

	msg := DataLoadedMsg{
		Schema:      "public",
		Table:       "users",
		Columns:     []string{"id", "name", "email"},
		ColumnTypes: []string{"int4", "varchar", "varchar"},
		Rows: [][]interface{}{
			{1, "Alice", "alice@example.com"},
			{2, "Bob", nil},
		},
	}

	m, _ = m.Update(msg)

	if m.tableName != "users" {
		t.Errorf("Expected tableName=users, got %s", m.tableName)
	}
	if len(m.columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(m.columns))
	}
	if len(m.rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(m.rows))
	}
	if len(m.colWidths) != 3 {
		t.Errorf("Expected 3 colWidths, got %d", len(m.colWidths))
	}
}

func TestDataViewNavigation(t *testing.T) {
	theme := DarkTheme()
	m := NewDataViewModel(nil, theme)
	m.SetSize(80, 24)

	m, _ = m.Update(DataLoadedMsg{
		Schema:      "public",
		Table:       "users",
		Columns:     []string{"id", "name"},
		ColumnTypes: []string{"int4", "varchar"},
		Rows: [][]interface{}{
			{1, "Alice"},
			{2, "Bob"},
			{3, "Charlie"},
		},
	})

	// Move down
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.cursorRow != 1 {
		t.Errorf("Expected cursorRow=1, got %d", m.cursorRow)
	}

	// Move right
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	if m.cursorCol != 1 {
		t.Errorf("Expected cursorCol=1, got %d", m.cursorCol)
	}

	// Jump to end
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("G")})
	if m.cursorRow != 2 {
		t.Errorf("Expected cursorRow=2 after G, got %d", m.cursorRow)
	}

	// Jump to start
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	if m.cursorRow != 0 {
		t.Errorf("Expected cursorRow=0 after g, got %d", m.cursorRow)
	}
}

func TestDataViewColumnWidths(t *testing.T) {
	theme := DarkTheme()
	m := NewDataViewModel(nil, theme)

	m, _ = m.Update(DataLoadedMsg{
		Schema:      "public",
		Table:       "test",
		Columns:     []string{"id", "description"},
		ColumnTypes: []string{"int4", "text"},
		Rows: [][]interface{}{
			{1, "short"},
			{2, "a much longer description value that exceeds limits for testing purposes and should be truncated at some reasonable width"},
		},
	})

	// Min width should be at least 5
	for i, w := range m.colWidths {
		if w < 5 {
			t.Errorf("colWidth[%d]=%d, expected >= 5", i, w)
		}
		if w > 50 {
			t.Errorf("colWidth[%d]=%d, expected <= 50", i, w)
		}
	}
}

func TestDataViewEmpty(t *testing.T) {
	theme := DarkTheme()
	m := NewDataViewModel(nil, theme)
	m.SetSize(80, 24)

	view := m.View()
	if view == "" {
		t.Error("DataView should render even when empty")
	}
}

// ── SQLEditor tests ───────────────────────────────────────────────

func TestSQLEditorInput(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)

	// Type "SELECT"
	for _, r := range "SELECT" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	input := m.GetInput()
	if input != "SELECT" {
		t.Errorf("Expected input='SELECT', got '%s'", input)
	}
	if m.cursorCol != 6 {
		t.Errorf("Expected cursorCol=6, got %d", m.cursorCol)
	}

	// Backspace
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	input = m.GetInput()
	if input != "SELEC" {
		t.Errorf("Expected input='SELEC' after backspace, got '%s'", input)
	}

	// Cursor left
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	if m.cursorCol != 4 {
		t.Errorf("Expected cursorCol=4, got %d", m.cursorCol)
	}
}

func TestSQLEditorView(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetSize(80, 5)
	m.SetFocused(true)

	view := m.View()
	if view == "" {
		t.Error("SQLEditor.View() should not be empty")
	}
}

// ── FormatCellValue tests ─────────────────────────────────────────

func TestFormatCellValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{42, "42"},
		{"hello", "hello"},
		{3.14, "3.14"},
		{true, "true"},
	}

	for _, tt := range tests {
		result := formatCellValue(tt.input)
		if result != tt.expected {
			t.Errorf("formatCellValue(%v) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

// ── Truncate tests ────────────────────────────────────────────────

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxWidth int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello world", 5, "hell…"},
		{"hi", 2, "hi"},
		{"hello", 3, "he…"},
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.maxWidth)
		if result != tt.expected {
			t.Errorf("truncate(%q, %d) = %q, expected %q", tt.input, tt.maxWidth, result, tt.expected)
		}
	}
}

// ── Model integration tests ──────────────────────────────────────

func TestModelInit(t *testing.T) {
	// NewModel should not panic with nil driver (for testing structure)
	// We can't call Init() without a real driver, but we can test construction
	theme := DarkTheme()
	_ = theme
}

func TestModelPaneSwitching(t *testing.T) {
	// Test pane switching logic directly (2-pane cycle: TableList ↔ DataView)
	theme := DarkTheme()
	m := Model{
		activePane: PaneTableList,
		theme:      theme,
	}

	m.switchPane()
	if m.activePane != PaneDataView {
		t.Errorf("Expected PaneDataView after first switch, got %d", m.activePane)
	}

	m.switchPane()
	if m.activePane != PaneTableList {
		t.Errorf("Expected PaneTableList after second switch, got %d", m.activePane)
	}

	// With SQL editor shown: TableList ↔ SQLEditor
	m.showSQLEditor = true
	m.activePane = PaneTableList

	m.switchPane()
	if m.activePane != PaneSQLEditor {
		t.Errorf("Expected PaneSQLEditor after switch with SQL editor shown, got %d", m.activePane)
	}

	m.switchPane()
	if m.activePane != PaneTableList {
		t.Errorf("Expected PaneTableList after second switch with SQL editor shown, got %d", m.activePane)
	}
}

func TestActivePaneConstants(t *testing.T) {
	if PaneTableList != 0 {
		t.Error("PaneTableList should be 0")
	}
	if PaneDataView != 1 {
		t.Error("PaneDataView should be 1")
	}
	if PaneSQLEditor != 2 {
		t.Error("PaneSQLEditor should be 2")
	}
}

// ══════════════════════════════════════════════════════════════════
//  Phase 2 Tests
// ══════════════════════════════════════════════════════════════════

// ── DataView Editing tests ────────────────────────────────────────

func newDataViewWithData() DataViewModel {
	theme := DarkTheme()
	m := NewDataViewModel(nil, theme)
	m.SetSize(80, 24)

	m, _ = m.Update(DataLoadedMsg{
		Schema:      "public",
		Table:       "users",
		Columns:     []string{"id", "name", "email"},
		ColumnTypes: []string{"int4", "varchar", "varchar"},
		Rows: [][]interface{}{
			{1, "Alice", "alice@example.com"},
			{2, "Bob", "bob@example.com"},
			{3, "Charlie", "charlie@example.com"},
		},
	})
	return m
}

func TestDataViewEditMode(t *testing.T) {
	m := newDataViewWithData()

	// Press 'e' to enter edit mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	if !m.IsEditMode() {
		t.Error("Expected edit mode after pressing 'e'")
	}
	if m.editBuffer != "1" {
		t.Errorf("Expected editBuffer='1', got '%s'", m.editBuffer)
	}

	// Type new value
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	for _, r := range "42" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	if m.editBuffer != "42" {
		t.Errorf("Expected editBuffer='42', got '%s'", m.editBuffer)
	}

	// Press Enter to confirm
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if m.IsEditMode() {
		t.Error("Expected to exit edit mode after Enter")
	}
	if m.rows[0][0] != "42" {
		t.Errorf("Expected row[0][0]='42', got '%v'", m.rows[0][0])
	}
}

func TestDataViewEditCancel(t *testing.T) {
	m := newDataViewWithData()

	// Enter edit mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})

	// Type something
	for _, r := range "999" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	// Press Esc to cancel
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if m.IsEditMode() {
		t.Error("Expected to exit edit mode after Esc")
	}
	// Original value should be preserved
	if m.rows[0][0] != 1 {
		t.Errorf("Expected row[0][0]=1 after cancel, got '%v'", m.rows[0][0])
	}
}

func TestDataViewCellModified(t *testing.T) {
	m := newDataViewWithData()

	// Move to name column and edit
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")}) // move right to 'name'
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")}) // edit
	// Clear and type new name
	for range 5 { // "Alice" is 5 chars
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	}
	for _, r := range "Zara" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})

	if !m.isCellModified(0, 1) {
		t.Error("Expected cell (0,1) to be modified")
	}
	if m.isCellModified(0, 0) {
		t.Error("Expected cell (0,0) to NOT be modified")
	}
}

func TestDataViewAddRow(t *testing.T) {
	m := newDataViewWithData()
	initialRowCount := len(m.rows)

	// Press 'a' to add row
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})

	if len(m.rows) != initialRowCount+1 {
		t.Errorf("Expected %d rows after add, got %d", initialRowCount+1, len(m.rows))
	}
	if m.cursorRow != len(m.rows)-1 {
		t.Errorf("Expected cursor at last row %d, got %d", len(m.rows)-1, m.cursorRow)
	}
	// New row should start at newRowStart
	if m.newRowStart != initialRowCount {
		t.Errorf("Expected newRowStart=%d, got %d", initialRowCount, m.newRowStart)
	}
}

func TestDataViewDeleteRow(t *testing.T) {
	m := newDataViewWithData()

	// Press 'd' to toggle delete
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if !m.deletedRows[0] {
		t.Error("Expected row 0 to be marked as deleted")
	}

	// Press 'd' again to undelete
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if m.deletedRows[0] {
		t.Error("Expected row 0 to be unmarked after second 'd'")
	}
}

func TestDataViewDiscardChanges(t *testing.T) {
	m := newDataViewWithData()

	// Make some changes
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	for _, r := range "new" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")}) // add row
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")}) // move down
	m.cursorRow = 1
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")}) // delete row

	if !m.HasPendingChanges() {
		t.Error("Expected HasPendingChanges=true")
	}

	// Ctrl+Z to discard
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlZ})

	if m.HasPendingChanges() {
		t.Error("Expected HasPendingChanges=false after Ctrl+Z")
	}
	if len(m.rows) != 3 {
		t.Errorf("Expected 3 rows after discard, got %d", len(m.rows))
	}
}

func TestDataViewHasPendingChanges(t *testing.T) {
	m := newDataViewWithData()

	if m.HasPendingChanges() {
		t.Error("No pending changes expected initially")
	}

	// Delete a row
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if !m.HasPendingChanges() {
		t.Error("Expected pending changes after delete")
	}
}

// ── DataView Sort tests ───────────────────────────────────────────

func TestDataViewSortToggle(t *testing.T) {
	m := newDataViewWithData()
	m.driver = nil // can't actually reload, but check state

	// Initial: no sort
	col, dir := m.SortInfo()
	if col != "" || dir != "" {
		t.Error("Expected no sort initially")
	}

	// Press 's' -> ASC
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if m.sortColumn != "id" || m.sortDir != "ASC" {
		t.Errorf("Expected sort id ASC, got %s %s", m.sortColumn, m.sortDir)
	}

	// Press 's' again -> DESC
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if m.sortColumn != "id" || m.sortDir != "DESC" {
		t.Errorf("Expected sort id DESC, got %s %s", m.sortColumn, m.sortDir)
	}

	// Press 's' again -> clear
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if m.sortColumn != "" || m.sortDir != "" {
		t.Errorf("Expected sort cleared, got %s %s", m.sortColumn, m.sortDir)
	}
}

// ── DataView Filter tests ─────────────────────────────────────────

func TestDataViewFilterMode(t *testing.T) {
	m := newDataViewWithData()

	// Press '/' to enter filter mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	if !m.IsFilterMode() {
		t.Error("Expected filter mode after /")
	}

	// Type filter text
	for _, r := range "id > 1" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	if m.filterInput != "id > 1" {
		t.Errorf("Expected filterInput='id > 1', got '%s'", m.filterInput)
	}

	// Esc to cancel
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if m.IsFilterMode() {
		t.Error("Expected filter mode to end after Esc")
	}
}

func TestDataViewQuickFilter(t *testing.T) {
	m := newDataViewWithData()

	// Move to 'name' column, row 0 (Alice)
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})

	// Press 'f' for quick filter
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("f")})

	// With parameterized queries, the WHERE clause uses placeholder syntax
	if m.activeFilter != `"name" = $1` {
		t.Errorf("Expected quick filter '\"name\" = $1', got '%s'", m.activeFilter)
	}
	if len(m.activeFilterArgs) != 1 || m.activeFilterArgs[0] != "Alice" {
		t.Errorf("Expected activeFilterArgs [Alice], got %v", m.activeFilterArgs)
	}
}

func TestDataViewDeepCopyRows(t *testing.T) {
	original := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	copied := deepCopyRows(original)

	// Modify copy
	copied[0][1] = "Modified"

	// Original should be unchanged
	if original[0][1] != "Alice" {
		t.Error("deepCopyRows should create independent copy")
	}
}

// ── DataView View rendering tests ─────────────────────────────────

func TestDataViewViewWithChanges(t *testing.T) {
	m := newDataViewWithData()
	m.SetSize(100, 24)

	// Make changes and render
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")}) // delete row 0
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")}) // move down
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")}) // add row

	view := m.View()
	if view == "" {
		t.Error("View should not be empty with changes")
	}
	// Footer should show unsaved changes
	if !strings.Contains(view, "Unsaved") {
		t.Error("View should show unsaved changes indicator")
	}
}

func TestDataViewSortIndicator(t *testing.T) {
	m := newDataViewWithData()
	m.SetSize(100, 24)
	m.sortColumn = "id"
	m.sortDir = "ASC"

	view := m.View()
	if !strings.Contains(view, "▲") {
		t.Error("View should show ASC sort indicator ▲")
	}

	m.sortDir = "DESC"
	view = m.View()
	if !strings.Contains(view, "▼") {
		t.Error("View should show DESC sort indicator ▼")
	}
}

func TestDataViewSQLResultMode(t *testing.T) {
	m := newDataViewWithData()
	m.SetSize(100, 24)
	m.sqlResultMode = true

	view := m.View()
	if !strings.Contains(view, "Query Results") {
		t.Error("View should show 'Query Results' in SQL result mode")
	}

	// Editing should be disabled in SQL result mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	if m.IsEditMode() {
		t.Error("Edit mode should be disabled in SQL result mode")
	}
}

// ── SQL Builder tests ─────────────────────────────────────────────

func TestSQLBuilderQuote(t *testing.T) {
	tests := []struct {
		driver   db.DriverType
		name     string
		expected string
	}{
		{db.Postgres, "users", `"users"`},
		{db.MySQL, "users", "`users`"},
		{db.SQLServer, "users", "[users]"},
		{db.SQLite, "users", `"users"`},
	}

	for _, tt := range tests {
		b := sqlBuilder{driverType: tt.driver}
		result := b.quote(tt.name)
		if result != tt.expected {
			t.Errorf("quote(%s, %s) = %s, expected %s", tt.driver, tt.name, result, tt.expected)
		}
	}
}

func TestSQLBuilderPlaceholder(t *testing.T) {
	tests := []struct {
		driver   db.DriverType
		idx      int
		expected string
	}{
		{db.Postgres, 1, "$1"},
		{db.Postgres, 3, "$3"},
		{db.MySQL, 1, "?"},
		{db.SQLite, 1, "?"},
		{db.SQLServer, 1, "@p1"},
	}

	for _, tt := range tests {
		b := sqlBuilder{driverType: tt.driver}
		result := b.placeholder(tt.idx)
		if result != tt.expected {
			t.Errorf("placeholder(%s, %d) = %s, expected %s", tt.driver, tt.idx, result, tt.expected)
		}
	}
}

func TestSQLBuilderTableRef(t *testing.T) {
	tests := []struct {
		driver   db.DriverType
		schema   string
		table    string
		expected string
	}{
		{db.Postgres, "public", "users", `"public"."users"`},
		{db.MySQL, "mydb", "users", "`mydb`.`users`"},
		{db.SQLite, "main", "users", `"users"`},
		{db.SQLServer, "dbo", "users", "[dbo].[users]"},
	}

	for _, tt := range tests {
		b := sqlBuilder{driverType: tt.driver}
		result := b.tableRef(tt.schema, tt.table)
		if result != tt.expected {
			t.Errorf("tableRef(%s, %s, %s) = %s, expected %s", tt.driver, tt.schema, tt.table, result, tt.expected)
		}
	}
}

func TestFormatValueForPreview(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{"hello", "'hello'"},
		{42, "42"},
		{3.14, "3.14"},
		{true, "TRUE"},
		{false, "FALSE"},
		{"it's", "'it''s'"},
	}

	for _, tt := range tests {
		result := formatValueForPreview(tt.input)
		if result != tt.expected {
			t.Errorf("formatValueForPreview(%v) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

// ── SQL Editor Phase 2 tests ──────────────────────────────────────

func TestSQLEditorMultiLine(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)

	// Type "SELECT *"
	for _, r := range "SELECT *" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	// Press Enter for new line
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if len(m.lines) != 2 {
		t.Errorf("Expected 2 lines, got %d", len(m.lines))
	}
	if m.cursorRow != 1 {
		t.Errorf("Expected cursorRow=1, got %d", m.cursorRow)
	}

	// Type "FROM users"
	for _, r := range "FROM users" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	input := m.GetInput()
	expected := "SELECT *\nFROM users"
	if input != expected {
		t.Errorf("Expected input=%q, got %q", expected, input)
	}
}

func TestSQLEditorSetInput(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)

	m.SetInput("SELECT *\nFROM users\nWHERE id = 1")
	if len(m.lines) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(m.lines))
	}
	if m.cursorRow != 2 {
		t.Errorf("Expected cursorRow=2, got %d", m.cursorRow)
	}
}

func TestSQLEditorVerticalNavigation(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)
	m.SetInput("line1\nline2\nline3")

	// Should be on last line
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
	if m.cursorRow != 1 {
		t.Errorf("Expected cursorRow=1 after up, got %d", m.cursorRow)
	}

	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	if m.cursorRow != 2 {
		t.Errorf("Expected cursorRow=2 after down, got %d", m.cursorRow)
	}
}

func TestSQLEditorDelete(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)

	m.SetInput("AB")
	m.cursorRow = 0
	m.cursorCol = 0

	// Delete forward
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyDelete})
	if m.GetInput() != "B" {
		t.Errorf("Expected 'B' after delete, got '%s'", m.GetInput())
	}
}

func TestSQLEditorCtrlK(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)

	m.SetInput("hello world")
	m.cursorRow = 0
	m.cursorCol = 5

	// Ctrl+K deletes to end of line
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlK})
	if m.GetInput() != "hello" {
		t.Errorf("Expected 'hello' after Ctrl+K, got '%s'", m.GetInput())
	}
}

func TestSQLEditorBackspaceMergeLines(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetFocused(true)

	m.SetInput("line1\nline2")
	m.cursorRow = 1
	m.cursorCol = 0

	// Backspace at start of line merges with previous
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	if len(m.lines) != 1 {
		t.Errorf("Expected 1 line after merge, got %d", len(m.lines))
	}
	if m.GetInput() != "line1line2" {
		t.Errorf("Expected 'line1line2', got '%s'", m.GetInput())
	}
}

func TestSQLEditorViewMultiLine(t *testing.T) {
	theme := DarkTheme()
	m := NewSQLEditorModel(theme)
	m.SetSize(80, 5)
	m.SetFocused(true)

	m.SetInput("SELECT * FROM users WHERE id = 1")

	view := m.View()
	if view == "" {
		t.Error("SQLEditor.View() should not be empty")
	}
}

func TestTokenizeSQL(t *testing.T) {
	tokens := tokenizeSQL("SELECT * FROM users WHERE id = 1")

	// Should have tokens including keywords
	hasKeyword := false
	hasNumber := false
	for _, tok := range tokens {
		if tok.typ == tokenKeyword && tok.text == "SELECT" {
			hasKeyword = true
		}
		if tok.typ == tokenNumber && tok.text == "1" {
			hasNumber = true
		}
	}
	if !hasKeyword {
		t.Error("Expected SELECT keyword token")
	}
	if !hasNumber {
		t.Error("Expected number token '1'")
	}
}

func TestTokenizeSQLString(t *testing.T) {
	tokens := tokenizeSQL("WHERE name = 'Alice'")

	hasString := false
	for _, tok := range tokens {
		if tok.typ == tokenString && tok.text == "'Alice'" {
			hasString = true
		}
	}
	if !hasString {
		t.Error("Expected string literal token 'Alice'")
	}
}

func TestTokenizeSQLComment(t *testing.T) {
	tokens := tokenizeSQL("SELECT * -- this is a comment")

	hasComment := false
	for _, tok := range tokens {
		if tok.typ == tokenComment {
			hasComment = true
		}
	}
	if !hasComment {
		t.Error("Expected comment token")
	}
}

func TestTokenizeSQLEmpty(t *testing.T) {
	tokens := tokenizeSQL("")
	if len(tokens) != 0 {
		t.Errorf("Expected 0 tokens for empty string, got %d", len(tokens))
	}
}

// ── Dialog tests ──────────────────────────────────────────────────

func TestDialogShowHide(t *testing.T) {
	theme := DarkTheme()
	d := NewDialogModel(theme)

	if d.IsVisible() {
		t.Error("Dialog should be hidden by default")
	}

	d.Show("Test", "Content", "test")
	if !d.IsVisible() {
		t.Error("Dialog should be visible after Show")
	}

	d.Hide()
	if d.IsVisible() {
		t.Error("Dialog should be hidden after Hide")
	}
}

func TestDialogKeyNavigation(t *testing.T) {
	theme := DarkTheme()
	d := NewDialogModel(theme)
	d.SetSize(80, 24)
	d.Show("Test", "Are you sure?", "test")

	// Initially selected button should be 0 (Execute)
	if d.selectedBtn != 0 {
		t.Errorf("Expected selectedBtn=0, got %d", d.selectedBtn)
	}

	// Press Tab to switch to Cancel
	d, _ = d.Update(tea.KeyMsg{Type: tea.KeyTab})
	if d.selectedBtn != 1 {
		t.Errorf("Expected selectedBtn=1 after Tab, got %d", d.selectedBtn)
	}

	// Press Tab again to go back to Execute
	d, _ = d.Update(tea.KeyMsg{Type: tea.KeyTab})
	if d.selectedBtn != 0 {
		t.Errorf("Expected selectedBtn=0 after second Tab, got %d", d.selectedBtn)
	}
}

func TestDialogConfirm(t *testing.T) {
	theme := DarkTheme()
	d := NewDialogModel(theme)
	d.SetSize(80, 24)
	d.Show("Test", "Content", "commit")

	// Press 'y' to confirm
	_, cmd := d.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	if cmd == nil {
		t.Error("Expected command from confirm")
	}
	msg := cmd()
	confirm, ok := msg.(DialogConfirmMsg)
	if !ok {
		t.Error("Expected DialogConfirmMsg")
	}
	if confirm.DialogType != "commit" {
		t.Errorf("Expected dialogType='commit', got '%s'", confirm.DialogType)
	}
}

func TestDialogCancel(t *testing.T) {
	theme := DarkTheme()
	d := NewDialogModel(theme)
	d.SetSize(80, 24)
	d.Show("Test", "Content", "test")

	// Press Esc to cancel
	d, cmd := d.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if d.IsVisible() {
		t.Error("Dialog should be hidden after Esc")
	}
	if cmd == nil {
		t.Error("Expected command from cancel")
	}
}

func TestDialogView(t *testing.T) {
	theme := DarkTheme()
	d := NewDialogModel(theme)
	d.SetSize(80, 24)

	// Hidden dialog should return empty
	view := d.View(80, 24)
	if view != "" {
		t.Error("Hidden dialog should return empty view")
	}

	// Visible dialog
	d.Show("Confirm", "DELETE FROM users WHERE id = 1;", "commit")
	view = d.View(80, 24)
	if view == "" {
		t.Error("Visible dialog should return non-empty view")
	}
}

// ── History tests ─────────────────────────────────────────────────

func TestQueryHistory(t *testing.T) {
	h := &QueryHistory{}

	// Add queries
	h.Add("SELECT 1")
	h.Add("SELECT 2")
	h.Add("SELECT 3")

	if h.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", h.Len())
	}

	// Get by index
	if h.Get(0) != "SELECT 1" {
		t.Errorf("Expected 'SELECT 1' at index 0, got '%s'", h.Get(0))
	}
	if h.Get(2) != "SELECT 3" {
		t.Errorf("Expected 'SELECT 3' at index 2, got '%s'", h.Get(2))
	}

	// Out of range
	if h.Get(-1) != "" {
		t.Error("Get(-1) should return empty string")
	}
	if h.Get(100) != "" {
		t.Error("Get(100) should return empty string")
	}
}

func TestQueryHistoryDedup(t *testing.T) {
	h := &QueryHistory{}
	h.Add("SELECT 1")
	h.Add("SELECT 1") // duplicate
	h.Add("SELECT 1") // duplicate

	if h.Len() != 1 {
		t.Errorf("Expected 1 entry after dedup, got %d", h.Len())
	}
}

func TestQueryHistoryMaxEntries(t *testing.T) {
	h := &QueryHistory{}
	for i := range 150 {
		h.Add(formatCellValue(i))
	}

	if h.Len() != maxHistoryEntries {
		t.Errorf("Expected %d entries, got %d", maxHistoryEntries, h.Len())
	}
}

func TestQueryHistoryEmpty(t *testing.T) {
	h := &QueryHistory{}
	h.Add("") // should be ignored

	if h.Len() != 0 {
		t.Errorf("Expected 0 entries for empty add, got %d", h.Len())
	}
}

// ── Commit SQL generation tests ───────────────────────────────────

func TestGenerateCommitStatementsUpdate(t *testing.T) {
	m := newDataViewWithData()
	m.tableDesc = &db.TableDescription{
		Schema:      "public",
		Name:        "users",
		PrimaryKeys: []string{"id"},
		Columns: []db.ColumnInfo{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
			{Name: "email"},
		},
	}

	// Simulate edit: change name in row 0
	m.rows[0][1] = "Modified"

	stmts, preview := m.generateCommitStatements()

	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}
	if preview == "" {
		t.Error("Expected non-empty preview")
	}
	if !strings.Contains(preview, "UPDATE") {
		t.Error("Preview should contain UPDATE")
	}
}

func TestGenerateCommitStatementsDelete(t *testing.T) {
	m := newDataViewWithData()
	m.tableDesc = &db.TableDescription{
		Schema:      "public",
		Name:        "users",
		PrimaryKeys: []string{"id"},
		Columns: []db.ColumnInfo{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
			{Name: "email"},
		},
	}

	// Mark row 1 for deletion
	m.deletedRows[1] = true

	stmts, preview := m.generateCommitStatements()

	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}
	if !strings.Contains(preview, "DELETE") {
		t.Error("Preview should contain DELETE")
	}
}

func TestGenerateCommitStatementsInsert(t *testing.T) {
	m := newDataViewWithData()
	m.tableDesc = &db.TableDescription{
		Schema:      "public",
		Name:        "users",
		PrimaryKeys: []string{"id"},
		Columns: []db.ColumnInfo{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
			{Name: "email"},
		},
	}

	// Add new row with values
	m.rows = append(m.rows, []interface{}{4, "Diana", "diana@example.com"})

	stmts, preview := m.generateCommitStatements()

	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}
	if !strings.Contains(preview, "INSERT") {
		t.Error("Preview should contain INSERT")
	}
}

func TestGenerateCommitStatementsNoPK(t *testing.T) {
	m := newDataViewWithData()
	// No table description = no PK info

	stmts, _ := m.generateCommitStatements()

	if len(stmts) != 0 {
		t.Errorf("Expected 0 statements without table desc, got %d", len(stmts))
	}
}

// ── StatusBar extras test ─────────────────────────────────────────

func TestStatusBarExtras(t *testing.T) {
	theme := DarkTheme()
	sb := NewStatusBarModel(theme)
	sb.SetWidth(100)
	sb.SetExtras("Sort: id ASC │ Filter: id > 1")

	view := sb.View()
	if !strings.Contains(view, "Sort") {
		t.Error("StatusBar should show extras")
	}
}

// ── GetRowsOptions builder test ───────────────────────────────────

func TestBuildGetRowsOptions(t *testing.T) {
	m := newDataViewWithData()
	m.sortColumn = "name"
	m.sortDir = "DESC"
	m.activeFilter = "id > 5"
	m.page = 2

	opts := m.buildGetRowsOptions()

	if opts.OrderBy != "name" {
		t.Errorf("Expected OrderBy='name', got '%s'", opts.OrderBy)
	}
	if opts.OrderDir != "DESC" {
		t.Errorf("Expected OrderDir='DESC', got '%s'", opts.OrderDir)
	}
	if opts.Where != "id > 5" {
		t.Errorf("Expected Where='id > 5', got '%s'", opts.Where)
	}
	if opts.Offset != 200 {
		t.Errorf("Expected Offset=200, got %d", opts.Offset)
	}
}
