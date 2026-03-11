package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	"github.com/mattn/go-runewidth"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/db"
)

// ── Messages ──────────────────────────────────────────────────────

// DataLoadedMsg is sent when data loading completes.
type DataLoadedMsg struct {
	Schema      string
	Table       string
	Columns     []string
	ColumnTypes []string
	Rows        [][]interface{}
	Err         error
}

// TableDescribedMsg is sent when table description is loaded.
type TableDescribedMsg struct {
	Schema string
	Table  string
	Desc   *db.TableDescription
	Err    error
}

// CommitResultMsg holds the result of a commit operation.
type CommitResultMsg struct {
	Message string
	Err     error
}

// RequestCommitMsg is sent when the user wants to preview and commit changes.
type RequestCommitMsg struct {
	SQLPreview string
}

// CopyResultMsg is sent when data is copied to clipboard.
type CopyResultMsg struct {
	Count int
	Kind  string // "cell", "row", "rows"
}

// ── Commands ──────────────────────────────────────────────────────

// LoadDataCmd loads data from the specified table.
func LoadDataCmd(driver db.Driver, schema, table string, page, pageSize int) tea.Cmd {
	return LoadDataWithOptionsCmd(driver, schema, table, db.GetRowsOptions{
		Limit:  pageSize,
		Offset: page * pageSize,
	})
}

// LoadDataWithOptionsCmd loads data with full options (sort, filter, pagination).
func LoadDataWithOptionsCmd(driver db.Driver, schema, table string, opts db.GetRowsOptions) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		result, err := driver.GetRows(ctx, schema, table, opts)
		if err != nil {
			return DataLoadedMsg{Schema: schema, Table: table, Err: err}
		}
		return DataLoadedMsg{
			Schema:      schema,
			Table:       table,
			Columns:     result.Columns,
			ColumnTypes: result.ColumnTypes,
			Rows:        result.Rows,
		}
	}
}

// DescribeTableCmd loads table description (for primary key info).
func DescribeTableCmd(driver db.Driver, schema, table string) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		desc, err := driver.DescribeTable(ctx, schema, table)
		if err != nil {
			return TableDescribedMsg{Schema: schema, Table: table, Err: err}
		}
		return TableDescribedMsg{Schema: schema, Table: table, Desc: desc}
	}
}

// CommitChangesCmd commits pending changes to the database within a transaction.
func CommitChangesCmd(driver db.Driver, schema, table string, statements []commitStatement) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()

		tx, err := driver.Begin(ctx)
		if err != nil {
			return CommitResultMsg{Err: fmt.Errorf("failed to begin transaction: %w", err)}
		}

		totalAffected := int64(0)
		for _, stmt := range statements {
			affected, err := tx.Exec(ctx, stmt.query, stmt.args...)
			if err != nil {
				_ = tx.Rollback()
				return CommitResultMsg{Err: fmt.Errorf("statement failed: %w\nSQL: %s", err, stmt.query)}
			}
			totalAffected += affected
		}

		if err := tx.Commit(); err != nil {
			return CommitResultMsg{Err: fmt.Errorf("commit failed: %w", err)}
		}

		return CommitResultMsg{
			Message: fmt.Sprintf("Committed successfully: %d rows affected", totalAffected),
		}
	}
}

// ── SQL Builder Helper ────────────────────────────────────────────

type commitStatement struct {
	query string
	args  []interface{}
}

type sqlBuilder struct {
	driverType db.DriverType
}

func (b sqlBuilder) quote(name string) string {
	switch b.driverType {
	case db.MySQL:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case db.SQLServer:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default: // Postgres, SQLite
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

func (b sqlBuilder) placeholder(idx int) string {
	switch b.driverType {
	case db.Postgres:
		return fmt.Sprintf("$%d", idx)
	case db.SQLServer:
		return fmt.Sprintf("@p%d", idx)
	default: // MySQL, SQLite
		return "?"
	}
}

func (b sqlBuilder) tableRef(schema, table string) string {
	switch b.driverType {
	case db.SQLite:
		return b.quote(table)
	default:
		if schema == "" {
			return b.quote(table)
		}
		return b.quote(schema) + "." + b.quote(table)
	}
}

// ── DataViewModel ─────────────────────────────────────────────────

// DataViewModel manages the data view pane.
// dataSnapshot captures the mutable data state for undo/redo.
type dataSnapshot struct {
	rows        [][]interface{}
	deletedRows map[int]bool
	newRowStart int
	cursorRow   int
	cursorCol   int
	colWidths   []int
}

type DataViewModel struct {
	tableName   string
	schemaName  string
	columns     []string
	columnTypes []string
	rows        [][]interface{}

	cursorRow int
	cursorCol int

	scrollOffsetX int
	scrollOffsetY int

	colWidths []int

	page       int
	totalPages int
	pageSize   int

	loading bool
	focused bool
	width   int
	height  int
	theme   *Theme
	driver  db.Driver

	// ── Phase 2: Editing state ──
	editMode      bool   // cell editing mode
	editBuffer    string // current cell edit text
	editCursorPos int    // cursor position in edit buffer

	// Original data (deep copy, preserved for diff/revert)
	originalRows [][]interface{}
	newRowStart  int          // index where new rows begin
	deletedRows  map[int]bool // indices of deleted rows

	// Table description (for primary key info)
	tableDesc *db.TableDescription

	// ── Phase 2: Sort state ──
	sortColumn string // column name being sorted
	sortDir    string // "", "ASC", "DESC"

	// ── Phase 2: Filter state ──
	filterMode      bool          // filter input mode active
	filterInput     string        // current filter text
	activeFilter    string        // applied filter (WHERE condition)
	activeFilterArgs []interface{} // parameterized args for activeFilter (nil for user-typed WHERE)

	// SQL results mode (when showing SQL query results)
	sqlResultMode bool

	// Visual line selection
	visualMode     bool
	visualStartRow int

	// Copy format menu
	copyFormatMenu   bool // whether the format selection menu is visible
	copyFormatIdx    int  // selected menu item index (0-3)
	copyFormatStartR int  // start row of selection
	copyFormatEndR   int  // end row of selection

	// Yank (copy) state — for yy double-key detection
	yankPending bool

	// Paste buffer — stores copied data for internal paste
	yankedCells [][]interface{} // copied row data (each inner slice = one row's cells)
	yankType    string          // "cell", "row", "rows"

	// ── Undo / Redo stacks ──
	undoStack []dataSnapshot
	redoStack []dataSnapshot
}

// NewDataViewModel creates a new DataViewModel.
func NewDataViewModel(driver db.Driver, theme *Theme) DataViewModel {
	return DataViewModel{
		pageSize:    100,
		theme:       theme,
		driver:      driver,
		deletedRows: make(map[int]bool),
	}
}

// SetFocused sets whether this pane is focused.
func (m *DataViewModel) SetFocused(f bool) {
	m.focused = f
}

// SetSize updates the pane size.
func (m *DataViewModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// Reset clears all data view state, used when switching databases.
func (m *DataViewModel) Reset() {
	m.sqlResultMode = false
	m.schemaName = ""
	m.tableName = ""
	m.columns = nil
	m.columnTypes = nil
	m.rows = nil
	m.originalRows = nil
	m.tableDesc = nil
	m.cursorRow = 0
	m.cursorCol = 0
	m.scrollOffsetX = 0
	m.scrollOffsetY = 0
	m.colWidths = nil
	m.page = 0
	m.editMode = false
	m.editBuffer = ""
	m.editCursorPos = 0
	m.newRowStart = 0
	m.deletedRows = make(map[int]bool)
	m.sortColumn = ""
	m.sortDir = ""
	m.filterMode = false
	m.filterInput = ""
	m.activeFilter = ""
	m.activeFilterArgs = nil
	m.loading = false
	m.undoStack = nil
	m.redoStack = nil
	m.yankedCells = nil
	m.yankType = ""
	m.copyFormatMenu = false
}

// RowCount returns the number of rows.
func (m DataViewModel) RowCount() int {
	return len(m.rows)
}

// IsFilterMode returns whether the data view is in filter input mode.
func (m DataViewModel) IsFilterMode() bool {
	return m.filterMode
}

// IsEditMode returns whether the data view is in cell edit mode.
func (m DataViewModel) IsEditMode() bool {
	return m.editMode
}

// HasPendingChanges returns whether there are uncommitted changes.
func (m DataViewModel) HasPendingChanges() bool {
	if len(m.deletedRows) > 0 {
		return true
	}
	if m.newRowStart < len(m.rows) {
		return true
	}
	// Check for cell modifications
	for r := 0; r < m.newRowStart && r < len(m.rows); r++ {
		if m.deletedRows[r] {
			continue
		}
		for c := 0; c < len(m.columns) && c < len(m.rows[r]) && c < len(m.originalRows[r]); c++ {
			if fmt.Sprintf("%v", m.rows[r][c]) != fmt.Sprintf("%v", m.originalRows[r][c]) {
				return true
			}
		}
	}
	return false
}

// SortInfo returns current sort column and direction.
func (m DataViewModel) SortInfo() (string, string) {
	return m.sortColumn, m.sortDir
}

// FilterInfo returns current active filter.
func (m DataViewModel) FilterInfo() string {
	return m.activeFilter
}

// deepCopyRows makes a deep copy of the rows slice.
func deepCopyRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	cp := make([][]interface{}, len(rows))
	for i, row := range rows {
		cp[i] = make([]interface{}, len(row))
		copy(cp[i], row)
	}
	return cp
}

// saveSnapshot captures the current mutable data state for undo/redo.
func (m DataViewModel) saveSnapshot() dataSnapshot {
	delCopy := make(map[int]bool, len(m.deletedRows))
	for k, v := range m.deletedRows {
		delCopy[k] = v
	}
	cwCopy := make([]int, len(m.colWidths))
	copy(cwCopy, m.colWidths)
	return dataSnapshot{
		rows:        deepCopyRows(m.rows),
		deletedRows: delCopy,
		newRowStart: m.newRowStart,
		cursorRow:   m.cursorRow,
		cursorCol:   m.cursorCol,
		colWidths:   cwCopy,
	}
}

// calculateColWidths computes column widths based on column names and data.
func (m *DataViewModel) calculateColWidths() {
	if len(m.columns) == 0 {
		m.colWidths = nil
		return
	}

	m.colWidths = make([]int, len(m.columns))
	for i, col := range m.columns {
		// Start with column name length + sort indicator space
		m.colWidths[i] = len(col) + 2
	}

	// Check data widths (sample up to 100 rows)
	sampleSize := len(m.rows)
	if sampleSize > 100 {
		sampleSize = 100
	}
	for _, row := range m.rows[:sampleSize] {
		for i, val := range row {
			if i >= len(m.colWidths) {
				break
			}
			w := len(formatCellValue(val))
			if w > m.colWidths[i] {
				m.colWidths[i] = w
			}
		}
	}

	// Apply min/max constraints
	for i := range m.colWidths {
		if m.colWidths[i] < 5 {
			m.colWidths[i] = 5
		}
		if m.colWidths[i] > 50 {
			m.colWidths[i] = 50
		}
	}
}

// formatCellValue formats a cell value for display.
func formatCellValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", val)
}

// visibleColumns returns the range of column indices visible in the viewport.
func (m DataViewModel) visibleColumns() (start, end int) {
	// Account for row number column and borders
	rowNumWidth := 5 // "  #  "
	available := m.width - rowNumWidth - 4 // borders + padding

	start = m.scrollOffsetX
	usedWidth := 0
	end = start
	for i := start; i < len(m.columns); i++ {
		colW := m.colWidths[i] + 3 // padding + separator
		if i > start {
			colW++ // column separator │
		}
		if usedWidth+colW > available && i > start {
			break
		}
		usedWidth += colW
		end = i + 1
	}
	return start, end
}

// visibleRows returns the range of row indices visible in the viewport.
func (m DataViewModel) visibleRows() (start, end int) {
	// Subtract header(2) + footer(1) + border(2) + table title(1) + filter/sort info(1)
	bodyHeight := m.height - 7
	if bodyHeight < 1 {
		bodyHeight = 1
	}

	start = m.scrollOffsetY
	end = start + bodyHeight
	if end > len(m.rows) {
		end = len(m.rows)
	}
	return start, end
}

// bodyHeight returns the number of data rows visible.
func (m DataViewModel) bodyHeight() int {
	h := m.height - 7
	if h < 1 {
		h = 1
	}
	return h
}

// buildGetRowsOptions creates GetRowsOptions from current state.
func (m DataViewModel) buildGetRowsOptions() db.GetRowsOptions {
	opts := db.GetRowsOptions{
		Limit:  m.pageSize,
		Offset: m.page * m.pageSize,
	}
	if m.sortColumn != "" && m.sortDir != "" {
		opts.OrderBy = m.sortColumn
		opts.OrderDir = m.sortDir
	}
	if m.activeFilter != "" {
		opts.Where = m.activeFilter
		opts.WhereArgs = m.activeFilterArgs // nil for user-typed WHERE, set for quick filter
	}
	return opts
}

// reloadDataCmd returns a command to reload data with current options.
func (m DataViewModel) reloadDataCmd() tea.Cmd {
	return LoadDataWithOptionsCmd(m.driver, m.schemaName, m.tableName, m.buildGetRowsOptions())
}

// isCellModified returns true if the cell has been changed from original.
func (m DataViewModel) isCellModified(row, col int) bool {
	if row >= m.newRowStart || row >= len(m.originalRows) {
		return false // new rows are not "modified"
	}
	if col >= len(m.rows[row]) || col >= len(m.originalRows[row]) {
		return false
	}
	return fmt.Sprintf("%v", m.rows[row][col]) != fmt.Sprintf("%v", m.originalRows[row][col])
}

// getPKColumnIndices returns the indices of primary key columns.
func (m DataViewModel) getPKColumnIndices() []int {
	if m.tableDesc == nil {
		return nil
	}
	var indices []int
	for _, pkName := range m.tableDesc.PrimaryKeys {
		for i, col := range m.columns {
			if col == pkName {
				indices = append(indices, i)
				break
			}
		}
	}
	return indices
}

// generateCommitStatements creates SQL statements for all pending changes.
func (m DataViewModel) generateCommitStatements() ([]commitStatement, string) {
	if m.tableDesc == nil {
		return nil, ""
	}

	var driverType db.DriverType
	if m.driver != nil {
		driverType = m.driver.DriverName()
	} else {
		driverType = db.Postgres // fallback for tests
	}

	builder := sqlBuilder{driverType: driverType}
	tableRef := builder.tableRef(m.schemaName, m.tableName)
	pkIndices := m.getPKColumnIndices()

	var stmts []commitStatement
	var preview strings.Builder

	// 1. DELETE statements
	for rowIdx := range m.deletedRows {
		if rowIdx >= m.newRowStart {
			continue // skip deleting new (unsaved) rows
		}
		if rowIdx >= len(m.originalRows) {
			continue
		}

		whereClause, args := m.buildPKWhere(builder, pkIndices, m.originalRows[rowIdx], 1)
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableRef, whereClause)
		stmts = append(stmts, commitStatement{query: query, args: args})

		// Preview
		preview.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s;\n",
			tableRef, m.buildPKWherePreview(builder, pkIndices, m.originalRows[rowIdx])))
	}

	// 2. UPDATE statements
	for r := 0; r < m.newRowStart && r < len(m.rows); r++ {
		if m.deletedRows[r] {
			continue
		}
		if r >= len(m.originalRows) {
			continue
		}

		// Find changed columns
		var setClauses []string
		var setArgs []interface{}
		argIdx := 1
		for c := 0; c < len(m.columns) && c < len(m.rows[r]) && c < len(m.originalRows[r]); c++ {
			if fmt.Sprintf("%v", m.rows[r][c]) != fmt.Sprintf("%v", m.originalRows[r][c]) {
				setClauses = append(setClauses, fmt.Sprintf("%s = %s", builder.quote(m.columns[c]), builder.placeholder(argIdx)))
				setArgs = append(setArgs, m.rows[r][c])
				argIdx++
			}
		}

		if len(setClauses) == 0 {
			continue
		}

		whereClause, whereArgs := m.buildPKWhere(builder, pkIndices, m.originalRows[r], argIdx)
		allArgs := append(setArgs, whereArgs...)
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableRef, strings.Join(setClauses, ", "), whereClause)
		stmts = append(stmts, commitStatement{query: query, args: allArgs})

		// Preview
		var previewSets []string
		for c := 0; c < len(m.columns) && c < len(m.rows[r]) && c < len(m.originalRows[r]); c++ {
			if fmt.Sprintf("%v", m.rows[r][c]) != fmt.Sprintf("%v", m.originalRows[r][c]) {
				previewSets = append(previewSets, fmt.Sprintf("%s = %s",
					builder.quote(m.columns[c]), formatValueForPreview(m.rows[r][c])))
			}
		}
		preview.WriteString(fmt.Sprintf("UPDATE %s SET %s WHERE %s;\n",
			tableRef, strings.Join(previewSets, ", "),
			m.buildPKWherePreview(builder, pkIndices, m.originalRows[r])))
	}

	// 3. INSERT statements
	for r := m.newRowStart; r < len(m.rows); r++ {
		if m.deletedRows[r] {
			continue
		}

		var cols []string
		var placeholders []string
		var args []interface{}
		argIdx := 1
		for c, col := range m.columns {
			var val interface{}
			if c < len(m.rows[r]) {
				val = m.rows[r][c]
			}
			if val == nil {
				continue // skip NULL values for insert
			}
			cols = append(cols, builder.quote(col))
			placeholders = append(placeholders, builder.placeholder(argIdx))
			args = append(args, val)
			argIdx++
		}

		if len(cols) == 0 {
			continue
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableRef, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		stmts = append(stmts, commitStatement{query: query, args: args})

		// Preview
		var previewVals []string
		for c, col := range m.columns {
			var val interface{}
			if c < len(m.rows[r]) {
				val = m.rows[r][c]
			}
			if val == nil {
				continue
			}
			_ = col
			previewVals = append(previewVals, formatValueForPreview(val))
		}
		preview.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n",
			tableRef, strings.Join(cols, ", "), strings.Join(previewVals, ", ")))
	}

	return stmts, preview.String()
}

// buildPKWhere builds a parameterized WHERE clause from primary key values.
func (m DataViewModel) buildPKWhere(builder sqlBuilder, pkIndices []int, row []interface{}, startIdx int) (string, []interface{}) {
	var parts []string
	var args []interface{}
	idx := startIdx
	for _, pkIdx := range pkIndices {
		if pkIdx < len(row) {
			parts = append(parts, fmt.Sprintf("%s = %s", builder.quote(m.columns[pkIdx]), builder.placeholder(idx)))
			args = append(args, row[pkIdx])
			idx++
		}
	}
	if len(parts) == 0 {
		return "1=0", nil // safety: no PK
	}
	return strings.Join(parts, " AND "), args
}

// buildPKWherePreview builds a human-readable WHERE clause for preview.
func (m DataViewModel) buildPKWherePreview(builder sqlBuilder, pkIndices []int, row []interface{}) string {
	var parts []string
	for _, pkIdx := range pkIndices {
		if pkIdx < len(row) {
			parts = append(parts, fmt.Sprintf("%s = %s",
				builder.quote(m.columns[pkIdx]), formatValueForPreview(row[pkIdx])))
		}
	}
	if len(parts) == 0 {
		return "1=0"
	}
	return strings.Join(parts, " AND ")
}

// formatValueForPreview formats a value for display in SQL preview.
func formatValueForPreview(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''") + "'"
	}
}

// clearPendingChanges resets all pending change state.
func (m *DataViewModel) clearPendingChanges() {
	m.deletedRows = make(map[int]bool)
	m.originalRows = deepCopyRows(m.rows)
	m.newRowStart = len(m.rows)
	m.editMode = false
	m.editBuffer = ""
	m.undoStack = nil
	m.redoStack = nil
	m.yankedCells = nil
	m.yankType = ""
}

// discardChanges reverts all changes and restores original data.
func (m *DataViewModel) discardChanges() {
	m.rows = deepCopyRows(m.originalRows)
	m.deletedRows = make(map[int]bool)
	m.newRowStart = len(m.rows)
	m.editMode = false
	m.editBuffer = ""
	m.calculateColWidths()
}

// ── Update ────────────────────────────────────────────────────────

// Update handles key input for the data view.
func (m DataViewModel) Update(msg tea.Msg) (DataViewModel, tea.Cmd) {
	switch msg := msg.(type) {
	case DataLoadedMsg:
		if msg.Err != nil {
			return m, nil
		}
		m.schemaName = msg.Schema
		m.tableName = msg.Table
		m.columns = msg.Columns
		m.columnTypes = msg.ColumnTypes
		m.rows = msg.Rows
		m.originalRows = deepCopyRows(msg.Rows)
		m.newRowStart = len(msg.Rows)
		m.deletedRows = make(map[int]bool)
		m.cursorRow = 0
		m.cursorCol = 0
		m.scrollOffsetX = 0
		m.scrollOffsetY = 0
		m.loading = false
		m.editMode = false
		m.sqlResultMode = false
		m.undoStack = nil
		m.redoStack = nil
		m.yankedCells = nil
		m.yankType = ""
		m.copyFormatMenu = false
		m.calculateColWidths()

		// Estimate total pages
		if len(m.rows) < m.pageSize {
			m.totalPages = m.page + 1
		} else {
			m.totalPages = m.page + 2
		}
		return m, nil

	case TableDescribedMsg:
		if msg.Err != nil {
			return m, nil
		}
		m.tableDesc = msg.Desc
		return m, nil

	case tea.KeyMsg:
		if m.editMode {
			return m.handleEditInput(msg)
		}
		if m.filterMode {
			return m.handleFilterInput(msg)
		}
		return m.handleKeyInput(msg)
	}
	return m, nil
}

// handleEditInput handles key input in cell edit mode.
func (m DataViewModel) handleEditInput(msg tea.KeyMsg) (DataViewModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.editMode = false
		m.editBuffer = ""
		return m, nil

	case "enter":
		// Apply edit
		if m.cursorRow < len(m.rows) && m.cursorCol < len(m.rows[m.cursorRow]) {
			// push undo before mutation
			snap := m.saveSnapshot()
			m.undoStack = append(m.undoStack, snap)
			if len(m.undoStack) > 100 {
				m.undoStack = m.undoStack[1:]
			}
			m.redoStack = nil

			if m.editBuffer == "NULL" || m.editBuffer == "null" {
				m.rows[m.cursorRow][m.cursorCol] = nil
			} else {
				m.rows[m.cursorRow][m.cursorCol] = m.editBuffer
			}
		}
		m.editMode = false
		m.editBuffer = ""
		return m, nil

	case "backspace":
		if m.editCursorPos > 0 {
			runes := []rune(m.editBuffer)
			m.editBuffer = string(runes[:m.editCursorPos-1]) + string(runes[m.editCursorPos:])
			m.editCursorPos--
		}
		return m, nil

	case "left":
		if m.editCursorPos > 0 {
			m.editCursorPos--
		}
		return m, nil

	case "right":
		if m.editCursorPos < len([]rune(m.editBuffer)) {
			m.editCursorPos++
		}
		return m, nil

	case "home":
		m.editCursorPos = 0
		return m, nil

	case "end":
		m.editCursorPos = len([]rune(m.editBuffer))
		return m, nil

	default:
		if msg.Type == tea.KeyRunes {
			runes := []rune(m.editBuffer)
			insertRunes := msg.Runes
			m.editBuffer = string(runes[:m.editCursorPos]) + string(insertRunes) + string(runes[m.editCursorPos:])
			m.editCursorPos += len(insertRunes)
		}
		return m, nil
	}
}

// handleFilterInput handles key input in filter mode.
func (m DataViewModel) handleFilterInput(msg tea.KeyMsg) (DataViewModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.filterMode = false
		return m, nil

	case "enter":
		// Apply filter
		// NOTE: The '/' filter accepts raw SQL WHERE clauses typed by the user,
		// similar to the SQL editor. WhereArgs is intentionally left nil here
		// because the user is writing arbitrary SQL expressions.
		m.filterMode = false
		m.activeFilter = m.filterInput
		m.activeFilterArgs = nil // user-typed WHERE — no parameterized args
		m.page = 0
		m.loading = true
		return m, m.reloadDataCmd()

	case "backspace":
		if len(m.filterInput) > 0 {
			m.filterInput = m.filterInput[:len(m.filterInput)-1]
		}
		return m, nil

	default:
		if msg.Type == tea.KeyRunes {
			m.filterInput += string(msg.Runes)
		}
		return m, nil
	}
}

func (m DataViewModel) handleKeyInput(msg tea.KeyMsg) (DataViewModel, tea.Cmd) {
	maxRow := len(m.rows) - 1
	if maxRow < 0 {
		// Even with no rows, some commands should work
		switch msg.String() {
		case "/":
			m.filterMode = true
			if m.activeFilter != "" {
				m.filterInput = m.activeFilter
			} else if m.cursorCol < len(m.columns) {
				m.filterInput = m.columns[m.cursorCol] + "="
			}
			return m, nil
		}
		return m, nil
	}

	// Handle copy format menu
	if m.copyFormatMenu {
		switch msg.String() {
		case "j", "down":
			if m.copyFormatIdx < 3 {
				m.copyFormatIdx++
			}
			return m, nil
		case "k", "up":
			if m.copyFormatIdx > 0 {
				m.copyFormatIdx--
			}
			return m, nil
		case "enter":
			text := m.formatCopySelection(m.copyFormatStartR, m.copyFormatEndR, m.copyFormatIdx)
			copyToClipboard(text)
			m.copyFormatMenu = false
			kind := []string{"SQL INSERT", "CSV (header)", "CSV", "JSON"}[m.copyFormatIdx]
			count := m.copyFormatEndR - m.copyFormatStartR + 1
			return m, func() tea.Msg {
				return CopyResultMsg{Count: count, Kind: kind}
			}
		case "esc", "q":
			m.copyFormatMenu = false
			return m, nil
		}
		return m, nil
	}

	maxCol := len(m.columns) - 1
	if maxCol < 0 {
		maxCol = 0
	}

	bh := m.bodyHeight()

	// Handle visual mode navigation
	if m.visualMode {
		switch msg.String() {
		case "j", "down":
			if m.cursorRow < maxRow {
				m.cursorRow++
				if m.cursorRow >= m.scrollOffsetY+bh {
					m.scrollOffsetY = m.cursorRow - bh + 1
				}
			}
			return m, nil
		case "k", "up":
			if m.cursorRow > 0 {
				m.cursorRow--
				if m.cursorRow < m.scrollOffsetY {
					m.scrollOffsetY = m.cursorRow
				}
			}
			return m, nil
		case "g":
			m.cursorRow = 0
			m.scrollOffsetY = 0
			return m, nil
		case "G":
			m.cursorRow = maxRow
			if maxRow >= bh {
				m.scrollOffsetY = maxRow - bh + 1
			}
			return m, nil
		case "y":
			// Copy selected rows
			startR, endR := m.visualStartRow, m.cursorRow
			if startR > endR {
				startR, endR = endR, startR
			}
			var lines []string
			for r := startR; r <= endR && r < len(m.rows); r++ {
				lines = append(lines, m.formatRowAsText(r))
			}
			copyToClipboard(strings.Join(lines, "\n"))
			m.yankedCells = nil
			for r := startR; r <= endR && r < len(m.rows); r++ {
				rowCopy := make([]interface{}, len(m.rows[r]))
				copy(rowCopy, m.rows[r])
				m.yankedCells = append(m.yankedCells, rowCopy)
			}
			m.yankType = "rows"
			m.visualMode = false
			return m, func() tea.Msg {
				return CopyResultMsg{Count: endR - startR + 1, Kind: "rows"}
			}
		case "d":
			if m.sqlResultMode {
				return m, nil
			}
			startR, endR := m.visualStartRow, m.cursorRow
			if startR > endR {
				startR, endR = endR, startR
			}
			// Push undo snapshot
			snap := m.saveSnapshot()
			m.undoStack = append(m.undoStack, snap)
			if len(m.undoStack) > 100 {
				m.undoStack = m.undoStack[1:]
			}
			m.redoStack = nil
			// Mark all selected rows as deleted (toggle)
			// Check if all selected rows are already deleted
			allDeleted := true
			for r := startR; r <= endR && r < len(m.rows); r++ {
				if !m.deletedRows[r] {
					allDeleted = false
					break
				}
			}
			// If all are deleted, undelete them all (toggle). Otherwise, delete all.
			for r := startR; r <= endR && r < len(m.rows); r++ {
				if allDeleted {
					delete(m.deletedRows, r)
				} else {
					m.deletedRows[r] = true
				}
			}
			m.visualMode = false
			return m, nil
		case "esc", "V":
			m.visualMode = false
			return m, nil
		case "Y":
			startR, endR := m.visualStartRow, m.cursorRow
			if startR > endR {
				startR, endR = endR, startR
			}
			m.copyFormatMenu = true
			m.copyFormatIdx = 0
			m.copyFormatStartR = startR
			m.copyFormatEndR = endR
			m.visualMode = false
			return m, nil
		}
		return m, nil
	}

	// Handle yy (yank pending + y = copy row)
	if m.yankPending {
		m.yankPending = false
		if msg.String() == "y" {
			// yy: copy current row
			if m.cursorRow < len(m.rows) {
				text := m.formatRowAsText(m.cursorRow)
				copyToClipboard(text)
				rowCopy := make([]interface{}, len(m.rows[m.cursorRow]))
				copy(rowCopy, m.rows[m.cursorRow])
				m.yankedCells = [][]interface{}{rowCopy}
				m.yankType = "row"
				return m, func() tea.Msg {
					return CopyResultMsg{Count: 1, Kind: "row"}
				}
			}
		}
		// Any other key after y: cancel yank, fall through
	}

	switch msg.String() {
	// ── Navigation ──
	case "j", "down":
		if m.cursorRow < maxRow {
			m.cursorRow++
			if m.cursorRow >= m.scrollOffsetY+bh {
				m.scrollOffsetY = m.cursorRow - bh + 1
			}
		}
	case "k", "up":
		if m.cursorRow > 0 {
			m.cursorRow--
			if m.cursorRow < m.scrollOffsetY {
				m.scrollOffsetY = m.cursorRow
			}
		}
	case "h", "left":
		if m.cursorCol > 0 {
			m.cursorCol--
			if m.cursorCol < m.scrollOffsetX {
				m.scrollOffsetX = m.cursorCol
			}
		}
	case "l", "right":
		if m.cursorCol < maxCol {
			m.cursorCol++
			_, visEnd := m.visibleColumns()
			if m.cursorCol >= visEnd {
				m.scrollOffsetX++
			}
		}
	case "g":
		m.cursorRow = 0
		m.scrollOffsetY = 0
	case "G":
		m.cursorRow = maxRow
		if maxRow >= bh {
			m.scrollOffsetY = maxRow - bh + 1
		}
	case "ctrl+u", "pgup":
		m.cursorRow -= bh
		if m.cursorRow < 0 {
			m.cursorRow = 0
		}
		m.scrollOffsetY -= bh
		if m.scrollOffsetY < 0 {
			m.scrollOffsetY = 0
		}
	case "ctrl+d", "pgdown":
		m.cursorRow += bh
		if m.cursorRow > maxRow {
			m.cursorRow = maxRow
		}
		m.scrollOffsetY += bh
		if m.scrollOffsetY > maxRow-bh+1 {
			m.scrollOffsetY = maxRow - bh + 1
		}
		if m.scrollOffsetY < 0 {
			m.scrollOffsetY = 0
		}

	// ── Page navigation ──
	case "n":
		if len(m.rows) >= m.pageSize {
			m.page++
			m.loading = true
			return m, m.reloadDataCmd()
		}
	case "P":
		if m.page > 0 {
			m.page--
			m.loading = true
			return m, m.reloadDataCmd()
		}

	// ── Cell editing (Enter or e) ──
	case "enter", "e":
		if m.sqlResultMode {
			return m, nil // no editing in SQL result mode
		}
		if m.cursorRow < len(m.rows) && m.cursorCol < len(m.rows[m.cursorRow]) {
			m.editMode = true
			m.editBuffer = formatCellValue(m.rows[m.cursorRow][m.cursorCol])
			m.editCursorPos = len([]rune(m.editBuffer))
		}
		return m, nil

	// ── Row add ──
	case "a":
		if m.sqlResultMode {
			return m, nil
		}
		// push undo before mutation
		snap := m.saveSnapshot()
		m.undoStack = append(m.undoStack, snap)
		if len(m.undoStack) > 100 {
			m.undoStack = m.undoStack[1:]
		}
		m.redoStack = nil

		// Add new empty row
		newRow := make([]interface{}, len(m.columns))
		m.rows = append(m.rows, newRow)
		m.cursorRow = len(m.rows) - 1
		// Scroll to new row
		if m.cursorRow >= m.scrollOffsetY+bh {
			m.scrollOffsetY = m.cursorRow - bh + 1
		}
		return m, nil

	// ── Row delete/undelete ──
	case "d":
		if m.sqlResultMode {
			return m, nil
		}
		// push undo before mutation
		snap := m.saveSnapshot()
		m.undoStack = append(m.undoStack, snap)
		if len(m.undoStack) > 100 {
			m.undoStack = m.undoStack[1:]
		}
		m.redoStack = nil

		if m.deletedRows[m.cursorRow] {
			delete(m.deletedRows, m.cursorRow)
		} else {
			m.deletedRows[m.cursorRow] = true
		}
		return m, nil

	// ── Commit (Ctrl+S) ──
	case "ctrl+s":
		if m.sqlResultMode || !m.HasPendingChanges() {
			return m, nil
		}
		if m.tableDesc == nil || len(m.tableDesc.PrimaryKeys) == 0 {
			// Can't commit without PK info (for updates/deletes)
			// Still allow inserts-only if no updates/deletes
			hasNonInsert := len(m.deletedRows) > 0
			if !hasNonInsert {
				for r := 0; r < m.newRowStart && r < len(m.rows); r++ {
					if m.isCellModified(r, 0) { // check any column
						for c := range m.columns {
							if m.isCellModified(r, c) {
								hasNonInsert = true
								break
							}
						}
						if hasNonInsert {
							break
						}
					}
				}
			}
			if hasNonInsert {
				return m, nil // can't commit updates/deletes without PK
			}
		}
		_, preview := m.generateCommitStatements()
		if preview == "" {
			return m, nil
		}
		return m, func() tea.Msg {
			return RequestCommitMsg{SQLPreview: preview}
		}

	// ── Discard changes (Ctrl+Z) ──
	case "ctrl+z":
		if m.sqlResultMode {
			return m, nil
		}
		// push undo before discard so it can be undone
		snap := m.saveSnapshot()
		m.undoStack = append(m.undoStack, snap)
		if len(m.undoStack) > 100 {
			m.undoStack = m.undoStack[1:]
		}
		m.redoStack = nil

		m.discardChanges()
		return m, nil

	// ── Undo ──
	case "u":
		if m.sqlResultMode || len(m.undoStack) == 0 {
			return m, nil
		}
		m.redoStack = append(m.redoStack, m.saveSnapshot())
		snap := m.undoStack[len(m.undoStack)-1]
		m.undoStack = m.undoStack[:len(m.undoStack)-1]
		m.rows = snap.rows
		m.deletedRows = snap.deletedRows
		m.newRowStart = snap.newRowStart
		m.cursorRow = snap.cursorRow
		m.cursorCol = snap.cursorCol
		m.colWidths = snap.colWidths
		// Adjust scroll to keep cursor visible
		bh := m.bodyHeight()
		if m.cursorRow < m.scrollOffsetY {
			m.scrollOffsetY = m.cursorRow
		} else if bh > 0 && m.cursorRow >= m.scrollOffsetY+bh {
			m.scrollOffsetY = m.cursorRow - bh + 1
		}
		if m.cursorCol < m.scrollOffsetX {
			m.scrollOffsetX = m.cursorCol
		}
		return m, nil

	// ── Redo ──
	case "U":
		if m.sqlResultMode || len(m.redoStack) == 0 {
			return m, nil
		}
		m.undoStack = append(m.undoStack, m.saveSnapshot())
		snap := m.redoStack[len(m.redoStack)-1]
		m.redoStack = m.redoStack[:len(m.redoStack)-1]
		m.rows = snap.rows
		m.deletedRows = snap.deletedRows
		m.newRowStart = snap.newRowStart
		m.cursorRow = snap.cursorRow
		m.cursorCol = snap.cursorCol
		m.colWidths = snap.colWidths
		// Adjust scroll to keep cursor visible
		bh := m.bodyHeight()
		if m.cursorRow < m.scrollOffsetY {
			m.scrollOffsetY = m.cursorRow
		} else if bh > 0 && m.cursorRow >= m.scrollOffsetY+bh {
			m.scrollOffsetY = m.cursorRow - bh + 1
		}
		if m.cursorCol < m.scrollOffsetX {
			m.scrollOffsetX = m.cursorCol
		}
		return m, nil

	// ── Paste ──
	case "p":
		if m.sqlResultMode || len(m.rows) == 0 {
			return m, nil
		}
		if m.yankType == "" || m.yankedCells == nil {
			// No internal buffer — try reading from clipboard for cell paste
			text, err := readFromClipboard()
			if err != nil || text == "" {
				return m, nil
			}
			text = strings.TrimRight(text, "\n\r")
			if strings.Contains(text, "\t") && !strings.Contains(text, "\n") {
				// Single row paste from clipboard
				snap := m.saveSnapshot()
				m.undoStack = append(m.undoStack, snap)
				if len(m.undoStack) > 100 {
					m.undoStack = m.undoStack[1:]
				}
				m.redoStack = nil
				parts := strings.Split(text, "\t")
				for i, part := range parts {
					col := i
					if col >= len(m.columns) || m.cursorRow >= len(m.rows) {
						break
					}
					if part == "NULL" {
						m.rows[m.cursorRow][col] = nil
					} else {
						m.rows[m.cursorRow][col] = part
					}
				}
				return m, func() tea.Msg {
					return CopyResultMsg{Count: len(parts), Kind: "cells pasted"}
				}
			} else if strings.Contains(text, "\n") {
				// Multi-row paste from clipboard
				snap := m.saveSnapshot()
				m.undoStack = append(m.undoStack, snap)
				if len(m.undoStack) > 100 {
					m.undoStack = m.undoStack[1:]
				}
				m.redoStack = nil
				lines := strings.Split(text, "\n")
				count := 0
				for i, line := range lines {
					row := m.cursorRow + i
					if row >= len(m.rows) {
						break
					}
					parts := strings.Split(line, "\t")
					for j, part := range parts {
						col := j
						if col >= len(m.columns) {
							break
						}
						if part == "NULL" {
							m.rows[row][col] = nil
						} else {
							m.rows[row][col] = part
						}
					}
					count++
				}
				return m, func() tea.Msg {
					return CopyResultMsg{Count: count, Kind: "rows pasted"}
				}
			} else {
				// Single cell paste from clipboard
				if m.cursorRow >= len(m.rows) || m.cursorCol >= len(m.rows[m.cursorRow]) {
					return m, nil
				}
				snap := m.saveSnapshot()
				m.undoStack = append(m.undoStack, snap)
				if len(m.undoStack) > 100 {
					m.undoStack = m.undoStack[1:]
				}
				m.redoStack = nil
				if text == "NULL" {
					m.rows[m.cursorRow][m.cursorCol] = nil
				} else {
					m.rows[m.cursorRow][m.cursorCol] = text
				}
				return m, func() tea.Msg {
					return CopyResultMsg{Count: 1, Kind: "cell pasted"}
				}
			}
		}

		// Internal buffer paste
		snap := m.saveSnapshot()
		m.undoStack = append(m.undoStack, snap)
		if len(m.undoStack) > 100 {
			m.undoStack = m.undoStack[1:]
		}
		m.redoStack = nil

		switch m.yankType {
		case "cell":
			if len(m.yankedCells) > 0 && len(m.yankedCells[0]) > 0 {
				if m.cursorRow < len(m.rows) && m.cursorCol < len(m.rows[m.cursorRow]) {
					m.rows[m.cursorRow][m.cursorCol] = m.yankedCells[0][0]
				}
			}
			return m, func() tea.Msg {
				return CopyResultMsg{Count: 1, Kind: "cell pasted"}
			}
		case "row":
			if len(m.yankedCells) > 0 && m.cursorRow < len(m.rows) {
				src := m.yankedCells[0]
				for i := 0; i < len(src) && i < len(m.rows[m.cursorRow]); i++ {
					m.rows[m.cursorRow][i] = src[i]
				}
			}
			return m, func() tea.Msg {
				return CopyResultMsg{Count: 1, Kind: "row pasted"}
			}
		case "rows":
			count := 0
			for i, srcRow := range m.yankedCells {
				row := m.cursorRow + i
				if row >= len(m.rows) {
					break
				}
				for j := 0; j < len(srcRow) && j < len(m.rows[row]); j++ {
					m.rows[row][j] = srcRow[j]
				}
				count++
			}
			return m, func() tea.Msg {
				return CopyResultMsg{Count: count, Kind: "rows pasted"}
			}
		}
		return m, nil

	// ── Sort ──
	case "s":
		if m.sqlResultMode || len(m.columns) == 0 {
			return m, nil
		}
		col := m.columns[m.cursorCol]
		if m.sortColumn == col {
			switch m.sortDir {
			case "":
				m.sortDir = "ASC"
			case "ASC":
				m.sortDir = "DESC"
			case "DESC":
				m.sortColumn = ""
				m.sortDir = ""
			}
		} else {
			m.sortColumn = col
			m.sortDir = "ASC"
		}
		m.page = 0
		m.loading = true
		return m, m.reloadDataCmd()

	// ── Yank (copy) ──
	case "y":
		if len(m.rows) == 0 {
			return m, nil
		}
		// First y: set pending, wait for second y
		m.yankPending = true
		// Also copy current cell value
		if m.cursorRow < len(m.rows) && m.cursorCol < len(m.rows[m.cursorRow]) {
			text := formatCellValue(m.rows[m.cursorRow][m.cursorCol])
			copyToClipboard(text)
			m.yankedCells = [][]interface{}{{m.rows[m.cursorRow][m.cursorCol]}}
			m.yankType = "cell"
			return m, func() tea.Msg {
				return CopyResultMsg{Count: 1, Kind: "cell"}
			}
		}
		return m, nil

	// ── Visual line select ──
	case "V":
		if len(m.rows) > 0 {
			m.visualMode = true
			m.visualStartRow = m.cursorRow
		}
		return m, nil

	// ── Filter ──
	case "/":
		m.filterMode = true
		if m.activeFilter != "" {
			m.filterInput = m.activeFilter
		} else if m.cursorCol < len(m.columns) {
			m.filterInput = m.columns[m.cursorCol] + "="
		}
		return m, nil

	// ── Quick filter ──
	case "f":
		if len(m.columns) == 0 || m.cursorRow >= len(m.rows) || m.cursorCol >= len(m.columns) {
			return m, nil
		}
		val := m.rows[m.cursorRow][m.cursorCol]
		col := m.columns[m.cursorCol]

		// Determine driver-specific quoting / placeholder.
		var driverType db.DriverType
		if m.driver != nil {
			driverType = m.driver.DriverName()
		} else {
			driverType = db.Postgres // fallback for tests
		}
		builder := sqlBuilder{driverType: driverType}

		if val == nil {
			// IS NULL doesn't need parameterized args
			m.activeFilter = fmt.Sprintf("%s IS NULL", builder.quote(col))
			m.activeFilterArgs = nil
		} else {
			m.activeFilter = fmt.Sprintf("%s = %s", builder.quote(col), builder.placeholder(1))
			m.activeFilterArgs = []interface{}{val}
		}
		m.filterInput = m.activeFilter
		m.page = 0
		m.loading = true
		return m, m.reloadDataCmd()

	// ── Clear filter ──
	case "esc":
		if m.activeFilter != "" {
			m.activeFilter = ""
			m.activeFilterArgs = nil
			m.filterInput = ""
			m.page = 0
			m.loading = true
			return m, m.reloadDataCmd()
		}

	case "ctrl+_":
		if m.activeFilter != "" {
			m.activeFilter = ""
			m.activeFilterArgs = nil
			m.filterInput = ""
			m.page = 0
			m.loading = true
			return m, m.reloadDataCmd()
		}
	}

	return m, nil
}

// ── View ──────────────────────────────────────────────────────────

// View renders the data view pane.
func (m DataViewModel) View() string {
	var sb strings.Builder

	// Header: schema.table
	title := "Data View"
	if m.tableName != "" {
		if m.schemaName != "" {
			title = m.schemaName + "." + m.tableName
		} else {
			title = m.tableName
		}
	}
	if m.sqlResultMode {
		title = "Query Results"
	}
	sb.WriteString(m.theme.DataHeader.Render(title))
	sb.WriteString("\n")

	if len(m.columns) == 0 {
		if m.loading {
			sb.WriteString(m.theme.DataRow.Render("  Loading..."))
		} else {
			sb.WriteString(m.theme.DataRow.Render("  Select a table to view data"))
		}
		return m.renderBorder(sb.String())
	}

	colStart, colEnd := m.visibleColumns()
	rowStart, rowEnd := m.visibleRows()

	// Column separator style
	colSep := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).Render("│")

	// Build column header line with sort indicators
	headerParts := []string{m.renderRowNum("#")}
	for c := colStart; c < colEnd; c++ {
		if c > colStart {
			headerParts = append(headerParts, colSep)
		}
		w := m.colWidths[c]
		colName := m.columns[c]

		// Add sort indicator
		sortIndicator := ""
		if m.sortColumn == colName {
			switch m.sortDir {
			case "ASC":
				sortIndicator = " ▲"
			case "DESC":
				sortIndicator = " ▼"
			}
		}

		headerText := truncate(colName+sortIndicator, w)
		headerParts = append(headerParts, m.theme.DataHeader.
			Width(w).
			MaxWidth(w+2).
			MaxHeight(1).
			Inline(true).
			Render(headerText))
	}
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, headerParts...))
	sb.WriteString("\n")

	// Separator line
	sepParts := []string{m.renderRowNum("───")}
	for c := colStart; c < colEnd; c++ {
		if c > colStart {
			sepParts = append(sepParts, colSep)
		}
		w := m.colWidths[c]
		sep := strings.Repeat("─", w)
		sepParts = append(sepParts, m.theme.DataRow.Width(w).MaxWidth(w+2).MaxHeight(1).Render(sep))
	}
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, sepParts...))
	sb.WriteString("\n")

	// Data rows
	for r := rowStart; r < rowEnd; r++ {
		row := m.rows[r]
		rowNum := fmt.Sprintf("%d", r+1+(m.page*m.pageSize))
		rowParts := []string{m.renderRowNum(rowNum)}

		isDeleted := m.deletedRows[r]
		isNew := r >= m.newRowStart

		for c := colStart; c < colEnd; c++ {
			if c > colStart {
				rowParts = append(rowParts, colSep)
			}
			w := m.colWidths[c]
			var val interface{}
			if c < len(row) {
				val = row[c]
			}

			cellText := formatCellValue(val)

			// Edit mode: show edit buffer with cursor
			if m.editMode && r == m.cursorRow && c == m.cursorCol {
				displayText := m.editBuffer
				runes := []rune(displayText)
				if m.editCursorPos < len(runes) {
					displayText = string(runes[:m.editCursorPos]) + "▏" + string(runes[m.editCursorPos:])
				} else {
					displayText = displayText + "▏"
				}
				cellText = displayText
			}

			displayText := truncate(cellText, w)

			cellStyle := m.getCellStyle(r, c, val, isDeleted, isNew, w)
			rowParts = append(rowParts, cellStyle.Render(displayText))
		}

		sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, rowParts...))
		sb.WriteString("\n")
	}

	// Filter input (if in filter mode)
	if m.filterMode {
		filterLine := m.theme.DataFilterInput.Render("WHERE: " + m.filterInput + "▏")
		sb.WriteString(filterLine)
		sb.WriteString("\n")
	}

	// Footer
	footerParts := []string{
		fmt.Sprintf("Rows: %d", len(m.rows)),
		fmt.Sprintf("Page %d/%d", m.page+1, max(m.totalPages, 1)),
	}
	if m.activeFilter != "" {
		footerParts = append(footerParts, fmt.Sprintf("Filter: %s", m.activeFilter))
	}
	if m.sortColumn != "" && m.sortDir != "" {
		footerParts = append(footerParts, fmt.Sprintf("Sort: %s %s", m.sortColumn, m.sortDir))
	}
	if m.HasPendingChanges() {
		footerParts = append(footerParts, "⚡ Unsaved changes")
	}
	if m.visualMode {
		startR, endR := m.visualStartRow, m.cursorRow
		if startR > endR {
			startR, endR = endR, startR
		}
		footerParts = append(footerParts, fmt.Sprintf("-- VISUAL LINE -- (%d selected)", endR-startR+1))
	}
	sb.WriteString(m.theme.DataFooter.Render(strings.Join(footerParts, " │ ")))

	result := m.renderBorder(sb.String())

	// Overlay copy format menu if visible
	if m.copyFormatMenu {
		menu := m.renderCopyFormatMenu()
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, menu)
	}

	return result
}

// getCellStyle returns the appropriate style for a cell based on state.
func (m DataViewModel) getCellStyle(row, col int, val interface{}, isDeleted, isNew bool, w int) lipgloss.Style {
	// Visual mode selection highlight
	if m.visualMode {
		startR, endR := m.visualStartRow, m.cursorRow
		if startR > endR {
			startR, endR = endR, startR
		}
		if row >= startR && row <= endR {
			return m.theme.DataSelected.Width(w).Inline(true)
		}
	}

	switch {
	case m.editMode && row == m.cursorRow && col == m.cursorCol:
		return m.theme.DataCellEditing.Width(w).Inline(true)
	case isDeleted:
		return m.theme.DataRowDeleted.Width(w).Inline(true)
	case isNew:
		if row == m.cursorRow && col == m.cursorCol {
			return m.theme.DataCellActive.Width(w).Inline(true)
		}
		return m.theme.DataRowNew.Width(w).Inline(true)
	case m.isCellModified(row, col):
		return m.theme.DataCellModified.Width(w).Inline(true)
	case row == m.cursorRow && col == m.cursorCol:
		return m.theme.DataCellActive.Width(w).Inline(true)
	case row == m.cursorRow:
		return m.theme.DataSelected.Width(w).Inline(true)
	case val == nil:
		return m.theme.DataNull.Width(w).Inline(true)
	case row%2 == 0:
		return m.theme.DataRow.Width(w).Inline(true)
	default:
		return m.theme.DataRowAlt.Width(w).Inline(true)
	}
}

func (m DataViewModel) renderRowNum(s string) string {
	return m.theme.DataRowNumber.Width(5).Render(s)
}

func (m DataViewModel) renderBorder(content string) string {
	borderStyle := m.theme.Border
	if m.focused {
		borderStyle = m.theme.FocusedBorder
	}
	return borderStyle.
		Width(m.width - 2).
		Height(m.height - 2).
		MaxHeight(m.height).
		Render(content)
}

// truncate truncates a string to fit within maxWidth (rune-aware).
func truncate(s string, maxWidth int) string {
	// Replace newlines/tabs with space to prevent multi-line cells
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\t", " ")

	if runewidth.StringWidth(s) <= maxWidth {
		return s
	}
	if maxWidth <= 1 {
		return "…"
	}
	return runewidth.Truncate(s, maxWidth-1, "") + "…"
}

// formatRowAsText formats a row as tab-separated text for clipboard.
func (m DataViewModel) formatRowAsText(row int) string {
	if row >= len(m.rows) {
		return ""
	}
	var parts []string
	for _, v := range m.rows[row] {
		parts = append(parts, formatCellValue(v))
	}
	return strings.Join(parts, "\t")
}

// readFromClipboard reads text from the system clipboard.
func readFromClipboard() (string, error) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbpaste")
	case "linux":
		cmd = exec.Command("xclip", "-selection", "clipboard", "-o")
	default:
		return "", fmt.Errorf("unsupported OS")
	}
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// copyToClipboard copies text to the system clipboard.
func copyToClipboard(text string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbcopy")
	case "linux":
		cmd = exec.Command("xclip", "-selection", "clipboard")
	default:
		return
	}
	cmd.Stdin = strings.NewReader(text)
	_ = cmd.Run()
}

// formatSQLCopyValue formats a single value for SQL INSERT statement.
func formatSQLCopyValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		s := fmt.Sprintf("%v", val)
		s = strings.ReplaceAll(s, "'", "''")
		return "'" + s + "'"
	}
}

// formatCSVCopyValue formats a single value for CSV output.
func formatCSVCopyValue(v interface{}) string {
	if v == nil {
		return ""
	}
	s := fmt.Sprintf("%v", v)
	if strings.ContainsAny(s, ",\"\n\r") {
		s = strings.ReplaceAll(s, "\"", "\"\"")
		return "\"" + s + "\""
	}
	return s
}

// formatCopySelection formats the selected rows in the specified format.
func (m DataViewModel) formatCopySelection(startR, endR, format int) string {
	var sb strings.Builder
	switch format {
	case 0: // SQL INSERT
		quoteID := func(name string) string {
			return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
		}
		tableName := quoteID(m.tableName)
		if m.schemaName != "" {
			tableName = quoteID(m.schemaName) + "." + quoteID(m.tableName)
		}
		cols := make([]string, len(m.columns))
		for i, c := range m.columns {
			cols[i] = quoteID(c)
		}
		colList := strings.Join(cols, ", ")
		for r := startR; r <= endR && r < len(m.rows); r++ {
			vals := make([]string, len(m.rows[r]))
			for c, v := range m.rows[r] {
				vals[c] = formatSQLCopyValue(v)
			}
			sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n",
				tableName, colList, strings.Join(vals, ", ")))
		}
	case 1: // CSV with header
		headerVals := make([]string, len(m.columns))
		for i, c := range m.columns {
			headerVals[i] = formatCSVCopyValue(c)
		}
		sb.WriteString(strings.Join(headerVals, ","))
		sb.WriteString("\n")
		for r := startR; r <= endR && r < len(m.rows); r++ {
			vals := make([]string, len(m.rows[r]))
			for c, v := range m.rows[r] {
				vals[c] = formatCSVCopyValue(v)
			}
			sb.WriteString(strings.Join(vals, ","))
			if r < endR {
				sb.WriteString("\n")
			}
		}
	case 2: // CSV without header
		for r := startR; r <= endR && r < len(m.rows); r++ {
			vals := make([]string, len(m.rows[r]))
			for c, v := range m.rows[r] {
				vals[c] = formatCSVCopyValue(v)
			}
			sb.WriteString(strings.Join(vals, ","))
			if r < endR {
				sb.WriteString("\n")
			}
		}
	case 3: // JSON
		var records []map[string]interface{}
		for r := startR; r <= endR && r < len(m.rows); r++ {
			record := make(map[string]interface{})
			for c, col := range m.columns {
				if c < len(m.rows[r]) {
					record[col] = m.rows[r][c]
				}
			}
			records = append(records, record)
		}
		jsonBytes, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			sb.WriteString("[]")
		} else {
			sb.Write(jsonBytes)
		}
	}
	return sb.String()
}

// renderCopyFormatMenu renders the copy format selection menu overlay.
func (m DataViewModel) renderCopyFormatMenu() string {
	items := []string{
		"  SQL INSERT",
		"  CSV (with header)",
		"  CSV (without header)",
		"  JSON",
	}
	var sb strings.Builder
	title := m.theme.DialogTitle.Render(" Copy Format ")
	sb.WriteString(title)
	sb.WriteString("\n\n")
	for i, item := range items {
		if i == m.copyFormatIdx {
			sb.WriteString(m.theme.DataSelected.Render("▸ " + item))
		} else {
			sb.WriteString(m.theme.DataRow.Render("  " + item))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpDesc.Render(" ↑↓: Select  Enter: Copy  Esc: Cancel "))
	return m.theme.HelpBox.Render(sb.String())
}

// IsCopyFormatMenu returns whether the copy format menu is visible.
func (m DataViewModel) IsCopyFormatMenu() bool {
	return m.copyFormatMenu
}
