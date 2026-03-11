package ui

import (
	"context"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/db"
)

// ── Structure View Tab ────────────────────────────────────────────

// StructureTab represents which tab is active in the structure view.
type StructureTab int

const (
	TabColumns StructureTab = iota
	TabIndexes
	TabForeignKeys
)

// ── Structure View Messages ───────────────────────────────────────

// ShowStructureMsg requests the structure view for a table.
type ShowStructureMsg struct {
	Schema string
	Table  string
}

// StructureLoadedMsg holds the loaded table structure.
type StructureLoadedMsg struct {
	Desc *db.TableDescription
	Err  error
}

// DDLExecuteResultMsg holds the result of a DDL operation.
type DDLExecuteResultMsg struct {
	Message string
	Err     error
}

// ── DDL Input Mode ────────────────────────────────────────────────

// ddlInputMode represents which DDL input form is active.
type ddlInputMode int

const (
	ddlNone ddlInputMode = iota
	ddlAddColumn
	ddlDropColumn
	ddlCreateTable
	ddlDropTable
	ddlCreateIndex
	ddlDropIndex
)

// ── Structure View Commands ───────────────────────────────────────

// LoadStructureCmd loads table structure from the driver.
func LoadStructureCmd(driver db.Driver, schema, table string) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		desc, err := driver.DescribeTable(ctx, schema, table)
		if err != nil {
			return StructureLoadedMsg{Err: err}
		}
		return StructureLoadedMsg{Desc: desc}
	}
}

// ExecuteDDLCmd executes a DDL statement.
func ExecuteDDLCmd(driver db.Driver, ddl string) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		_, err := driver.Exec(ctx, ddl)
		if err != nil {
			return DDLExecuteResultMsg{Err: err}
		}
		return DDLExecuteResultMsg{Message: "DDL executed successfully"}
	}
}

// ── StructureViewModel ────────────────────────────────────────────

// StructureViewModel manages the table structure display with DDL operations.
type StructureViewModel struct {
	schema string
	table  string
	desc   *db.TableDescription
	driver db.Driver

	activeTab   StructureTab
	cursorRow   int
	cursorCol   int
	scrollY     int

	// DDL input state
	ddlMode       ddlInputMode
	ddlFields     []ddlField
	ddlFieldIdx   int
	ddlPreview    string
	ddlConfirming bool

	// Create table state
	createTableName    string
	createTableColumns []createColumnDef

	focused bool
	width   int
	height  int
	theme   *Theme
	loading bool
}

// ddlField represents a single input field in a DDL form.
type ddlField struct {
	label string
	value string
}

// createColumnDef represents a column definition for CREATE TABLE.
type createColumnDef struct {
	name     string
	dataType string
	nullable bool
	defVal   string
}

// NewStructureViewModel creates a new StructureViewModel.
func NewStructureViewModel(driver db.Driver, theme *Theme) StructureViewModel {
	return StructureViewModel{
		driver: driver,
		theme:  theme,
	}
}

// SetFocused sets whether this view is focused.
func (m *StructureViewModel) SetFocused(f bool) {
	m.focused = f
}

// SetSize updates the view size.
func (m *StructureViewModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// SetTable sets the current table and triggers structure loading.
func (m *StructureViewModel) SetTable(schema, table string) {
	m.schema = schema
	m.table = table
	m.desc = nil
	m.cursorRow = 0
	m.scrollY = 0
	m.ddlMode = ddlNone
	m.ddlConfirming = false
}

// IsInputMode returns true when the structure view is in DDL input mode.
func (m StructureViewModel) IsInputMode() bool {
	return m.ddlMode != ddlNone
}

// IsDDLConfirming returns true when a DDL preview is being confirmed.
func (m StructureViewModel) IsDDLConfirming() bool {
	return m.ddlConfirming
}

// bodyHeight returns the number of visible rows.
func (m StructureViewModel) bodyHeight() int {
	h := m.height - 8
	if h < 3 {
		h = 3
	}
	return h
}

// ── Update ────────────────────────────────────────────────────────

// Update handles messages for the structure view.
func (m StructureViewModel) Update(msg tea.Msg) (StructureViewModel, tea.Cmd) {
	switch msg := msg.(type) {
	case StructureLoadedMsg:
		m.loading = false
		if msg.Err != nil {
			return m, nil
		}
		m.desc = msg.Desc
		m.cursorRow = 0
		m.scrollY = 0
		return m, nil

	case tea.KeyMsg:
		if m.ddlConfirming {
			return m.handleDDLConfirm(msg)
		}
		if m.ddlMode != ddlNone {
			return m.handleDDLInput(msg)
		}
		return m.handleKeyInput(msg)
	}
	return m, nil
}

func (m StructureViewModel) handleKeyInput(msg tea.KeyMsg) (StructureViewModel, tea.Cmd) {
	switch msg.String() {
	// Tab switching
	case "1":
		m.activeTab = TabColumns
		m.cursorRow = 0
		m.cursorCol = 0
		m.scrollY = 0
	case "2":
		m.activeTab = TabIndexes
		m.cursorRow = 0
		m.cursorCol = 0
		m.scrollY = 0
	case "3":
		m.activeTab = TabForeignKeys
		m.cursorRow = 0
		m.cursorCol = 0
		m.scrollY = 0
	case "tab":
		m.activeTab = (m.activeTab + 1) % 3
		m.cursorRow = 0
		m.cursorCol = 0
		m.scrollY = 0

	// Navigation
	case "j", "down":
		maxRow := m.maxRows() - 1
		if maxRow < 0 {
			maxRow = 0
		}
		if m.cursorRow < maxRow {
			m.cursorRow++
			bh := m.bodyHeight()
			if m.cursorRow >= m.scrollY+bh {
				m.scrollY = m.cursorRow - bh + 1
			}
		}
	case "k", "up":
		if m.cursorRow > 0 {
			m.cursorRow--
			if m.cursorRow < m.scrollY {
				m.scrollY = m.cursorRow
			}
		}
	case "h", "left":
		if m.cursorCol > 0 {
			m.cursorCol--
		}
	case "l", "right":
		maxCol := m.maxCols() - 1
		if maxCol < 0 {
			maxCol = 0
		}
		if m.cursorCol < maxCol {
			m.cursorCol++
		}

	// Yank cell value
	case "y":
		cellVal := m.getCellValue()
		if cellVal != "" {
			return m, copyToClipboardOSC52(cellVal)
		}

	// DDL operations
	case "a":
		// Add column
		if m.activeTab == TabColumns {
			m.ddlMode = ddlAddColumn
			m.ddlFields = []ddlField{
				{label: "Column Name", value: ""},
				{label: "Data Type", value: "VARCHAR(255)"},
				{label: "Nullable (y/n)", value: "y"},
				{label: "Default Value", value: ""},
			}
			m.ddlFieldIdx = 0
			return m, nil
		}
		// Create index
		if m.activeTab == TabIndexes {
			m.ddlMode = ddlCreateIndex
			m.ddlFields = []ddlField{
				{label: "Index Name", value: ""},
				{label: "Columns (comma-separated)", value: ""},
				{label: "Unique (y/n)", value: "n"},
			}
			m.ddlFieldIdx = 0
			return m, nil
		}

	case "d":
		// Delete column
		if m.activeTab == TabColumns && m.desc != nil && m.cursorRow < len(m.desc.Columns) {
			col := m.desc.Columns[m.cursorRow]
			m.ddlPreview = m.buildDropColumnDDL(col.Name)
			m.ddlMode = ddlDropColumn
			m.ddlConfirming = true
			return m, nil
		}
		// Drop index
		if m.activeTab == TabIndexes && m.desc != nil && m.cursorRow < len(m.desc.Indexes) {
			idx := m.desc.Indexes[m.cursorRow]
			m.ddlPreview = m.buildDropIndexDDL(idx.Name)
			m.ddlMode = ddlDropIndex
			m.ddlConfirming = true
			return m, nil
		}
	}

	return m, nil
}

// handleDDLInput handles key input in DDL form mode.
func (m StructureViewModel) handleDDLInput(msg tea.KeyMsg) (StructureViewModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.ddlMode = ddlNone
		m.ddlFieldIdx = 0
		m.ddlConfirming = false
		m.createTableColumns = nil
		return m, nil

	case "enter":
		if m.ddlMode == ddlCreateTable {
			return m.handleCreateTableInput(msg)
		}
		// Move to next field or confirm
		if m.ddlFieldIdx < len(m.ddlFields)-1 {
			m.ddlFieldIdx++
			return m, nil
		}
		// All fields entered, show preview
		ddl := m.buildDDLFromFields()
		if ddl == "" {
			m.ddlMode = ddlNone
			return m, nil
		}
		m.ddlPreview = ddl
		m.ddlConfirming = true
		return m, nil

	case "tab":
		// Move to next field
		if m.ddlFieldIdx < len(m.ddlFields)-1 {
			m.ddlFieldIdx++
		}
		return m, nil

	case "shift+tab":
		// Move to previous field
		if m.ddlFieldIdx > 0 {
			m.ddlFieldIdx--
		}
		return m, nil

	case "backspace":
		if m.ddlFieldIdx < len(m.ddlFields) {
			f := &m.ddlFields[m.ddlFieldIdx]
			if len(f.value) > 0 {
				f.value = f.value[:len(f.value)-1]
			}
		}
		return m, nil

	default:
		if msg.Type == tea.KeyRunes && m.ddlFieldIdx < len(m.ddlFields) {
			f := &m.ddlFields[m.ddlFieldIdx]
			f.value += string(msg.Runes)
		}
		return m, nil
	}
}

// handleCreateTableInput handles input for CREATE TABLE multi-step form.
func (m StructureViewModel) handleCreateTableInput(msg tea.KeyMsg) (StructureViewModel, tea.Cmd) {
	switch msg.String() {
	case "enter":
		// Step 1: table name
		if m.createTableName == "" && len(m.ddlFields) > 0 {
			m.createTableName = m.ddlFields[0].value
			if m.createTableName == "" {
				m.ddlMode = ddlNone
				return m, nil
			}
			// Reset fields for column definition
			m.ddlFields = []ddlField{
				{label: "Column Name (empty to finish)", value: ""},
				{label: "Data Type", value: "VARCHAR(255)"},
				{label: "Nullable (y/n)", value: "y"},
				{label: "Default Value", value: ""},
			}
			m.ddlFieldIdx = 0
			return m, nil
		}

		// Step 2+: adding columns
		if m.ddlFieldIdx < len(m.ddlFields)-1 {
			m.ddlFieldIdx++
			return m, nil
		}

		// Column name empty → finish
		colName := m.ddlFields[0].value
		if colName == "" {
			// No more columns; show preview
			if len(m.createTableColumns) == 0 {
				m.ddlMode = ddlNone
				m.createTableName = ""
				return m, nil
			}
			m.ddlPreview = m.buildCreateTableDDL()
			m.ddlConfirming = true
			return m, nil
		}

		// Add column to list
		nullable := strings.ToLower(m.ddlFields[2].value) == "y"
		m.createTableColumns = append(m.createTableColumns, createColumnDef{
			name:     colName,
			dataType: m.ddlFields[1].value,
			nullable: nullable,
			defVal:   m.ddlFields[3].value,
		})

		// Reset for next column
		m.ddlFields = []ddlField{
			{label: "Column Name (empty to finish)", value: ""},
			{label: "Data Type", value: "VARCHAR(255)"},
			{label: "Nullable (y/n)", value: "y"},
			{label: "Default Value", value: ""},
		}
		m.ddlFieldIdx = 0
		return m, nil

	default:
		// Delegate to generic DDL input for character handling
		return m.handleDDLInput(msg)
	}
}

// handleDDLConfirm handles confirmation of DDL preview.
func (m StructureViewModel) handleDDLConfirm(msg tea.KeyMsg) (StructureViewModel, tea.Cmd) {
	switch msg.String() {
	case "y", "Y", "enter":
		ddl := m.ddlPreview
		m.ddlMode = ddlNone
		m.ddlConfirming = false
		m.ddlPreview = ""
		m.createTableName = ""
		m.createTableColumns = nil
		return m, ExecuteDDLCmd(m.driver, ddl)
	case "n", "N", "esc":
		m.ddlMode = ddlNone
		m.ddlConfirming = false
		m.ddlPreview = ""
		m.createTableName = ""
		m.createTableColumns = nil
		return m, nil
	}
	return m, nil
}

// ── DDL Builders ──────────────────────────────────────────────────

// buildDDLFromFields builds a DDL statement from the current form fields.
func (m StructureViewModel) buildDDLFromFields() string {
	switch m.ddlMode {
	case ddlAddColumn:
		return m.buildAddColumnDDL()
	case ddlCreateIndex:
		return m.buildCreateIndexDDL()
	default:
		return ""
	}
}

func (m StructureViewModel) buildAddColumnDDL() string {
	if len(m.ddlFields) < 4 {
		return ""
	}
	colName := strings.TrimSpace(m.ddlFields[0].value)
	dataType := strings.TrimSpace(m.ddlFields[1].value)
	nullable := strings.ToLower(strings.TrimSpace(m.ddlFields[2].value))
	defVal := strings.TrimSpace(m.ddlFields[3].value)

	if colName == "" || dataType == "" {
		return ""
	}

	tableRef := m.quoteTable()
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		tableRef, quoteIdent(colName), dataType)

	if nullable != "y" && nullable != "yes" {
		ddl += " NOT NULL"
	}
	if defVal != "" {
		ddl += " DEFAULT " + defVal
	}

	return ddl
}

func (m StructureViewModel) buildDropColumnDDL(colName string) string {
	tableRef := m.quoteTable()
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableRef, quoteIdent(colName))
}

func (m StructureViewModel) buildCreateIndexDDL() string {
	if len(m.ddlFields) < 3 {
		return ""
	}
	idxName := strings.TrimSpace(m.ddlFields[0].value)
	columns := strings.TrimSpace(m.ddlFields[1].value)
	unique := strings.ToLower(strings.TrimSpace(m.ddlFields[2].value))

	if idxName == "" || columns == "" {
		return ""
	}

	tableRef := m.quoteTable()
	uniqueStr := ""
	if unique == "y" || unique == "yes" {
		uniqueStr = "UNIQUE "
	}

	// Quote individual column names
	cols := strings.Split(columns, ",")
	quotedCols := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c != "" {
			quotedCols = append(quotedCols, quoteIdent(c))
		}
	}

	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr, quoteIdent(idxName), tableRef, strings.Join(quotedCols, ", "))
}

func (m StructureViewModel) buildDropIndexDDL(idxName string) string {
	return fmt.Sprintf("DROP INDEX %s", quoteIdent(idxName))
}

func (m StructureViewModel) buildCreateTableDDL() string {
	if m.createTableName == "" || len(m.createTableColumns) == 0 {
		return ""
	}

	var colDefs []string
	for _, col := range m.createTableColumns {
		def := fmt.Sprintf("  %s %s", quoteIdent(col.name), col.dataType)
		if !col.nullable {
			def += " NOT NULL"
		}
		if col.defVal != "" {
			def += " DEFAULT " + col.defVal
		}
		colDefs = append(colDefs, def)
	}

	tableRef := quoteIdent(m.createTableName)
	if m.schema != "" {
		tableRef = quoteIdent(m.schema) + "." + tableRef
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		tableRef, strings.Join(colDefs, ",\n"))
}

func (m StructureViewModel) buildDropTableDDL() string {
	return fmt.Sprintf("DROP TABLE %s", m.quoteTable())
}

// quoteTable returns the quoted schema.table reference.
func (m StructureViewModel) quoteTable() string {
	if m.schema != "" {
		return quoteIdent(m.schema) + "." + quoteIdent(m.table)
	}
	return quoteIdent(m.table)
}

// quoteIdent quotes a SQL identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// maxRows returns the number of items in the current tab.
func (m StructureViewModel) maxRows() int {
	if m.desc == nil {
		return 0
	}
	switch m.activeTab {
	case TabColumns:
		return len(m.desc.Columns)
	case TabIndexes:
		return len(m.desc.Indexes)
	case TabForeignKeys:
		return len(m.desc.ForeignKeys)
	}
	return 0
}

// maxCols returns the number of columns for the current tab.
func (m StructureViewModel) maxCols() int {
	switch m.activeTab {
	case TabColumns:
		return 5 // Name, Type, Null, Default, PK
	case TabIndexes:
		return 3 // Name, Columns, Unique
	case TabForeignKeys:
		return 3 // Name, Columns, References
	}
	return 0
}

// getCellValue returns the string value of the cell at (cursorRow, cursorCol).
func (m StructureViewModel) getCellValue() string {
	if m.desc == nil {
		return ""
	}
	switch m.activeTab {
	case TabColumns:
		if m.cursorRow >= len(m.desc.Columns) {
			return ""
		}
		col := m.desc.Columns[m.cursorRow]
		switch m.cursorCol {
		case 0:
			return col.Name
		case 1:
			return col.DataType
		case 2:
			if col.Nullable {
				return "YES"
			}
			return "NO"
		case 3:
			if col.DefaultValue != nil {
				return *col.DefaultValue
			}
			return ""
		case 4:
			if col.IsPrimaryKey {
				return "PK"
			}
			return ""
		}
	case TabIndexes:
		if m.cursorRow >= len(m.desc.Indexes) {
			return ""
		}
		idx := m.desc.Indexes[m.cursorRow]
		switch m.cursorCol {
		case 0:
			return idx.Name
		case 1:
			return strings.Join(idx.Columns, ", ")
		case 2:
			if idx.IsUnique {
				return "YES"
			}
			return "NO"
		}
	case TabForeignKeys:
		if m.cursorRow >= len(m.desc.ForeignKeys) {
			return ""
		}
		fk := m.desc.ForeignKeys[m.cursorRow]
		switch m.cursorCol {
		case 0:
			return fk.Name
		case 1:
			return strings.Join(fk.Columns, ", ")
		case 2:
			return fk.RefTable + "(" + strings.Join(fk.RefColumns, ", ") + ")"
		}
	}
	return ""
}

// ── View ──────────────────────────────────────────────────────────

// View renders the structure view.
func (m StructureViewModel) View() string {
	var sb strings.Builder

	// Title
	title := "Structure"
	if m.table != "" {
		if m.schema != "" {
			title = m.schema + "." + m.table + " - Structure"
		} else {
			title = m.table + " - Structure"
		}
	}
	sb.WriteString(m.theme.DataHeader.Render(title))
	sb.WriteString("\n")

	// Tab bar
	sb.WriteString(m.renderTabBar())
	sb.WriteString("\n")

	if m.loading {
		sb.WriteString(m.theme.DataRow.Render("  Loading..."))
		return m.renderBorder(sb.String())
	}
	if m.desc == nil {
		sb.WriteString(m.theme.DataRow.Render("  No structure loaded"))
		return m.renderBorder(sb.String())
	}

	// DDL confirm overlay
	if m.ddlConfirming {
		sb.WriteString(m.renderDDLConfirm())
		return m.renderBorder(sb.String())
	}

	// DDL input form
	if m.ddlMode != ddlNone {
		sb.WriteString(m.renderDDLForm())
		return m.renderBorder(sb.String())
	}

	// Content based on active tab
	switch m.activeTab {
	case TabColumns:
		sb.WriteString(m.renderColumnsTab())
	case TabIndexes:
		sb.WriteString(m.renderIndexesTab())
	case TabForeignKeys:
		sb.WriteString(m.renderForeignKeysTab())
	}

	// Footer with DDL hints
	sb.WriteString("\n")
	sb.WriteString(m.renderFooter())

	return m.renderBorder(sb.String())
}

func (m StructureViewModel) renderTabBar() string {
	tabs := []struct {
		label string
		tab   StructureTab
		key   string
	}{
		{"Columns", TabColumns, "1"},
		{"Indexes", TabIndexes, "2"},
		{"Foreign Keys", TabForeignKeys, "3"},
	}

	var parts []string
	for _, t := range tabs {
		label := fmt.Sprintf(" %s [%s] ", t.label, t.key)
		if m.activeTab == t.tab {
			parts = append(parts, m.theme.DialogButtonActive.Render(label))
		} else {
			parts = append(parts, m.theme.DialogButton.Render(label))
		}
	}

	return lipgloss.JoinHorizontal(lipgloss.Top, parts...)
}

func (m StructureViewModel) renderColumnsTab() string {
	if m.desc == nil || len(m.desc.Columns) == 0 {
		return m.theme.DataRow.Render("  No columns")
	}

	var sb strings.Builder

	// Dynamic column widths based on available width
	availW := m.width - 4 // border padding
	if availW < 20 {
		availW = 20
	}
	nameW := availW * 30 / 100
	typeW := availW * 25 / 100
	nullW := availW * 10 / 100
	defW := availW * 20 / 100
	pkW := availW - nameW - typeW - nullW - defW // remaining goes to last

	// Header
	header := lipgloss.JoinHorizontal(lipgloss.Top,
		m.theme.DataHeader.Width(nameW).MaxWidth(nameW).Height(1).MaxHeight(1).Render("Name"),
		m.theme.DataHeader.Width(typeW).MaxWidth(typeW).Height(1).MaxHeight(1).Render("Type"),
		m.theme.DataHeader.Width(nullW).MaxWidth(nullW).Height(1).MaxHeight(1).Render("Null"),
		m.theme.DataHeader.Width(defW).MaxWidth(defW).Height(1).MaxHeight(1).Render("Default"),
		m.theme.DataHeader.Width(pkW).MaxWidth(pkW).Height(1).MaxHeight(1).Render("PK"),
	)
	sb.WriteString(header)
	sb.WriteString("\n")

	// Separator
	sep := strings.Repeat("─", availW)
	sb.WriteString(m.theme.DataRow.Render(sep))
	sb.WriteString("\n")

	bh := m.bodyHeight()
	start := m.scrollY
	end := start + bh
	if end > len(m.desc.Columns) {
		end = len(m.desc.Columns)
	}

	for i := start; i < end; i++ {
		col := m.desc.Columns[i]

		nullStr := "NO"
		if col.Nullable {
			nullStr = "YES"
		}
		defStr := ""
		if col.DefaultValue != nil {
			defStr = *col.DefaultValue
		}
		pkStr := ""
		if col.IsPrimaryKey {
			pkStr = "PK"
		}

		vals := []string{col.Name, col.DataType, nullStr, defStr, pkStr}
		widths := []int{nameW, typeW, nullW, defW, pkW}

		var cells []string
		for ci, w := range widths {
			style := m.theme.DataRow
			if i == m.cursorRow && ci == m.cursorCol {
				style = m.theme.DataCellActive
			} else if i == m.cursorRow {
				style = m.theme.DataSelected
			}
			cells = append(cells, style.Width(w).MaxWidth(w).Height(1).MaxHeight(1).Render(truncate(vals[ci], w)))
		}

		row := lipgloss.JoinHorizontal(lipgloss.Top, cells...)
		sb.WriteString(row)
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m StructureViewModel) renderIndexesTab() string {
	if m.desc == nil || len(m.desc.Indexes) == 0 {
		return m.theme.DataRow.Render("  No indexes")
	}

	var sb strings.Builder

	// Dynamic column widths based on available width
	availW := m.width - 4
	if availW < 20 {
		availW = 20
	}
	nameW := availW * 35 / 100
	colsW := availW * 45 / 100
	uniqW := availW - nameW - colsW // remaining goes to last

	header := lipgloss.JoinHorizontal(lipgloss.Top,
		m.theme.DataHeader.Width(nameW).MaxWidth(nameW).Height(1).MaxHeight(1).Render("Name"),
		m.theme.DataHeader.Width(colsW).MaxWidth(colsW).Height(1).MaxHeight(1).Render("Columns"),
		m.theme.DataHeader.Width(uniqW).MaxWidth(uniqW).Height(1).MaxHeight(1).Render("Unique"),
	)
	sb.WriteString(header)
	sb.WriteString("\n")

	sep := strings.Repeat("─", availW)
	sb.WriteString(m.theme.DataRow.Render(sep))
	sb.WriteString("\n")

	bh := m.bodyHeight()
	start := m.scrollY
	end := start + bh
	if end > len(m.desc.Indexes) {
		end = len(m.desc.Indexes)
	}

	for i := start; i < end; i++ {
		idx := m.desc.Indexes[i]

		uniqStr := "NO"
		if idx.IsUnique {
			uniqStr = "YES"
		}

		vals := []string{idx.Name, strings.Join(idx.Columns, ", "), uniqStr}
		widths := []int{nameW, colsW, uniqW}

		var cells []string
		for ci, w := range widths {
			style := m.theme.DataRow
			if i == m.cursorRow && ci == m.cursorCol {
				style = m.theme.DataCellActive
			} else if i == m.cursorRow {
				style = m.theme.DataSelected
			}
			cells = append(cells, style.Width(w).MaxWidth(w).Height(1).MaxHeight(1).Render(truncate(vals[ci], w)))
		}

		row := lipgloss.JoinHorizontal(lipgloss.Top, cells...)
		sb.WriteString(row)
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m StructureViewModel) renderForeignKeysTab() string {
	if m.desc == nil || len(m.desc.ForeignKeys) == 0 {
		return m.theme.DataRow.Render("  No foreign keys")
	}

	var sb strings.Builder

	// Dynamic column widths based on available width
	availW := m.width - 4
	if availW < 20 {
		availW = 20
	}
	nameW := availW * 30 / 100
	colsW := availW * 30 / 100
	refW := availW - nameW - colsW // remaining goes to last

	header := lipgloss.JoinHorizontal(lipgloss.Top,
		m.theme.DataHeader.Width(nameW).MaxWidth(nameW).Height(1).MaxHeight(1).Render("Name"),
		m.theme.DataHeader.Width(colsW).MaxWidth(colsW).Height(1).MaxHeight(1).Render("Columns"),
		m.theme.DataHeader.Width(refW).MaxWidth(refW).Height(1).MaxHeight(1).Render("References"),
	)
	sb.WriteString(header)
	sb.WriteString("\n")

	sep := strings.Repeat("─", availW)
	sb.WriteString(m.theme.DataRow.Render(sep))
	sb.WriteString("\n")

	bh := m.bodyHeight()
	start := m.scrollY
	end := start + bh
	if end > len(m.desc.ForeignKeys) {
		end = len(m.desc.ForeignKeys)
	}

	for i := start; i < end; i++ {
		fk := m.desc.ForeignKeys[i]

		refStr := fk.RefTable + "(" + strings.Join(fk.RefColumns, ", ") + ")"

		vals := []string{fk.Name, strings.Join(fk.Columns, ", "), refStr}
		widths := []int{nameW, colsW, refW}

		var cells []string
		for ci, w := range widths {
			style := m.theme.DataRow
			if i == m.cursorRow && ci == m.cursorCol {
				style = m.theme.DataCellActive
			} else if i == m.cursorRow {
				style = m.theme.DataSelected
			}
			cells = append(cells, style.Width(w).MaxWidth(w).Height(1).MaxHeight(1).Render(truncate(vals[ci], w)))
		}

		row := lipgloss.JoinHorizontal(lipgloss.Top, cells...)
		sb.WriteString(row)
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m StructureViewModel) renderDDLForm() string {
	var sb strings.Builder

	title := ""
	switch m.ddlMode {
	case ddlAddColumn:
		title = "Add Column"
	case ddlCreateIndex:
		title = "Create Index"
	case ddlCreateTable:
		if m.createTableName == "" {
			title = "Create Table"
		} else {
			title = fmt.Sprintf("Create Table: %s (Column %d)", m.createTableName, len(m.createTableColumns)+1)
		}
	}

	sb.WriteString(m.theme.DialogTitle.Render(" " + title + " "))
	sb.WriteString("\n\n")

	for i, f := range m.ddlFields {
		labelStyle := m.theme.DataRow
		if i == m.ddlFieldIdx {
			labelStyle = m.theme.DataSelected
		}

		label := f.label + ": "
		value := f.value
		if i == m.ddlFieldIdx {
			value += "▏"
		}

		sb.WriteString(labelStyle.Render(label))
		sb.WriteString(m.theme.SQLInput.Render(value))
		sb.WriteString("\n")
	}

	// Show already added columns for CREATE TABLE
	if m.ddlMode == ddlCreateTable && len(m.createTableColumns) > 0 {
		sb.WriteString("\n")
		sb.WriteString(m.theme.DataHeader.Render("Defined columns:"))
		sb.WriteString("\n")
		for _, col := range m.createTableColumns {
			nullStr := "NULL"
			if !col.nullable {
				nullStr = "NOT NULL"
			}
			def := ""
			if col.defVal != "" {
				def = " DEFAULT " + col.defVal
			}
			sb.WriteString(m.theme.DataRow.Render(
				fmt.Sprintf("  %s %s %s%s", col.name, col.dataType, nullStr, def)))
			sb.WriteString("\n")
		}
	}

	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpDesc.Render("Enter: Next/Confirm | Tab: Next field | Esc: Cancel"))

	return sb.String()
}

func (m StructureViewModel) renderDDLConfirm() string {
	var sb strings.Builder

	sb.WriteString(m.theme.DialogTitle.Render(" DDL Preview "))
	sb.WriteString("\n\n")

	// Show the DDL
	lines := strings.Split(m.ddlPreview, "\n")
	for _, line := range lines {
		sb.WriteString(m.theme.SQLKeyword.Render(line))
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpDesc.Render("Execute? (y)es / (n)o"))

	return sb.String()
}

func (m StructureViewModel) renderFooter() string {
	hints := ""
	switch m.activeTab {
	case TabColumns:
		hints = "a: Add column | d: Drop column | y: Yank cell | h/l: Move cell | Tab/1/2/3: Switch tab | Esc: Back"
	case TabIndexes:
		hints = "a: Create index | d: Drop index | y: Yank cell | h/l: Move cell | Tab/1/2/3: Switch tab | Esc: Back"
	case TabForeignKeys:
		hints = "y: Yank cell | h/l: Move cell | Tab/1/2/3: Switch tab | Esc: Back"
	}
	return m.theme.DataFooter.Render(hints)
}

func (m StructureViewModel) renderBorder(content string) string {
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