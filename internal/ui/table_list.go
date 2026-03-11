package ui

import (
	"context"
	"strconv"
	"strings"

	"github.com/mattn/go-runewidth"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minee/tdb/internal/db"
)

// ── Messages ──────────────────────────────────────────────────────

// TablesLoadedMsg is sent when table list loading completes.
type TablesLoadedMsg struct {
	Schemas []string
	Tables  map[string][]db.TableInfo
	Err     error
}

// TableSelectedMsg is sent when a user selects a table.
type TableSelectedMsg struct {
	Schema string
	Table  string
}

// DatabasesLoadedMsg is sent when database list loading completes.
type DatabasesLoadedMsg struct {
	Databases []string
	CurrentDB string
	Err       error
}

// SwitchDatabaseMsg requests switching to a different database.
type SwitchDatabaseMsg struct {
	Database string
}

// DatabaseSwitchedMsg is sent when database switching completes.
type DatabaseSwitchedMsg struct {
	Database string
	Err      error
}

// ── Commands ──────────────────────────────────────────────────────

// LoadTablesCmd loads schemas and their tables from the driver.
func LoadTablesCmd(driver db.Driver) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()

		schemas, err := driver.ListSchemas(ctx)
		if err != nil {
			return TablesLoadedMsg{Err: err}
		}

		tables := make(map[string][]db.TableInfo)
		for _, s := range schemas {
			tbl, err := driver.ListTables(ctx, s)
			if err != nil {
				return TablesLoadedMsg{Err: err}
			}
			tables[s] = tbl
		}

		return TablesLoadedMsg{
			Schemas: schemas,
			Tables:  tables,
		}
	}
}

// LoadDatabasesCmd loads the list of databases and the current database name.
func LoadDatabasesCmd(driver db.Driver) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()

		databases, err := driver.ListDatabases(ctx)
		if err != nil {
			return DatabasesLoadedMsg{Err: err}
		}

		currentDB, err := driver.CurrentDatabase(ctx)
		if err != nil {
			return DatabasesLoadedMsg{Err: err}
		}

		return DatabasesLoadedMsg{
			Databases: databases,
			CurrentDB: currentDB,
		}
	}
}

// SwitchDatabaseCmd executes the database switch on the driver.
func SwitchDatabaseCmd(driver db.Driver, dbName string) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		err := driver.SwitchDatabase(ctx, dbName)
		return DatabaseSwitchedMsg{
			Database: dbName,
			Err:      err,
		}
	}
}

// ── TableListModel ────────────────────────────────────────────────

// TableListModel manages the table list pane.
type TableListModel struct {
	schemas        []string
	tables         map[string][]db.TableInfo
	selectedSchema int
	selectedTable  int
	filterInput    string
	filtering      bool
	focused        bool
	width          int
	height         int
	theme          *Theme
	driver         db.Driver

	// Database switching state
	databases     []string // available databases
	selectedDB    int      // index of selected DB in databases
	currentDBName string   // current database name
	dbSwitchable  bool     // whether DB switching is supported (false for SQLite)
	dbSwitching   bool     // true while a database switch is in progress (prevents double execution)

	// DB select mode: shown when connected without specifying a database
	dbSelectMode bool // true = show database list instead of table list
	dbSelectIdx  int  // cursor position in the database list (during dbSelectMode)
}

// NewTableListModel creates a new TableListModel.
func NewTableListModel(driver db.Driver, theme *Theme) TableListModel {
	return TableListModel{
		tables: make(map[string][]db.TableInfo),
		theme:  theme,
		driver: driver,
	}
}

// SetFocused sets whether this pane is focused.
func (m *TableListModel) SetFocused(f bool) {
	m.focused = f
}

// SetSize updates the pane size.
func (m *TableListModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// filteredTables returns tables for the current schema that match the filter.
func (m TableListModel) filteredTables() []db.TableInfo {
	if len(m.schemas) == 0 {
		return nil
	}
	schema := m.schemas[m.selectedSchema]
	tables := m.tables[schema]

	if m.filterInput == "" {
		return tables
	}

	filter := strings.ToLower(m.filterInput)
	var filtered []db.TableInfo
	for _, t := range tables {
		if strings.Contains(strings.ToLower(t.Name), filter) {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

// filteredDatabases returns the databases that match the current filter (used in DB select mode).
func (m TableListModel) filteredDatabases() []string {
	if m.filterInput == "" {
		return m.databases
	}
	filter := strings.ToLower(m.filterInput)
	var filtered []string
	for _, d := range m.databases {
		if strings.Contains(strings.ToLower(d), filter) {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

// pageSize returns the number of visible table rows (used for page scroll).
func (m TableListModel) pageSize() int {
	headerLines := 1 // schema header
	if m.currentDBName != "" {
		headerLines += 2
	}
	h := m.height - 2 - headerLines - 1
	if h < 1 {
		h = 1
	}
	return h / 2
}

// SelectedTable returns the currently selected table info.
func (m TableListModel) SelectedTable() (schema, table string) {
	if len(m.schemas) == 0 {
		return "", ""
	}
	tables := m.filteredTables()
	if len(tables) == 0 {
		return m.schemas[m.selectedSchema], ""
	}
	idx := m.selectedTable
	if idx >= len(tables) {
		idx = len(tables) - 1
	}
	return tables[idx].Schema, tables[idx].Name
}

// Update handles key input for the table list.
func (m TableListModel) Update(msg tea.Msg) (TableListModel, tea.Cmd) {
	switch msg := msg.(type) {
	case TablesLoadedMsg:
		if msg.Err != nil {
			return m, nil
		}
		m.schemas = msg.Schemas
		m.tables = msg.Tables
		m.selectedSchema = 0
		m.selectedTable = 0
		return m, nil

	case DatabasesLoadedMsg:
		if msg.Err != nil {
			return m, nil
		}
		m.databases = msg.Databases
		m.currentDBName = msg.CurrentDB
		m.dbSwitchable = len(msg.Databases) > 0

		// Find the index of the current database
		m.selectedDB = 0
		for i, d := range m.databases {
			if d == msg.CurrentDB {
				m.selectedDB = i
				break
			}
		}

		// In DB select mode, reset cursor
		if m.dbSelectMode {
			m.dbSelectIdx = 0
		}
		return m, nil

	case tea.KeyMsg:
		if m.dbSelectMode {
			return m.handleDBSelectInput(msg)
		}
		if m.filtering {
			return m.handleFilterInput(msg)
		}
		return m.handleNormalInput(msg)
	}
	return m, nil
}

func (m TableListModel) handleFilterInput(msg tea.KeyMsg) (TableListModel, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.filtering = false
		m.filterInput = ""
		if m.dbSelectMode {
			m.dbSelectIdx = 0
		} else {
			m.selectedTable = 0
		}
		return m, nil
	case tea.KeyEnter:
		m.filtering = false
		if m.dbSelectMode {
			// In DB select mode, pressing Enter in filter confirms filter and stays
			return m, nil
		}
		// Keep filter applied; select first matching table
		return m, nil
	case tea.KeyBackspace:
		if len(m.filterInput) > 0 {
			m.filterInput = m.filterInput[:len(m.filterInput)-1]
			if m.dbSelectMode {
				m.dbSelectIdx = 0
			} else {
				m.selectedTable = 0
			}
		}
		return m, nil
	default:
		if msg.Type == tea.KeyRunes {
			m.filterInput += string(msg.Runes)
			if m.dbSelectMode {
				m.dbSelectIdx = 0
			} else {
				m.selectedTable = 0
			}
		}
		return m, nil
	}
}

// handleDBSelectInput handles key input in DB select mode.
func (m TableListModel) handleDBSelectInput(msg tea.KeyMsg) (TableListModel, tea.Cmd) {
	// Delegate to filter handler when actively filtering
	if m.filtering {
		return m.handleFilterInput(msg)
	}

	dbs := m.filteredDatabases()
	maxIdx := len(dbs) - 1
	if maxIdx < 0 {
		maxIdx = 0
	}

	switch msg.String() {
	case "j", "down":
		if m.dbSelectIdx < maxIdx {
			m.dbSelectIdx++
		}
	case "k", "up":
		if m.dbSelectIdx > 0 {
			m.dbSelectIdx--
		}
	case "g":
		m.dbSelectIdx = 0
	case "G":
		m.dbSelectIdx = maxIdx
	case "/":
		m.filtering = true
		m.filterInput = ""
	case "enter":
		if len(dbs) > 0 {
			idx := m.dbSelectIdx
			if idx >= len(dbs) {
				idx = len(dbs) - 1
			}
			targetDB := dbs[idx]
			m.dbSwitching = true
			return m, func() tea.Msg {
				return SwitchDatabaseMsg{Database: targetDB}
			}
		}
	}

	return m, nil
}

func (m TableListModel) handleNormalInput(msg tea.KeyMsg) (TableListModel, tea.Cmd) {
	tables := m.filteredTables()
	maxIdx := len(tables) - 1
	if maxIdx < 0 {
		maxIdx = 0
	}

	switch msg.String() {
	case "j", "down":
		if m.selectedTable < maxIdx {
			m.selectedTable++
		}
	case "k", "up":
		if m.selectedTable > 0 {
			m.selectedTable--
		}
	case "g":
		m.selectedTable = 0
	case "G":
		m.selectedTable = maxIdx
	case "ctrl+d":
		// Page down (half page)
		pageSize := m.pageSize()
		m.selectedTable += pageSize
		if m.selectedTable > maxIdx {
			m.selectedTable = maxIdx
		}
	case "ctrl+u":
		// Page up (half page)
		pageSize := m.pageSize()
		m.selectedTable -= pageSize
		if m.selectedTable < 0 {
			m.selectedTable = 0
		}
	case "/":
		m.filtering = true
		m.filterInput = ""
	case "enter":
		if len(tables) > 0 {
			idx := m.selectedTable
			if idx >= len(tables) {
				idx = len(tables) - 1
			}
			t := tables[idx]
			return m, func() tea.Msg {
				return TableSelectedMsg{Schema: t.Schema, Table: t.Name}
			}
		}
	case "[":
		// Switch to previous database
		if m.dbSwitchable && len(m.databases) > 1 && !m.dbSwitching {
			newIdx := m.selectedDB - 1
			if newIdx < 0 {
				newIdx = len(m.databases) - 1
			}
			targetDB := m.databases[newIdx]
			// Optimistic update of selectedDB and set switching flag
			m.selectedDB = newIdx
			m.dbSwitching = true
			return m, func() tea.Msg {
				return SwitchDatabaseMsg{Database: targetDB}
			}
		}
	case "]":
		// Switch to next database
		if m.dbSwitchable && len(m.databases) > 1 && !m.dbSwitching {
			newIdx := m.selectedDB + 1
			if newIdx >= len(m.databases) {
				newIdx = 0
			}
			targetDB := m.databases[newIdx]
			// Optimistic update of selectedDB and set switching flag
			m.selectedDB = newIdx
			m.dbSwitching = true
			return m, func() tea.Msg {
				return SwitchDatabaseMsg{Database: targetDB}
			}
		}
	}

	return m, nil
}

// truncateStr truncates s so its display width fits in maxWidth,
// appending "…" if truncated. Uses terminal display widths so that
// wide characters (CJK, emoji) are measured correctly.
func truncateStr(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	sw := runewidth.StringWidth(s)
	if sw <= maxWidth {
		return s
	}
	if maxWidth <= 1 {
		return "…"
	}
	return runewidth.Truncate(s, maxWidth-1, "") + "…"
}

// View renders the table list pane.
func (m TableListModel) View() string {
	if m.dbSelectMode {
		return m.viewDBSelect()
	}
	return m.viewTableList()
}

// viewDBSelect renders the database selection list.
func (m TableListModel) viewDBSelect() string {
	var sb strings.Builder

	const stylePad = 2
	innerW := m.width - 2 - stylePad
	if innerW < 6 {
		innerW = 6
	}

	// Header
	sb.WriteString(m.theme.TableListHeader.Width(innerW).MaxWidth(innerW).Render(truncateStr("Select Database", innerW)))
	sb.WriteString("\n")
	headerLines := 1

	// Filter input (above database list)
	filterLines := 0
	if m.filtering {
		searchLabel := "✨ " + m.filterInput + "▏"
		searchBox := m.theme.TableListSearchPopup.
			Width(innerW - 2).
			MaxWidth(innerW).
			Render(searchLabel)
		sb.WriteString(searchBox)
		sb.WriteString("\n")
		filterLines = 1
	} else if m.filterInput != "" {
		filterText := truncateStr("✨ "+m.filterInput, innerW)
		sb.WriteString(m.theme.TableListFilter.Width(innerW).MaxWidth(innerW).Render(filterText))
		sb.WriteString("\n")
		filterLines = 1
	}

	// Database list
	dbs := m.filteredDatabases()
	visibleHeight := m.height - 2 - headerLines - filterLines
	if len(dbs) > visibleHeight {
		visibleHeight-- // reserve 1 line for scroll indicator
	}
	if visibleHeight < 1 {
		visibleHeight = 1
	}

	// Determine scroll offset to keep dbSelectIdx visible
	scrollOffset := 0
	if len(dbs) > visibleHeight {
		if m.dbSelectIdx >= visibleHeight {
			scrollOffset = m.dbSelectIdx - visibleHeight + 1
		}
		if scrollOffset > len(dbs)-visibleHeight {
			scrollOffset = len(dbs) - visibleHeight
		}
	}

	// Prefix "▸ ▣ " has display width: ▸(1) + space(1) + ▣(1) + space(1) = 4
	prefixWidth := runewidth.StringWidth("▸ ▣ ")
	const rightMargin = 3
	nameMaxW := innerW - prefixWidth - rightMargin
	if nameMaxW < 4 {
		nameMaxW = 4
	}

	rendered := 0
	for i := scrollOffset; i < len(dbs) && rendered < visibleHeight; i++ {
		displayName := truncateStr(dbs[i], nameMaxW)

		if i == m.dbSelectIdx {
			line := "▸ ▣ " + displayName
			sb.WriteString(m.theme.TableListActive.Width(innerW).MaxWidth(innerW).Render(line))
		} else {
			line := "  ▣ " + displayName
			sb.WriteString(m.theme.TableListItem.Width(innerW).MaxWidth(innerW).Render(line))
		}
		sb.WriteString("\n")
		rendered++
	}

	if len(dbs) == 0 {
		sb.WriteString(m.theme.TableListItem.Width(innerW).Render("  (no databases)"))
		sb.WriteString("\n")
	}

	// Scroll indicator
	if len(dbs) > visibleHeight {
		indicator := truncateStr(
			"  ↕ "+strconv.Itoa(len(dbs))+" databases",
			innerW,
		)
		sb.WriteString(m.theme.TableListFilter.Width(innerW).Render(indicator))
		sb.WriteString("\n")
	}

	content := strings.TrimRight(sb.String(), "\n")

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

// viewTableList renders the normal table list view.
func (m TableListModel) viewTableList() string {
	var sb strings.Builder

	// stylePad accounts for the horizontal padding in table-list styles (Padding(0,1) = 2 chars).
	const stylePad = 2
	// Inner content width (total width minus border(2) minus style padding)
	innerW := m.width - 2 - stylePad
	if innerW < 6 {
		innerW = 6
	}

	// Database header (show current DB name with navigation indicators)
	headerLines := 0
	if m.currentDBName != "" {
		prefix := "▣ "
		suffix := ""
		if m.dbSwitchable && len(m.databases) > 1 {
			suffix = " ◄►"
		}
		nameW := innerW - runewidth.StringWidth(prefix) - runewidth.StringWidth(suffix)
		dbDisplay := prefix + truncateStr(m.currentDBName, nameW) + suffix
		sb.WriteString(m.theme.TableListHeader.Width(innerW).MaxWidth(innerW).Render(dbDisplay))
		sb.WriteString("\n")
		sep := strings.Repeat("─", innerW)
		sb.WriteString(m.theme.TableListItem.MaxWidth(innerW).MaxHeight(1).Render(sep))
		sb.WriteString("\n")
		headerLines = 2
	}

	// Schema header (only show if different from DB name)
	schemaName := "Tables"
	if len(m.schemas) > 0 {
		schemaName = m.schemas[m.selectedSchema]
	}
	if schemaName != m.currentDBName {
		sb.WriteString(m.theme.TableListHeader.Width(innerW).MaxWidth(innerW).Render(truncateStr(schemaName, innerW)))
		sb.WriteString("\n")
		headerLines++
	}

	// Filter input (above table list)
	filterLines := 0
	if m.filtering {
		searchLabel := "✨ " + m.filterInput + "▏"
		searchBox := m.theme.TableListSearchPopup.
			Width(innerW - 2).
			MaxWidth(innerW).
			Render(searchLabel)
		sb.WriteString(searchBox)
		sb.WriteString("\n")
		filterLines = 1
	} else if m.filterInput != "" {
		filterText := truncateStr("✨ "+m.filterInput, innerW)
		sb.WriteString(m.theme.TableListFilter.Width(innerW).MaxWidth(innerW).Render(filterText))
		sb.WriteString("\n")
		filterLines = 1
	}

	// Table list
	tables := m.filteredTables()
	// visible area = total height - borders(2) - headers - filter
	// Reserve 1 line for scroll indicator when there are more tables than fit
	visibleHeight := m.height - 2 - headerLines - filterLines
	needsScroll := len(tables) > visibleHeight
	if needsScroll {
		visibleHeight-- // reserve 1 line for scroll indicator
	}
	if visibleHeight < 1 {
		visibleHeight = 1
	}

	// Determine scroll offset to keep selectedTable visible
	scrollOffset := 0
	if len(tables) > visibleHeight {
		if m.selectedTable >= visibleHeight {
			scrollOffset = m.selectedTable - visibleHeight + 1
		}
		if scrollOffset > len(tables)-visibleHeight {
			scrollOffset = len(tables) - visibleHeight
		}
	}

	// Max display width for a table name.
	// Prefix "▸ ◦ " has display width: ▸(1) + space(1) + ◦(1) + space(1) = 4
	// Add extra margin (3) to keep text comfortably away from the border.
	prefixWidth := runewidth.StringWidth("▸ ◦ ")
	const rightMargin = 3
	nameMaxW := innerW - prefixWidth - rightMargin
	if nameMaxW < 4 {
		nameMaxW = 4
	}

	rendered := 0
	for i := scrollOffset; i < len(tables) && rendered < visibleHeight; i++ {
		t := tables[i]

		// Icon based on type
		icon := "≡ "
		if t.Type == "view" {
			icon = "• "
		}

		displayName := truncateStr(t.Name, nameMaxW)

		if i == m.selectedTable {
			line := "▸ " + icon + displayName
			sb.WriteString(m.theme.TableListActive.Width(innerW).MaxWidth(innerW).Render(line))
		} else {
			line := "  " + icon + displayName
			sb.WriteString(m.theme.TableListItem.Width(innerW).MaxWidth(innerW).Render(line))
		}
		sb.WriteString("\n")
		rendered++
	}

	if len(tables) == 0 && len(m.schemas) > 0 {
		sb.WriteString(m.theme.TableListItem.Width(innerW).Render("  (no tables)"))
		sb.WriteString("\n")
	}

	// Scroll indicator
	if len(tables) > visibleHeight {
		pos := scrollOffset * 100 / (len(tables) - visibleHeight)
		indicator := truncateStr(
			"  ↕ "+strconv.Itoa(len(tables))+" tables",
			innerW,
		)
		sb.WriteString(m.theme.TableListFilter.Width(innerW).Render(indicator))
		_ = pos // position is implicitly shown by selected row
		sb.WriteString("\n")
	}

	content := strings.TrimRight(sb.String(), "\n")

	// Apply border
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
