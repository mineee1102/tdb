package ui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/config"
	"github.com/minee/tdb/internal/db"
)

// ── Connection Screen Messages ────────────────────────────────────

// ConnectionEstablishedMsg is sent when a connection is successfully established.
type ConnectionEstablishedMsg struct {
	Driver     db.Driver
	Connection config.Connection
}

// ConnectionTestResultMsg is sent when a connection test completes.
type ConnectionTestResultMsg struct {
	Success bool
	Message string
}

// ── Connection Screen State ───────────────────────────────────────

// connScreenMode represents the sub-mode of the connection screen.
type connScreenMode int

const (
	connModeList connScreenMode = iota
	connModeForm
	connModeDeleteConfirm
)

// formField identifies the current form field.
type formField int

const (
	fieldName formField = iota
	fieldType
	fieldHost
	fieldPort
	fieldUser
	fieldPassword
	fieldDatabase
	fieldSSLMode
	fieldDSN
	fieldCount // sentinel for iteration
)

// DB type options for the type selector.
var dbTypeOptions = []string{"postgres", "mysql", "sqlite", "mssql"}

// DB type display names with icons.
var dbTypeIcons = map[string]string{
	"postgres": "🐘",
	"mysql":    "🐬",
	"sqlite":   "📁",
	"mssql":    "🔷",
}

// Default ports per DB type.
var defaultPorts = map[string]int{
	"postgres": 5432,
	"mysql":    3306,
	"sqlite":   0,
	"mssql":    1433,
}

// SSL mode options per DB type.
var sslModeOptions = map[string][]string{
	"postgres": {"disable", "require", "verify-ca", "verify-full"},
	"mysql":    {"false", "true", "skip-verify", "preferred"},
	"sqlite":   {},
	"mssql":    {"disable", "false", "true"},
}

// ── ConnectionFormModel ───────────────────────────────────────────

// ConnectionFormModel manages the connection management screen (list + form).
type ConnectionFormModel struct {
	config *config.Config

	// Screen mode
	mode connScreenMode

	// List state
	selectedIdx int
	listOffset  int

	// Form state
	editing       bool // true = editing existing, false = new
	editingIdx    int  // index in config.Connections when editing
	activeField   formField
	formValues    [fieldCount]string
	dsnMode       bool // true = direct DSN input mode
	typeSelectIdx int  // index in dbTypeOptions
	sslSelectIdx  int  // index in ssl options for current type

	// Connection test
	testing       bool
	testResult    string
	testIsSuccess bool

	// Delete confirmation
	deleteIdx int

	// Filter (search)
	filterInput string
	filtering   bool

	// Layout
	width  int
	height int
	theme  *Theme

	// Validation
	validationErr string
}

// NewConnectionFormModel creates a new connection form model.
func NewConnectionFormModel(cfg *config.Config, theme *Theme) ConnectionFormModel {
	return ConnectionFormModel{
		config: cfg,
		mode:   connModeList,
		theme:  theme,
	}
}

// SetSize updates the model dimensions.
func (m *ConnectionFormModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// ── Init ──────────────────────────────────────────────────────────

// Init returns the initial command for the connection screen.
func (m ConnectionFormModel) Init() tea.Cmd {
	return nil
}

// ── Update ────────────────────────────────────────────────────────

// Update handles messages for the connection screen.
func (m ConnectionFormModel) Update(msg tea.Msg) (ConnectionFormModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch m.mode {
		case connModeList:
			return m.updateList(msg)
		case connModeForm:
			return m.updateForm(msg)
		case connModeDeleteConfirm:
			return m.updateDeleteConfirm(msg)
		}

	case ConnectionTestResultMsg:
		m.testing = false
		m.testIsSuccess = msg.Success
		m.testResult = msg.Message
		return m, nil
	}

	return m, nil
}

// ── List Mode ─────────────────────────────────────────────────────

func (m ConnectionFormModel) updateList(msg tea.KeyMsg) (ConnectionFormModel, tea.Cmd) {
	// Filter input mode
	if m.filtering {
		return m.updateListFilter(msg)
	}

	filtered := m.filteredConnections()
	connCount := len(filtered)

	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit

	case "j", "down":
		if connCount > 0 && m.selectedIdx < connCount-1 {
			m.selectedIdx++
			m.ensureVisible()
		}

	case "k", "up":
		if m.selectedIdx > 0 {
			m.selectedIdx--
			m.ensureVisible()
		}

	case "g":
		m.selectedIdx = 0
		m.listOffset = 0

	case "G":
		if connCount > 0 {
			m.selectedIdx = connCount - 1
			m.ensureVisible()
		}

	case "enter":
		if connCount > 0 {
			realIdx := filtered[m.selectedIdx]
			return m, m.connectCmd(m.config.Connections[realIdx])
		}

	case "n":
		m.enterNewForm()

	case "e":
		if connCount > 0 {
			realIdx := filtered[m.selectedIdx]
			m.enterEditForm(realIdx)
		}

	case "d":
		if connCount > 0 {
			m.mode = connModeDeleteConfirm
			m.deleteIdx = filtered[m.selectedIdx]
		}

	case "t":
		if connCount > 0 {
			m.testing = true
			m.testResult = ""
			realIdx := filtered[m.selectedIdx]
			return m, m.testConnectionCmd(m.config.Connections[realIdx])
		}

	case "/":
		m.filtering = true
		m.filterInput = ""
	}

	return m, nil
}

func (m ConnectionFormModel) updateListFilter(msg tea.KeyMsg) (ConnectionFormModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.filtering = false
		m.filterInput = ""
		m.selectedIdx = 0
		m.listOffset = 0
	case "enter":
		m.filtering = false
		// keep filter applied
	case "backspace":
		if len(m.filterInput) > 0 {
			m.filterInput = m.filterInput[:len(m.filterInput)-1]
			m.selectedIdx = 0
			m.listOffset = 0
		}
	default:
		if len(msg.Runes) > 0 {
			m.filterInput += string(msg.Runes)
			m.selectedIdx = 0
			m.listOffset = 0
		}
	}
	return m, nil
}

// ensureVisible adjusts listOffset so that selectedIdx is visible.
func (m *ConnectionFormModel) ensureVisible() {
	visibleRows := m.listVisibleRows()
	if visibleRows <= 0 {
		visibleRows = 10
	}
	if m.selectedIdx < m.listOffset {
		m.listOffset = m.selectedIdx
	}
	if m.selectedIdx >= m.listOffset+visibleRows {
		m.listOffset = m.selectedIdx - visibleRows + 1
	}
}

func (m ConnectionFormModel) listVisibleRows() int {
	// Header (title + border) takes ~6 lines, footer ~4 lines
	return m.height - 12
}

// filteredConnections returns connections matching the current filter.
func (m ConnectionFormModel) filteredConnections() []int {
	conns := m.config.Connections
	if m.filterInput == "" {
		indices := make([]int, len(conns))
		for i := range conns {
			indices[i] = i
		}
		return indices
	}
	filter := strings.ToLower(m.filterInput)
	var indices []int
	for i, c := range conns {
		if strings.Contains(strings.ToLower(c.Name), filter) ||
			strings.Contains(strings.ToLower(c.Host), filter) ||
			strings.Contains(strings.ToLower(c.Type), filter) ||
			strings.Contains(strings.ToLower(c.Database), filter) {
			indices = append(indices, i)
		}
	}
	return indices
}

// ── Form Mode ─────────────────────────────────────────────────────

func (m *ConnectionFormModel) enterNewForm() {
	m.mode = connModeForm
	m.editing = false
	m.activeField = fieldName
	m.dsnMode = false
	m.typeSelectIdx = 0
	m.sslSelectIdx = 0
	m.validationErr = ""
	m.testResult = ""

	// Clear form values
	for i := range m.formValues {
		m.formValues[i] = ""
	}
	m.formValues[fieldType] = "postgres"
	m.formValues[fieldPort] = strconv.Itoa(defaultPorts["postgres"])
}

func (m *ConnectionFormModel) enterEditForm(idx int) {
	conn := m.config.Connections[idx]
	m.mode = connModeForm
	m.editing = true
	m.editingIdx = idx
	m.activeField = fieldName
	m.validationErr = ""
	m.testResult = ""

	m.formValues[fieldName] = conn.Name
	m.formValues[fieldType] = conn.Type
	m.formValues[fieldHost] = conn.Host
	m.formValues[fieldPort] = strconv.Itoa(conn.Port)
	m.formValues[fieldUser] = conn.User
	m.formValues[fieldPassword] = conn.Password
	m.formValues[fieldDatabase] = conn.Database
	m.formValues[fieldSSLMode] = conn.SSLMode
	m.formValues[fieldDSN] = conn.DSN

	m.dsnMode = conn.DSN != ""

	// Set type select index
	for i, t := range dbTypeOptions {
		if t == conn.Type {
			m.typeSelectIdx = i
			break
		}
	}

	// Set SSL select index
	if conn.SSLMode != "" {
		for i, s := range sslModeOptions[conn.Type] {
			if s == conn.SSLMode {
				m.sslSelectIdx = i
				break
			}
		}
	}
}

func (m ConnectionFormModel) updateForm(msg tea.KeyMsg) (ConnectionFormModel, tea.Cmd) {
	key := msg.String()

	switch key {
	case "esc":
		m.mode = connModeList
		m.validationErr = ""
		m.testResult = ""
		return m, nil

	case "ctrl+c":
		return m, tea.Quit

	case "tab", "down", "j":
		// Don't move with j if we're typing in a text field
		if key == "j" && m.isTextField(m.activeField) {
			m.appendToField(msg)
			return m, nil
		}
		m.nextField()
		return m, nil

	case "shift+tab", "up", "k":
		if key == "k" && m.isTextField(m.activeField) {
			m.appendToField(msg)
			return m, nil
		}
		m.prevField()
		return m, nil

	case "ctrl+d":
		// Toggle DSN mode
		m.dsnMode = !m.dsnMode
		if m.dsnMode {
			m.activeField = fieldDSN
		} else {
			m.activeField = fieldHost
		}
		return m, nil

	case "enter":
		// Save connection
		if err := m.validateForm(); err != "" {
			m.validationErr = err
			return m, nil
		}
		return m.saveConnection()

	case "ctrl+t":
		// Test connection from form
		conn := m.buildConnectionFromForm()
		m.testing = true
		m.testResult = ""
		return m, m.testConnectionCmd(conn)

	case "left", "h":
		if m.activeField == fieldType && !m.dsnMode {
			if m.typeSelectIdx > 0 {
				m.typeSelectIdx--
				m.formValues[fieldType] = dbTypeOptions[m.typeSelectIdx]
				m.formValues[fieldPort] = strconv.Itoa(defaultPorts[m.formValues[fieldType]])
				m.sslSelectIdx = 0
				if len(sslModeOptions[m.formValues[fieldType]]) > 0 {
					m.formValues[fieldSSLMode] = sslModeOptions[m.formValues[fieldType]][0]
				} else {
					m.formValues[fieldSSLMode] = ""
				}
			}
			return m, nil
		}
		if m.activeField == fieldSSLMode && !m.dsnMode {
			opts := sslModeOptions[m.formValues[fieldType]]
			if len(opts) > 0 && m.sslSelectIdx > 0 {
				m.sslSelectIdx--
				m.formValues[fieldSSLMode] = opts[m.sslSelectIdx]
			}
			return m, nil
		}
		// For text fields, h is just a character
		if m.isTextField(m.activeField) {
			m.appendToField(msg)
			return m, nil
		}

	case "right", "l":
		if m.activeField == fieldType && !m.dsnMode {
			if m.typeSelectIdx < len(dbTypeOptions)-1 {
				m.typeSelectIdx++
				m.formValues[fieldType] = dbTypeOptions[m.typeSelectIdx]
				m.formValues[fieldPort] = strconv.Itoa(defaultPorts[m.formValues[fieldType]])
				m.sslSelectIdx = 0
				if len(sslModeOptions[m.formValues[fieldType]]) > 0 {
					m.formValues[fieldSSLMode] = sslModeOptions[m.formValues[fieldType]][0]
				} else {
					m.formValues[fieldSSLMode] = ""
				}
			}
			return m, nil
		}
		if m.activeField == fieldSSLMode && !m.dsnMode {
			opts := sslModeOptions[m.formValues[fieldType]]
			if len(opts) > 0 && m.sslSelectIdx < len(opts)-1 {
				m.sslSelectIdx++
				m.formValues[fieldSSLMode] = opts[m.sslSelectIdx]
			}
			return m, nil
		}
		if m.isTextField(m.activeField) {
			m.appendToField(msg)
			return m, nil
		}

	case "backspace":
		m.backspaceField()
		return m, nil

	default:
		// Type into the current text field
		if m.isTextField(m.activeField) {
			m.appendToField(msg)
		}
	}

	return m, nil
}

// isTextField returns whether the given field accepts free text input.
func (m ConnectionFormModel) isTextField(f formField) bool {
	if m.dsnMode {
		return f == fieldName || f == fieldDSN
	}
	switch f {
	case fieldName, fieldHost, fieldPort, fieldUser, fieldPassword, fieldDatabase:
		return true
	}
	return false
}

func (m *ConnectionFormModel) nextField() {
	if m.dsnMode {
		// In DSN mode: Name -> Type -> DSN
		switch m.activeField {
		case fieldName:
			m.activeField = fieldType
		case fieldType:
			m.activeField = fieldDSN
		default:
			m.activeField = fieldName
		}
		return
	}
	// Normal mode: skip DSN field, skip SSLMode for sqlite
	for {
		m.activeField++
		if m.activeField >= fieldCount {
			m.activeField = fieldName
		}
		if m.activeField == fieldDSN {
			continue
		}
		if m.activeField == fieldSSLMode && m.formValues[fieldType] == "sqlite" {
			continue
		}
		// Skip host/port/user/password for sqlite
		if m.formValues[fieldType] == "sqlite" {
			if m.activeField == fieldHost || m.activeField == fieldPort ||
				m.activeField == fieldUser || m.activeField == fieldPassword ||
				m.activeField == fieldSSLMode {
				continue
			}
		}
		break
	}
}

func (m *ConnectionFormModel) prevField() {
	if m.dsnMode {
		switch m.activeField {
		case fieldDSN:
			m.activeField = fieldType
		case fieldType:
			m.activeField = fieldName
		default:
			m.activeField = fieldDSN
		}
		return
	}
	for {
		if m.activeField == fieldName {
			m.activeField = fieldCount - 1
		} else {
			m.activeField--
		}
		if m.activeField == fieldDSN {
			continue
		}
		if m.activeField == fieldSSLMode && m.formValues[fieldType] == "sqlite" {
			continue
		}
		if m.formValues[fieldType] == "sqlite" {
			if m.activeField == fieldHost || m.activeField == fieldPort ||
				m.activeField == fieldUser || m.activeField == fieldPassword ||
				m.activeField == fieldSSLMode {
				continue
			}
		}
		break
	}
}

func (m *ConnectionFormModel) appendToField(msg tea.KeyMsg) {
	var s string
	if msg.Type == tea.KeyRunes {
		// Handles both single character input and paste (multiple runes)
		s = string(msg.Runes)
	} else if msg.Type == tea.KeySpace {
		s = " "
	} else {
		return
	}

	switch m.activeField {
	case fieldName:
		m.formValues[fieldName] += s
	case fieldHost:
		m.formValues[fieldHost] += s
	case fieldPort:
		// Only allow digits; filter when pasting
		for _, r := range s {
			if r >= '0' && r <= '9' {
				m.formValues[fieldPort] += string(r)
			}
		}
	case fieldUser:
		m.formValues[fieldUser] += s
	case fieldPassword:
		m.formValues[fieldPassword] += s
	case fieldDatabase:
		m.formValues[fieldDatabase] += s
	case fieldDSN:
		m.formValues[fieldDSN] += s
	}
}

func (m *ConnectionFormModel) backspaceField() {
	var val *string

	switch m.activeField {
	case fieldName:
		val = &m.formValues[fieldName]
	case fieldHost:
		val = &m.formValues[fieldHost]
	case fieldPort:
		val = &m.formValues[fieldPort]
	case fieldUser:
		val = &m.formValues[fieldUser]
	case fieldPassword:
		val = &m.formValues[fieldPassword]
	case fieldDatabase:
		val = &m.formValues[fieldDatabase]
	case fieldDSN:
		val = &m.formValues[fieldDSN]
	default:
		return
	}

	if len(*val) > 0 {
		*val = (*val)[:len(*val)-1]
	}
}

func (m ConnectionFormModel) validateForm() string {
	name := strings.TrimSpace(m.formValues[fieldName])
	if name == "" {
		return "Connection name is required"
	}

	// Check for duplicate name (unless editing with same name)
	for i, conn := range m.config.Connections {
		if conn.Name == name {
			if m.editing && i == m.editingIdx {
				continue
			}
			return fmt.Sprintf("Connection name %q already exists", name)
		}
	}

	if m.dsnMode {
		if strings.TrimSpace(m.formValues[fieldDSN]) == "" {
			return "DSN is required in DSN mode"
		}
		return ""
	}

	dbType := m.formValues[fieldType]
	if dbType == "sqlite" {
		if strings.TrimSpace(m.formValues[fieldDatabase]) == "" {
			return "Database file path is required for SQLite"
		}
		return ""
	}

	if strings.TrimSpace(m.formValues[fieldHost]) == "" {
		return "Host is required"
	}
	if strings.TrimSpace(m.formValues[fieldPort]) == "" {
		return "Port is required"
	}

	return ""
}

func (m ConnectionFormModel) buildConnectionFromForm() config.Connection {
	port, _ := strconv.Atoi(m.formValues[fieldPort])

	conn := config.Connection{
		Name:     strings.TrimSpace(m.formValues[fieldName]),
		Type:     m.formValues[fieldType],
		Host:     strings.TrimSpace(m.formValues[fieldHost]),
		Port:     port,
		User:     strings.TrimSpace(m.formValues[fieldUser]),
		Password: m.formValues[fieldPassword],
		Database: strings.TrimSpace(m.formValues[fieldDatabase]),
		SSLMode:  m.formValues[fieldSSLMode],
	}

	if m.dsnMode {
		conn.DSN = strings.TrimSpace(m.formValues[fieldDSN])
		conn.Host = ""
		conn.Port = 0
		conn.User = ""
		conn.Password = ""
		conn.Database = ""
		conn.SSLMode = ""
	}

	return conn
}

func (m ConnectionFormModel) saveConnection() (ConnectionFormModel, tea.Cmd) {
	conn := m.buildConnectionFromForm()

	if m.editing {
		// Update existing connection
		m.config.Connections[m.editingIdx] = conn
	} else {
		// Add new connection
		m.config.Connections = append(m.config.Connections, conn)
		m.selectedIdx = len(m.config.Connections) - 1
	}

	// Save to disk
	if err := m.config.Save(); err != nil {
		m.validationErr = "Failed to save: " + err.Error()
		return m, nil
	}

	m.mode = connModeList
	m.validationErr = ""
	m.testResult = ""
	return m, nil
}

// ── Delete Confirm Mode ───────────────────────────────────────────

func (m ConnectionFormModel) updateDeleteConfirm(msg tea.KeyMsg) (ConnectionFormModel, tea.Cmd) {
	switch msg.String() {
	case "y", "Y", "enter":
		if m.deleteIdx >= 0 && m.deleteIdx < len(m.config.Connections) {
			name := m.config.Connections[m.deleteIdx].Name
			_ = m.config.RemoveConnection(name)
			_ = m.config.Save()

			// Adjust selection
			if m.selectedIdx >= len(m.config.Connections) && m.selectedIdx > 0 {
				m.selectedIdx--
			}
		}
		m.mode = connModeList
		return m, nil

	case "n", "N", "esc", "q":
		m.mode = connModeList
		return m, nil
	}
	return m, nil
}

// ── Commands ──────────────────────────────────────────────────────

func (m ConnectionFormModel) connectCmd(conn config.Connection) tea.Cmd {
	return func() tea.Msg {
		driverType := db.DriverType(conn.Type)
		driver, err := db.NewDriver(driverType)
		if err != nil {
			return ConnectionTestResultMsg{
				Success: false,
				Message: fmt.Sprintf("Driver error: %v", err),
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dsn := conn.BuildDSN()
		if err := driver.Connect(ctx, dsn); err != nil {
			return ConnectionTestResultMsg{
				Success: false,
				Message: fmt.Sprintf("Connection failed: %v", err),
			}
		}

		return ConnectionEstablishedMsg{
			Driver:     driver,
			Connection: conn,
		}
	}
}

func (m ConnectionFormModel) testConnectionCmd(conn config.Connection) tea.Cmd {
	return func() tea.Msg {
		driverType := db.DriverType(conn.Type)
		driver, err := db.NewDriver(driverType)
		if err != nil {
			return ConnectionTestResultMsg{
				Success: false,
				Message: fmt.Sprintf("Driver error: %v", err),
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dsn := conn.BuildDSN()
		if err := driver.Connect(ctx, dsn); err != nil {
			return ConnectionTestResultMsg{
				Success: false,
				Message: fmt.Sprintf("Connection failed: %v", err),
			}
		}
		driver.Close()

		return ConnectionTestResultMsg{
			Success: true,
			Message: "Connection successful!",
		}
	}
}

// ── View ──────────────────────────────────────────────────────────

// View renders the connection screen.
func (m ConnectionFormModel) View() string {
	if m.width == 0 || m.height == 0 {
		return "Initializing..."
	}

	switch m.mode {
	case connModeList:
		return m.viewList()
	case connModeForm:
		return m.viewForm()
	case connModeDeleteConfirm:
		return m.viewDeleteConfirm()
	}
	return ""
}

// ── List View ─────────────────────────────────────────────────────

func (m ConnectionFormModel) viewList() string {
	var sb strings.Builder

	// Title
	titleStyle := m.theme.TitleBar.Width(m.width - 4)
	sb.WriteString(titleStyle.Render(" 🗄  tdb - Connection Manager "))
	sb.WriteString("\n\n")

	// Filter input (above connection list)
	if m.filtering {
		searchLabel := "✨ " + m.filterInput + "▏"
		searchBox := m.theme.TableListSearchPopup.
			Width(m.width - 10).
			MaxWidth(m.width - 6).
			Render(searchLabel)
		sb.WriteString("  " + searchBox)
		sb.WriteString("\n")
	} else if m.filterInput != "" {
		filterText := "✨ " + m.filterInput
		sb.WriteString("  " + m.theme.TableListFilter.Render(filterText))
		sb.WriteString("\n")
	}

	filtered := m.filteredConnections()

	if len(m.config.Connections) == 0 {
		emptyStyle := lipgloss.NewStyle().
			Foreground(m.theme.ColorFgDim).
			Italic(true).
			Padding(1, 2)
		sb.WriteString(emptyStyle.Render("No saved connections.\nPress 'n' to add a new connection."))
		sb.WriteString("\n")
	} else if len(filtered) == 0 {
		emptyStyle := lipgloss.NewStyle().
			Foreground(m.theme.ColorFgDim).
			Italic(true).
			Padding(1, 2)
		sb.WriteString(emptyStyle.Render("No matching connections."))
		sb.WriteString("\n")
	} else {
		// Connection list
		visibleRows := m.listVisibleRows()
		if visibleRows < 1 {
			visibleRows = 1
		}

		start := m.listOffset
		end := start + visibleRows
		if end > len(filtered) {
			end = len(filtered)
		}

		for i := start; i < end; i++ {
			conn := m.config.Connections[filtered[i]]
			icon := dbTypeIcons[conn.Type]
			if icon == "" {
				icon = "🔌"
			}

			// Build display line
			hostInfo := conn.Host
			if conn.Port > 0 {
				hostInfo = fmt.Sprintf("%s:%d", conn.Host, conn.Port)
			}
			if conn.Type == "sqlite" {
				hostInfo = conn.Database
			}
			if conn.DSN != "" && hostInfo == "" {
				dsn := conn.DSN
				if len(dsn) > 40 {
					dsn = dsn[:37] + "..."
				}
				hostInfo = dsn
			}

			line := fmt.Sprintf(" %s  %-20s  %s", icon, conn.Name, hostInfo)

			if i == m.selectedIdx {
				sb.WriteString(m.theme.TableListActive.Width(m.width - 6).Render(line))
			} else {
				sb.WriteString(m.theme.TableListItem.Width(m.width - 6).Render(line))
			}
			sb.WriteString("\n")
		}

		// Scroll indicator
		if len(filtered) > visibleRows {
			indicator := fmt.Sprintf("  %d/%d connections", m.selectedIdx+1, len(filtered))
			sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).Render(indicator))
			sb.WriteString("\n")
		}
	}

	// Test result
	if m.testResult != "" {
		sb.WriteString("\n")
		if m.testIsSuccess {
			sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorSuccess).Bold(true).
				Render("  ✓ " + m.testResult))
		} else {
			sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorError).Bold(true).
				Render("  ✗ " + m.testResult))
		}
		sb.WriteString("\n")
	}

	if m.testing {
		sb.WriteString("\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorSecondary).
			Render("  ⟳ Testing connection..."))
		sb.WriteString("\n")
	}

	// Footer / keybindings
	sb.WriteString("\n")
	footerStyle := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim)
	keys := []struct{ key, desc string }{
		{"Enter", "Connect"},
		{"n", "New"},
		{"e", "Edit"},
		{"d", "Delete"},
		{"t", "Test"},
		{"/", "Search"},
		{"q", "Quit"},
	}

	var keyParts []string
	keyStyle := lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorSecondary)
	for _, k := range keys {
		keyParts = append(keyParts, keyStyle.Render(k.key)+footerStyle.Render(":"+k.desc))
	}
	sb.WriteString("  " + strings.Join(keyParts, "  "))

	content := sb.String()

	// Place in a bordered container
	container := m.theme.FocusedBorder.
		Width(m.width - 2).
		Height(m.height - 2).
		Render(content)

	return container
}

// ── Form View ─────────────────────────────────────────────────────

func (m ConnectionFormModel) viewForm() string {
	var sb strings.Builder

	// Title
	title := " New Connection "
	if m.editing {
		title = " Edit Connection "
	}
	titleStyle := m.theme.TitleBar.Width(m.width - 8)
	sb.WriteString(titleStyle.Render(title))
	sb.WriteString("\n\n")

	// Mode indicator
	modeLabel := "Field Mode"
	if m.dsnMode {
		modeLabel = "DSN Mode"
	}
	modeStyle := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).Italic(true)
	sb.WriteString(modeStyle.Render("  Mode: "+modeLabel+" (Ctrl+D to toggle)"))
	sb.WriteString("\n\n")

	// Fields
	labelWidth := 16

	// Connection Name (always shown)
	sb.WriteString(m.renderFormField("Connection Name", m.formValues[fieldName], fieldName, labelWidth, false))
	sb.WriteString("\n")

	// DB Type (always shown)
	sb.WriteString(m.renderTypeSelector(labelWidth))
	sb.WriteString("\n")

	if m.dsnMode {
		// DSN input
		sb.WriteString(m.renderFormField("DSN", m.formValues[fieldDSN], fieldDSN, labelWidth, false))
		sb.WriteString("\n")
	} else {
		dbType := m.formValues[fieldType]

		if dbType != "sqlite" {
			// Host
			sb.WriteString(m.renderFormField("Host", m.formValues[fieldHost], fieldHost, labelWidth, false))
			sb.WriteString("\n")

			// Port
			sb.WriteString(m.renderFormField("Port", m.formValues[fieldPort], fieldPort, labelWidth, false))
			sb.WriteString("\n")

			// User
			sb.WriteString(m.renderFormField("User", m.formValues[fieldUser], fieldUser, labelWidth, false))
			sb.WriteString("\n")

			// Password (masked)
			sb.WriteString(m.renderFormField("Password", m.formValues[fieldPassword], fieldPassword, labelWidth, true))
			sb.WriteString("\n")
		}

		// Database
		dbLabel := "Database"
		if dbType == "sqlite" {
			dbLabel = "File Path"
		} else {
			dbLabel = "Database (opt)"
		}
		sb.WriteString(m.renderFormField(dbLabel, m.formValues[fieldDatabase], fieldDatabase, labelWidth, false))
		sb.WriteString("\n")

		// SSL Mode (not for sqlite)
		if dbType != "sqlite" {
			sb.WriteString(m.renderSSLSelector(labelWidth))
			sb.WriteString("\n")
		}
	}

	// Validation error
	if m.validationErr != "" {
		sb.WriteString("\n")
		errStyle := lipgloss.NewStyle().Foreground(m.theme.ColorError).Bold(true)
		sb.WriteString(errStyle.Render("  ⚠ " + m.validationErr))
		sb.WriteString("\n")
	}

	// Test result
	if m.testResult != "" {
		sb.WriteString("\n")
		if m.testIsSuccess {
			sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorSuccess).Bold(true).
				Render("  ✓ " + m.testResult))
		} else {
			sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorError).Bold(true).
				Render("  ✗ " + m.testResult))
		}
		sb.WriteString("\n")
	}

	if m.testing {
		sb.WriteString("\n")
		sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorSecondary).
			Render("  ⟳ Testing connection..."))
		sb.WriteString("\n")
	}

	// Footer
	sb.WriteString("\n")
	footerStyle := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim)
	keyStyle := lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorSecondary)

	keyParts := []string{
		keyStyle.Render("Enter") + footerStyle.Render(":Save"),
		keyStyle.Render("Esc") + footerStyle.Render(":Cancel"),
		keyStyle.Render("Tab") + footerStyle.Render(":Next"),
		keyStyle.Render("Ctrl+D") + footerStyle.Render(":DSN Mode"),
		keyStyle.Render("Ctrl+T") + footerStyle.Render(":Test"),
	}
	sb.WriteString("  " + strings.Join(keyParts, "  "))

	content := sb.String()

	container := m.theme.FocusedBorder.
		Width(m.width - 2).
		Height(m.height - 2).
		Render(content)

	return container
}

func (m ConnectionFormModel) renderFormField(label, value string, field formField, labelWidth int, masked bool) string {
	isActive := m.activeField == field

	// Label
	labelStyle := lipgloss.NewStyle().Width(labelWidth).Foreground(m.theme.ColorFgDim)
	if isActive {
		labelStyle = labelStyle.Foreground(m.theme.ColorSecondary).Bold(true)
	}

	// Value
	displayValue := value
	if masked && !isActive {
		displayValue = strings.Repeat("•", len(value))
	} else if masked && isActive {
		// Show last char, mask the rest
		if len(value) > 1 {
			displayValue = strings.Repeat("•", len(value)-1) + value[len(value)-1:]
		}
	}

	valueStyle := lipgloss.NewStyle().Foreground(m.theme.ColorFg)

	indicator := "  "
	if isActive {
		indicator = "▸ "
		// Add cursor
		displayValue += "█"
		valueStyle = valueStyle.Foreground(m.theme.ColorFg).Bold(true)
	}

	return indicator + labelStyle.Render(label+":") + " " + valueStyle.Render(displayValue)
}

func (m ConnectionFormModel) renderTypeSelector(labelWidth int) string {
	isActive := m.activeField == fieldType

	labelStyle := lipgloss.NewStyle().Width(labelWidth).Foreground(m.theme.ColorFgDim)
	if isActive {
		labelStyle = labelStyle.Foreground(m.theme.ColorSecondary).Bold(true)
	}

	indicator := "  "
	if isActive {
		indicator = "▸ "
	}

	var options []string
	for i, t := range dbTypeOptions {
		icon := dbTypeIcons[t]
		opt := icon + " " + t
		if i == m.typeSelectIdx {
			if isActive {
				opt = lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorFg).
					Background(m.theme.ColorPrimary).Padding(0, 1).Render(opt)
			} else {
				opt = lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorFg).
					Padding(0, 1).Render(opt)
			}
		} else {
			opt = lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).Padding(0, 1).Render(opt)
		}
		options = append(options, opt)
	}

	return indicator + labelStyle.Render("DB Type:") + " " + strings.Join(options, " ")
}

func (m ConnectionFormModel) renderSSLSelector(labelWidth int) string {
	isActive := m.activeField == fieldSSLMode
	dbType := m.formValues[fieldType]
	opts := sslModeOptions[dbType]

	if len(opts) == 0 {
		return ""
	}

	labelStyle := lipgloss.NewStyle().Width(labelWidth).Foreground(m.theme.ColorFgDim)
	if isActive {
		labelStyle = labelStyle.Foreground(m.theme.ColorSecondary).Bold(true)
	}

	indicator := "  "
	if isActive {
		indicator = "▸ "
	}

	var options []string
	for i, s := range opts {
		if i == m.sslSelectIdx {
			if isActive {
				s = lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorFg).
					Background(m.theme.ColorPrimary).Padding(0, 1).Render(s)
			} else {
				s = lipgloss.NewStyle().Bold(true).Foreground(m.theme.ColorFg).
					Padding(0, 1).Render(s)
			}
		} else {
			s = lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).Padding(0, 1).Render(s)
		}
		options = append(options, s)
	}

	return indicator + labelStyle.Render("SSL Mode:") + " " + strings.Join(options, " ")
}

// ── Delete Confirm View ───────────────────────────────────────────

func (m ConnectionFormModel) viewDeleteConfirm() string {
	connName := ""
	if m.deleteIdx >= 0 && m.deleteIdx < len(m.config.Connections) {
		connName = m.config.Connections[m.deleteIdx].Name
	}

	var sb strings.Builder
	sb.WriteString(m.theme.DialogTitle.Render(" Delete Connection "))
	sb.WriteString("\n\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(m.theme.ColorFg).Render(
		fmt.Sprintf("  Are you sure you want to delete %q?", connName)))
	sb.WriteString("\n\n")

	yesBtn := m.theme.DialogButtonActive.Render(" Yes (y) ")
	noBtn := m.theme.DialogButton.Render(" No (n) ")
	sb.WriteString("  " + lipgloss.JoinHorizontal(lipgloss.Top, yesBtn, "  ", noBtn))

	content := m.theme.DialogBorder.Render(sb.String())

	return lipgloss.Place(
		m.width, m.height,
		lipgloss.Center, lipgloss.Center,
		content,
	)
}
