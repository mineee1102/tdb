package ui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minee/tdb/internal/config"
)

// ── ConnectionFormModel tests ─────────────────────────────────────

func newTestConfig(connections ...config.Connection) *config.Config {
	return &config.Config{
		Connections: connections,
	}
}

func TestConnectionFormModelNew(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	if m.mode != connModeList {
		t.Error("Expected initial mode to be connModeList")
	}
	if m.selectedIdx != 0 {
		t.Error("Expected initial selectedIdx to be 0")
	}
}

func TestConnectionFormListNavigation(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "dev-pg", Type: "postgres", Host: "localhost", Port: 5432},
		config.Connection{Name: "dev-mysql", Type: "mysql", Host: "localhost", Port: 3306},
		config.Connection{Name: "test-db", Type: "sqlite", Database: "./test.db"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Move down
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.selectedIdx != 1 {
		t.Errorf("Expected selectedIdx=1 after j, got %d", m.selectedIdx)
	}

	// Move down again
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.selectedIdx != 2 {
		t.Errorf("Expected selectedIdx=2 after j, got %d", m.selectedIdx)
	}

	// Should not go past end
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.selectedIdx != 2 {
		t.Errorf("Expected selectedIdx=2 (clamped), got %d", m.selectedIdx)
	}

	// Move up
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("k")})
	if m.selectedIdx != 1 {
		t.Errorf("Expected selectedIdx=1 after k, got %d", m.selectedIdx)
	}

	// Jump to start
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	if m.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx=0 after g, got %d", m.selectedIdx)
	}

	// Jump to end
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("G")})
	if m.selectedIdx != 2 {
		t.Errorf("Expected selectedIdx=2 after G, got %d", m.selectedIdx)
	}
}

func TestConnectionFormNewConnection(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Press 'n' to enter new form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	if m.mode != connModeForm {
		t.Error("Expected mode to be connModeForm after 'n'")
	}
	if m.editing {
		t.Error("Expected editing=false for new connection")
	}
	if m.activeField != fieldName {
		t.Error("Expected activeField to be fieldName")
	}

	// Default values
	if m.formValues[fieldType] != "postgres" {
		t.Errorf("Expected default type=postgres, got %s", m.formValues[fieldType])
	}
	if m.formValues[fieldPort] != "5432" {
		t.Errorf("Expected default port=5432, got %s", m.formValues[fieldPort])
	}
}

func TestConnectionFormFieldNavigation(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})

	// Tab should move to next field
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.activeField != fieldType {
		t.Errorf("Expected activeField=fieldType after Tab, got %d", m.activeField)
	}

	// Tab again should skip to host (since type is selector)
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.activeField != fieldHost {
		t.Errorf("Expected activeField=fieldHost after Tab, got %d", m.activeField)
	}
}

func TestConnectionFormTypeSelector(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form and navigate to type field
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab}) // Go to type

	// Press right to change type
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	if m.formValues[fieldType] != "mysql" {
		t.Errorf("Expected type=mysql after right, got %s", m.formValues[fieldType])
	}
	if m.formValues[fieldPort] != "3306" {
		t.Errorf("Expected port=3306 for mysql, got %s", m.formValues[fieldPort])
	}

	// Press right again for sqlite
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	if m.formValues[fieldType] != "sqlite" {
		t.Errorf("Expected type=sqlite after right, got %s", m.formValues[fieldType])
	}

	// Press left back to mysql
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("h")})
	if m.formValues[fieldType] != "mysql" {
		t.Errorf("Expected type=mysql after left, got %s", m.formValues[fieldType])
	}
}

func TestConnectionFormTextInput(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})

	// Type connection name
	for _, r := range "my-db" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	if m.formValues[fieldName] != "my-db" {
		t.Errorf("Expected name='my-db', got '%s'", m.formValues[fieldName])
	}

	// Backspace
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	if m.formValues[fieldName] != "my-d" {
		t.Errorf("Expected name='my-d' after backspace, got '%s'", m.formValues[fieldName])
	}
}

func TestConnectionFormValidation(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})

	// Try to save without name
	result := m.validateForm()
	if result == "" {
		t.Error("Expected validation error for empty name")
	}
	if !strings.Contains(result, "name") {
		t.Errorf("Expected error about name, got: %s", result)
	}
}

func TestConnectionFormValidationDuplicateName(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "existing", Type: "postgres"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form and type duplicate name
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	for _, r := range "existing" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	result := m.validateForm()
	if result == "" {
		t.Error("Expected validation error for duplicate name")
	}
	if !strings.Contains(result, "already exists") {
		t.Errorf("Expected duplicate name error, got: %s", result)
	}
}

func TestConnectionFormEscCancels(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	if m.mode != connModeForm {
		t.Error("Expected connModeForm")
	}

	// Press Esc
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if m.mode != connModeList {
		t.Error("Expected connModeList after Esc")
	}
}

func TestConnectionFormEditExisting(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{
			Name:     "dev-pg",
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			User:     "admin",
			Password: "secret",
			Database: "mydb",
			SSLMode:  "disable",
		},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Press 'e' to edit
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	if m.mode != connModeForm {
		t.Error("Expected connModeForm after 'e'")
	}
	if !m.editing {
		t.Error("Expected editing=true")
	}
	if m.formValues[fieldName] != "dev-pg" {
		t.Errorf("Expected name='dev-pg', got '%s'", m.formValues[fieldName])
	}
	if m.formValues[fieldHost] != "localhost" {
		t.Errorf("Expected host='localhost', got '%s'", m.formValues[fieldHost])
	}
	if m.formValues[fieldPort] != "5432" {
		t.Errorf("Expected port='5432', got '%s'", m.formValues[fieldPort])
	}
	if m.formValues[fieldUser] != "admin" {
		t.Errorf("Expected user='admin', got '%s'", m.formValues[fieldUser])
	}
	if m.formValues[fieldPassword] != "secret" {
		t.Errorf("Expected password='secret', got '%s'", m.formValues[fieldPassword])
	}
	if m.formValues[fieldDatabase] != "mydb" {
		t.Errorf("Expected database='mydb', got '%s'", m.formValues[fieldDatabase])
	}
}

func TestConnectionFormDeleteConfirm(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "to-delete", Type: "postgres"},
		config.Connection{Name: "keep-this", Type: "mysql"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Press 'd' to delete
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if m.mode != connModeDeleteConfirm {
		t.Error("Expected connModeDeleteConfirm after 'd'")
	}

	// Cancel with 'n'
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	if m.mode != connModeList {
		t.Error("Expected connModeList after cancel")
	}
	if len(m.config.Connections) != 2 {
		t.Errorf("Expected 2 connections after cancel, got %d", len(m.config.Connections))
	}
}

func TestConnectionFormDeleteConfirmYes(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "to-delete", Type: "postgres"},
		config.Connection{Name: "keep-this", Type: "mysql"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Press 'd' to delete
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})

	// Confirm with 'y'
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	if m.mode != connModeList {
		t.Error("Expected connModeList after confirm")
	}
	if len(m.config.Connections) != 1 {
		t.Errorf("Expected 1 connection after delete, got %d", len(m.config.Connections))
	}
	if m.config.Connections[0].Name != "keep-this" {
		t.Errorf("Expected remaining connection to be 'keep-this', got '%s'", m.config.Connections[0].Name)
	}
}

func TestConnectionFormDSNMode(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})

	// Toggle DSN mode
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlD})
	if !m.dsnMode {
		t.Error("Expected dsnMode=true after Ctrl+D")
	}
	if m.activeField != fieldDSN {
		t.Errorf("Expected activeField=fieldDSN, got %d", m.activeField)
	}

	// Toggle back
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlD})
	if m.dsnMode {
		t.Error("Expected dsnMode=false after second Ctrl+D")
	}
}

func TestConnectionFormBuildConnection(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	m.formValues[fieldName] = "test-conn"
	m.formValues[fieldType] = "postgres"
	m.formValues[fieldHost] = "db.example.com"
	m.formValues[fieldPort] = "5432"
	m.formValues[fieldUser] = "admin"
	m.formValues[fieldPassword] = "pass123"
	m.formValues[fieldDatabase] = "mydb"
	m.formValues[fieldSSLMode] = "require"

	conn := m.buildConnectionFromForm()

	if conn.Name != "test-conn" {
		t.Errorf("Expected name='test-conn', got '%s'", conn.Name)
	}
	if conn.Type != "postgres" {
		t.Errorf("Expected type='postgres', got '%s'", conn.Type)
	}
	if conn.Host != "db.example.com" {
		t.Errorf("Expected host='db.example.com', got '%s'", conn.Host)
	}
	if conn.Port != 5432 {
		t.Errorf("Expected port=5432, got %d", conn.Port)
	}
	if conn.User != "admin" {
		t.Errorf("Expected user='admin', got '%s'", conn.User)
	}
	if conn.Password != "pass123" {
		t.Errorf("Expected password='pass123', got '%s'", conn.Password)
	}
	if conn.Database != "mydb" {
		t.Errorf("Expected database='mydb', got '%s'", conn.Database)
	}
	if conn.SSLMode != "require" {
		t.Errorf("Expected sslMode='require', got '%s'", conn.SSLMode)
	}
}

func TestConnectionFormBuildConnectionDSNMode(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	m.dsnMode = true
	m.formValues[fieldName] = "dsn-conn"
	m.formValues[fieldType] = "postgres"
	m.formValues[fieldDSN] = "postgres://admin:pass@localhost:5432/mydb"
	m.formValues[fieldHost] = "leftover" // should be cleared

	conn := m.buildConnectionFromForm()

	if conn.DSN != "postgres://admin:pass@localhost:5432/mydb" {
		t.Errorf("Expected DSN to be set, got '%s'", conn.DSN)
	}
	if conn.Host != "" {
		t.Errorf("Expected host to be empty in DSN mode, got '%s'", conn.Host)
	}
}

func TestConnectionFormViewList(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "dev-pg", Type: "postgres", Host: "localhost", Port: 5432},
		config.Connection{Name: "dev-mysql", Type: "mysql", Host: "db.host", Port: 3306},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	view := m.View()
	if view == "" {
		t.Error("View should not be empty")
	}
	if !strings.Contains(view, "Connection Manager") {
		t.Error("View should contain title 'Connection Manager'")
	}
}

func TestConnectionFormViewForm(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})

	view := m.View()
	if view == "" {
		t.Error("Form view should not be empty")
	}
	if !strings.Contains(view, "New Connection") {
		t.Error("Form view should contain 'New Connection'")
	}
}

func TestConnectionFormViewDeleteConfirm(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "my-conn", Type: "postgres"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter delete confirm
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})

	view := m.View()
	if view == "" {
		t.Error("Delete confirm view should not be empty")
	}
	if !strings.Contains(view, "Delete") {
		t.Error("Delete confirm view should contain 'Delete'")
	}
}

func TestConnectionFormEmptyListActions(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// These should not crash on empty list
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if m.mode != connModeList {
		t.Error("Enter on empty list should stay in list mode")
	}

	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	if m.mode != connModeList {
		t.Error("Edit on empty list should stay in list mode")
	}

	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("d")})
	if m.mode != connModeList {
		t.Error("Delete on empty list should stay in list mode")
	}
}

func TestConnectionFormTestResult(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig(
		config.Connection{Name: "test-conn", Type: "postgres"},
	)
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Simulate test result
	m, _ = m.Update(ConnectionTestResultMsg{
		Success: true,
		Message: "Connection successful!",
	})

	if m.testing {
		t.Error("Expected testing=false after result")
	}
	if !m.testIsSuccess {
		t.Error("Expected testIsSuccess=true")
	}
	if m.testResult != "Connection successful!" {
		t.Errorf("Expected testResult='Connection successful!', got '%s'", m.testResult)
	}
}

func TestConnectionFormTestResultFailure(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	m, _ = m.Update(ConnectionTestResultMsg{
		Success: false,
		Message: "Connection refused",
	})

	if m.testIsSuccess {
		t.Error("Expected testIsSuccess=false")
	}
	if m.testResult != "Connection refused" {
		t.Errorf("Expected testResult='Connection refused', got '%s'", m.testResult)
	}
}

func TestConnectionFormPortOnlyDigits(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	// Enter form and navigate to port field
	m.enterNewForm()
	m.activeField = fieldPort
	m.formValues[fieldPort] = ""

	// Type digits
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("5")})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("4")})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2")})

	if m.formValues[fieldPort] != "5432" {
		t.Errorf("Expected port='5432', got '%s'", m.formValues[fieldPort])
	}

	// Try non-digit
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})
	if m.formValues[fieldPort] != "5432" {
		t.Errorf("Expected port='5432' (unchanged), got '%s'", m.formValues[fieldPort])
	}
}

func TestNewConnectionModel(t *testing.T) {
	cfg := newTestConfig()
	m := NewConnectionModel(cfg)

	if m.screen != ScreenConnection {
		t.Error("Expected screen=ScreenConnection")
	}
}

func TestConnectionFormSQLiteSkipsServerFields(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)
	m.SetSize(80, 24)

	m.enterNewForm()
	m.formValues[fieldType] = "sqlite"
	m.typeSelectIdx = 2 // sqlite is index 2

	// Starting from fieldName, tab through fields
	m.activeField = fieldName
	m.nextField()
	if m.activeField != fieldType {
		t.Errorf("Expected fieldType after name, got %d", m.activeField)
	}

	// Next should skip host, port, user, password, ssl and go to database
	m.nextField()
	if m.activeField != fieldDatabase {
		t.Errorf("Expected fieldDatabase for sqlite (skipping server fields), got %d", m.activeField)
	}

	// Next should wrap to name
	m.nextField()
	if m.activeField != fieldName {
		t.Errorf("Expected fieldName after database for sqlite, got %d", m.activeField)
	}
}

func TestConnectionFormValidationSQLite(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	m.formValues[fieldName] = "test-sqlite"
	m.formValues[fieldType] = "sqlite"
	m.formValues[fieldDatabase] = ""

	result := m.validateForm()
	if result == "" {
		t.Error("Expected validation error for empty database path with sqlite")
	}
	if !strings.Contains(result, "File Path") || !strings.Contains(result, "path") {
		// Accept any error mentioning the required field
		if !strings.Contains(result, "required") {
			t.Errorf("Expected validation error about file path, got: %s", result)
		}
	}
}

func TestConnectionFormValidationDSNMode(t *testing.T) {
	theme := DarkTheme()
	cfg := newTestConfig()
	m := NewConnectionFormModel(cfg, theme)

	m.dsnMode = true
	m.formValues[fieldName] = "test-dsn"
	m.formValues[fieldType] = "postgres"
	m.formValues[fieldDSN] = ""

	result := m.validateForm()
	if result == "" {
		t.Error("Expected validation error for empty DSN in DSN mode")
	}
	if !strings.Contains(result, "DSN") {
		t.Errorf("Expected error about DSN, got: %s", result)
	}
}

func TestConnectionFormDefaultPorts(t *testing.T) {
	tests := []struct {
		dbType string
		port   int
	}{
		{"postgres", 5432},
		{"mysql", 3306},
		{"sqlite", 0},
		{"mssql", 1433},
	}

	for _, tt := range tests {
		got := defaultPorts[tt.dbType]
		if got != tt.port {
			t.Errorf("defaultPorts[%s] = %d, want %d", tt.dbType, got, tt.port)
		}
	}
}
