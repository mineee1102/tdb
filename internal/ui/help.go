package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// ActivePane represents which pane is currently active.
type ActivePane int

const (
	PaneTableList ActivePane = iota
	PaneDataView
	PaneSQLEditor
)

// HelpModel manages the help overlay display.
type HelpModel struct {
	visible bool
	theme   *Theme
}

// NewHelpModel creates a new HelpModel.
func NewHelpModel(theme *Theme) HelpModel {
	return HelpModel{
		theme: theme,
	}
}

// Toggle toggles the help visibility.
func (m *HelpModel) Toggle() {
	m.visible = !m.visible
}

// IsVisible returns whether help is currently shown.
func (m HelpModel) IsVisible() bool {
	return m.visible
}

type helpBinding struct {
	key  string
	desc string
}

// View renders the help overlay for the given active pane.
func (m HelpModel) View(pane ActivePane, width, height int) string {
	if !m.visible {
		return ""
	}

	// Global bindings
	globalBindings := []helpBinding{
		{"Tab", "Switch pane"},
		{"Arrow keys", "Move pane at edge"},
		{"Ctrl+Q", "New SQL tab"},
		{"gw", "Close current tab"},
		{"gl / gh", "Next / Prev tab"},
		{"Ctrl+G", "Execute SQL"},
		{"?", "Toggle help"},
		{"q / Ctrl+C", "Quit"},
	}

	// Pane-specific bindings
	var contextBindings []helpBinding
	var contextTitle string

	switch pane {
	case PaneTableList:
		contextTitle = "Table List"
		contextBindings = []helpBinding{
			{"j/k / ↑/↓", "Navigate tables"},
			{"Enter", "Open table"},
			{"/", "Filter tables"},
			{"[  ]", "Switch database (prev / next)"},
			{"i", "Table structure"},
			{"c", "Create table"},
			{"D (Shift+d)", "Drop table"},
			{"Esc", "Clear filter"},
		}
	case PaneDataView:
		contextTitle = "Data View"
		contextBindings = []helpBinding{
			{"j/k / ↑/↓", "Move row up/down"},
			{"h/l / ←/→", "Move column left/right"},
			{"g / G", "First / last row"},
			{"Ctrl+U/D", "Page up / down"},
			{"n / P", "Next / prev data page"},
			{"Enter / e", "Edit cell"},
			{"a", "Add new row"},
			{"d", "Delete/undelete row"},
			{"Ctrl+S", "Commit changes"},
			{"Ctrl+Z", "Discard all changes"},
			{"s", "Sort by current column"},
			{"/", "Filter (WHERE clause)"},
			{"f", "Quick filter by cell value"},
			{"Ctrl+/", "Clear filter"},
			{"Ctrl+O", "Export data"},
			{"y", "Copy cell to clipboard"},
			{"yy", "Copy row to clipboard"},
			{"V (Shift+v)", "Visual line select"},
			{"p", "Paste from yank/clipboard"},
		}
	case PaneSQLEditor:
		contextTitle = "SQL Editor"
		contextBindings = []helpBinding{
			{"Enter", "Execute (when SQL ends with ;) / New line"},
			{"Ctrl+G", "Execute SQL"},
			{"← / → / ↑ / ↓", "Move cursor"},
			{"Ctrl+↑/↓", "Navigate query history"},
			{"Ctrl+R", "Search history"},
			{"Tab", "Accept suggestion / Autocomplete"},
			{"↑ / ↓", "Navigate suggestions"},
			{"Esc", "Close suggestions"},
			{"Ctrl+K", "Delete to end of line"},
		}
	}

	// Render
	var sb strings.Builder
	sb.WriteString(m.theme.TitleBar.Render(" Keyboard Shortcuts "))
	sb.WriteString("\n\n")

	// Context-specific section
	sb.WriteString(m.theme.TableListHeader.Render("── " + contextTitle + " ──"))
	sb.WriteString("\n")
	for _, b := range contextBindings {
		sb.WriteString(m.theme.HelpKey.Render(b.key))
		sb.WriteString(m.theme.HelpDesc.Render(b.desc))
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(m.theme.TableListHeader.Render("── Global ──"))
	sb.WriteString("\n")
	for _, b := range globalBindings {
		sb.WriteString(m.theme.HelpKey.Render(b.key))
		sb.WriteString(m.theme.HelpDesc.Render(b.desc))
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpDesc.Render("Press ? to close"))

	content := m.theme.HelpBox.Render(sb.String())

	// Center the help box
	return lipgloss.Place(
		width, height,
		lipgloss.Center, lipgloss.Center,
		content,
	)
}
