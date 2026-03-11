package ui

import (
	"fmt"
)

// StatusBarModel displays connection info, row count, and operational messages.
type StatusBarModel struct {
	connectionInfo string
	rowCount       int
	message        string
	messageType    string // "info", "error", "success"
	extras         string // additional info (sort, filter)
	width          int
	theme          *Theme
}

// NewStatusBarModel creates a new StatusBarModel.
func NewStatusBarModel(theme *Theme) StatusBarModel {
	return StatusBarModel{
		theme:       theme,
		messageType: "info",
	}
}

// SetConnectionInfo sets the connection info string.
func (m *StatusBarModel) SetConnectionInfo(info string) {
	m.connectionInfo = info
}

// SetRowCount sets the displayed row count.
func (m *StatusBarModel) SetRowCount(count int) {
	m.rowCount = count
}

// SetMessage sets a feedback message.
func (m *StatusBarModel) SetMessage(msg, msgType string) {
	m.message = msg
	m.messageType = msgType
}

// SetExtras sets additional status info (sort/filter state).
func (m *StatusBarModel) SetExtras(extras string) {
	m.extras = extras
}

// SetWidth updates the terminal width.
func (m *StatusBarModel) SetWidth(w int) {
	m.width = w
}

// View renders the status bar.
func (m StatusBarModel) View() string {
	// Determine the width available (subtract border padding)
	w := m.width
	if w < 10 {
		w = 80
	}

	connPart := ""
	if m.connectionInfo != "" {
		connPart = "Connected: " + m.connectionInfo
	}

	rowsPart := ""
	if m.rowCount > 0 {
		rowsPart = fmt.Sprintf("%d rows", m.rowCount)
	}

	hintPart := "Tab: Switch | ?: Help | Ctrl+E: SQL"

	// Build message part if present
	msgPart := ""
	if m.message != "" {
		msgPart = m.message
	}

	// Compose left portion
	parts := make([]string, 0, 5)
	if connPart != "" {
		parts = append(parts, connPart)
	}
	if rowsPart != "" {
		parts = append(parts, rowsPart)
	}
	if m.extras != "" {
		parts = append(parts, m.extras)
	}
	if msgPart != "" {
		parts = append(parts, msgPart)
	}
	parts = append(parts, hintPart)

	content := ""
	for i, p := range parts {
		if i > 0 {
			content += " │ "
		}
		content += p
	}

	// Pick style based on message type
	style := m.theme.StatusBar
	if m.message != "" {
		switch m.messageType {
		case "error":
			style = m.theme.StatusBarError
		case "success":
			style = m.theme.StatusBarSuccess
		}
	}

	return style.Width(w).MaxHeight(1).Render(content)
}


