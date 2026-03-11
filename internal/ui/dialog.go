package ui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── Dialog Messages ───────────────────────────────────────────────

// DialogConfirmMsg is sent when the user confirms a dialog.
type DialogConfirmMsg struct {
	DialogType string
}

// DialogCancelMsg is sent when the user cancels a dialog.
type DialogCancelMsg struct{}

// ── DialogModel ───────────────────────────────────────────────────

// DialogModel manages a modal confirm/preview dialog.
type DialogModel struct {
	visible      bool
	title        string
	content      string
	dialogType   string // "commit", etc.
	selectedBtn  int    // 0=Execute, 1=Cancel
	scrollOffset int
	contentLines int
	width        int
	height       int
	theme        *Theme
}

// NewDialogModel creates a new DialogModel.
func NewDialogModel(theme *Theme) DialogModel {
	return DialogModel{
		theme: theme,
	}
}

// Show displays the dialog with the given title, content, and type.
func (m *DialogModel) Show(title, content, dialogType string) {
	m.visible = true
	m.title = title
	m.content = content
	m.dialogType = dialogType
	m.selectedBtn = 0
	m.scrollOffset = 0
	m.contentLines = strings.Count(content, "\n") + 1
}

// ShowError displays an error dialog with just an OK button.
func (m *DialogModel) ShowError(title, content string) {
	m.Show(title, content, "error")
}

// Hide hides the dialog.
func (m *DialogModel) Hide() {
	m.visible = false
	m.content = ""
	m.title = ""
}

// IsVisible returns whether the dialog is currently shown.
func (m DialogModel) IsVisible() bool {
	return m.visible
}

// SetSize updates the dialog's parent dimensions for centering.
func (m *DialogModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// Update handles key input for the dialog.
func (m DialogModel) Update(msg tea.KeyMsg) (DialogModel, tea.Cmd) {
	// Error dialog: any dismiss key just closes
	if m.dialogType == "error" {
		switch msg.String() {
		case "enter", "esc", "q", " ":
			m.Hide()
			return m, func() tea.Msg { return DialogCancelMsg{} }
		case "j", "down":
			maxScroll := m.contentLines - m.visibleContentHeight()
			if maxScroll < 0 {
				maxScroll = 0
			}
			if m.scrollOffset < maxScroll {
				m.scrollOffset++
			}
		case "k", "up":
			if m.scrollOffset > 0 {
				m.scrollOffset--
			}
		}
		return m, nil
	}

	switch msg.String() {
	case "left", "h":
		if m.selectedBtn > 0 {
			m.selectedBtn--
		}
	case "right", "l":
		if m.selectedBtn < 1 {
			m.selectedBtn++
		}
	case "tab":
		m.selectedBtn = (m.selectedBtn + 1) % 2
	case "enter":
		if m.selectedBtn == 0 {
			return m, func() tea.Msg {
				return DialogConfirmMsg{DialogType: m.dialogType}
			}
		}
		m.Hide()
		return m, func() tea.Msg { return DialogCancelMsg{} }
	case "esc", "q":
		m.Hide()
		return m, func() tea.Msg { return DialogCancelMsg{} }
	case "j", "down":
		maxScroll := m.contentLines - m.visibleContentHeight()
		if maxScroll < 0 {
			maxScroll = 0
		}
		if m.scrollOffset < maxScroll {
			m.scrollOffset++
		}
	case "k", "up":
		if m.scrollOffset > 0 {
			m.scrollOffset--
		}
	case "y", "Y":
		return m, func() tea.Msg {
			return DialogConfirmMsg{DialogType: m.dialogType}
		}
	case "n", "N":
		m.Hide()
		return m, func() tea.Msg { return DialogCancelMsg{} }
	}
	return m, nil
}

func (m DialogModel) visibleContentHeight() int {
	h := m.height/2 - 8
	if h < 5 {
		h = 5
	}
	return h
}

// View renders the dialog as a centered overlay.
func (m DialogModel) View(width, height int) string {
	if !m.visible {
		return ""
	}

	// Max dialog width: 80% of screen, capped at 100 chars
	maxW := width * 80 / 100
	if maxW > 100 {
		maxW = 100
	}
	if maxW < 40 {
		maxW = 40
	}
	// Account for border + padding (2 border + 2*2 padding = 6)
	contentMaxW := maxW - 6
	if contentMaxW < 20 {
		contentMaxW = 20
	}

	var sb strings.Builder

	// Title
	sb.WriteString(m.theme.DialogTitle.Render(" " + m.title + " "))
	sb.WriteString("\n\n")

	// Content with scrolling
	lines := strings.Split(m.content, "\n")
	visHeight := m.visibleContentHeight()

	start := m.scrollOffset
	end := start + visHeight
	if end > len(lines) {
		end = len(lines)
	}
	if start >= end {
		start = 0
	}

	contentStyle := m.theme.SQLInput.Width(contentMaxW).MaxWidth(contentMaxW)
	for i := start; i < end; i++ {
		line := lines[i]
		// Truncate overly long lines to prevent overflow
		runes := []rune(line)
		if len(runes) > contentMaxW {
			line = string(runes[:contentMaxW-1]) + "…"
		}
		sb.WriteString(contentStyle.Render(line))
		sb.WriteString("\n")
	}

	// Scroll indicator
	if len(lines) > visHeight {
		indW := contentMaxW
		if indW > 58 {
			indW = 58
		}
		pad := (indW - 18) / 2
		if pad < 1 {
			pad = 1
		}
		indicator := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim).
			Render(strings.Repeat("─", pad) + " scroll with j/k " + strings.Repeat("─", pad))
		sb.WriteString(indicator)
		sb.WriteString("\n")
	}

	sb.WriteString("\n")

	// Buttons
	if m.dialogType == "error" {
		okBtn := m.theme.DialogButtonActive.Render(" OK (Enter) ")
		sb.WriteString(okBtn)
	} else {
		var execBtn, cancelBtn string
		if m.selectedBtn == 0 {
			execBtn = m.theme.DialogButtonActive.Render(" Execute (y) ")
			cancelBtn = m.theme.DialogButton.Render(" Cancel (n) ")
		} else {
			execBtn = m.theme.DialogButton.Render(" Execute (y) ")
			cancelBtn = m.theme.DialogButtonActive.Render(" Cancel (n) ")
		}
		sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, execBtn, "  ", cancelBtn))
	}

	content := m.theme.DialogBorder.MaxWidth(maxW).Render(sb.String())

	return lipgloss.Place(
		width, height,
		lipgloss.Center, lipgloss.Center,
		content,
	)
}
