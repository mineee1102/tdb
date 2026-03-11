package ui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/config"
	"github.com/minee/tdb/internal/db"
)

// FuzzySearchType represents the type of search result
type FuzzySearchType int

const (
	FuzzyDatabase FuzzySearchType = iota
	FuzzyTable
	FuzzyTab
	FuzzySQLFile
)

// FuzzySearchResult represents a single search result
type FuzzySearchResult struct {
	Type     FuzzySearchType
	Name     string
	Schema   string // for tables
	TabIdx   int    // for tabs
	FilePath string // for SQL files
}

// FuzzySearchModal manages the search modal state
type FuzzySearchModal struct {
	visible        bool
	input          string
	results        []FuzzySearchResult
	selectedIdx    int
	allDatabases   []string
	allTables      []FuzzyTableInfo
	allTabs        []FuzzyTabInfo
	allSQLFiles    []FuzzySQLFileInfo
	theme          *Theme
	width          int
	height         int
	lastUpdateTime time.Time
}

// FuzzyTableInfo holds table metadata
type FuzzyTableInfo struct {
	Schema string
	Name   string
}

// FuzzyTabInfo holds tab metadata
type FuzzyTabInfo struct {
	Name  string
	Index int
}

// FuzzySQLFileInfo holds saved SQL file metadata
type FuzzySQLFileInfo struct {
	Name     string
	FilePath string
}

// NewFuzzySearchModal creates a new search modal
func NewFuzzySearchModal(theme *Theme) FuzzySearchModal {
	return FuzzySearchModal{
		theme:     theme,
		allTables: make([]FuzzyTableInfo, 0),
		allTabs:   make([]FuzzyTabInfo, 0),
	}
}

// Refresh loads current databases/tables/tabs from the driver and tab bar
func (m *FuzzySearchModal) Refresh(
	driver db.Driver,
	databases []string,
	currentDB string,
	schemas []string,
	tables map[string][]db.TableInfo,
	tabs []Tab,
) {
	m.allDatabases = databases
	m.lastUpdateTime = time.Now()

	// Load tables for all schemas
	m.allTables = nil
	for _, schema := range schemas {
		if tablesInSchema, ok := tables[schema]; ok {
			for _, t := range tablesInSchema {
				m.allTables = append(m.allTables, FuzzyTableInfo{Schema: schema, Name: t.Name})
			}
		}
	}

	// Load open tabs
	m.allTabs = nil
	for i, tab := range tabs {
		m.allTabs = append(m.allTabs, FuzzyTabInfo{Name: tab.title, Index: i})
	}

	// Load saved SQL files
	m.allSQLFiles = nil
	sqlDir := filepath.Join(config.ConfigDir(), "queries")
	if entries, err := os.ReadDir(sqlDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
				name := strings.TrimSuffix(e.Name(), ".sql")
				m.allSQLFiles = append(m.allSQLFiles, FuzzySQLFileInfo{
					Name:     name,
					FilePath: filepath.Join(sqlDir, e.Name()),
				})
			}
		}
	}

	m.updateResults()
}

// Open opens the search modal
func (m *FuzzySearchModal) Open() {
	m.visible = true
	m.input = ""
	m.selectedIdx = 0
	m.updateResults()
}

// Close closes the search modal
func (m *FuzzySearchModal) Close() {
	m.visible = false
	m.input = ""
}

// IsVisible returns whether the modal is visible
func (m FuzzySearchModal) IsVisible() bool {
	return m.visible
}

// updateResults performs fuzzy filtering
func (m *FuzzySearchModal) updateResults() {
	m.results = nil
	if m.input == "" {
		// Show all if empty
		for _, db := range m.allDatabases {
			m.results = append(m.results, FuzzySearchResult{
				Type: FuzzyDatabase,
				Name: db,
			})
		}
		for _, t := range m.allTables {
			m.results = append(m.results, FuzzySearchResult{
				Type:   FuzzyTable,
				Name:   t.Name,
				Schema: t.Schema,
			})
		}
		for _, tab := range m.allTabs {
			m.results = append(m.results, FuzzySearchResult{
				Type:   FuzzyTab,
				Name:   tab.Name,
				TabIdx: tab.Index,
			})
		}
		for _, f := range m.allSQLFiles {
			m.results = append(m.results, FuzzySearchResult{
				Type:     FuzzySQLFile,
				Name:     f.Name,
				FilePath: f.FilePath,
			})
		}
	} else {
		// Fuzzy filter
		for _, db := range m.allDatabases {
			if fuzzyMatch(m.input, db) {
				m.results = append(m.results, FuzzySearchResult{
					Type: FuzzyDatabase,
					Name: db,
				})
			}
		}
		for _, t := range m.allTables {
			if fuzzyMatch(m.input, t.Name) || fuzzyMatch(m.input, t.Schema+"."+t.Name) {
				m.results = append(m.results, FuzzySearchResult{
					Type:   FuzzyTable,
					Name:   t.Name,
					Schema: t.Schema,
				})
			}
		}
		for _, tab := range m.allTabs {
			if fuzzyMatch(m.input, tab.Name) {
				m.results = append(m.results, FuzzySearchResult{
					Type:   FuzzyTab,
					Name:   tab.Name,
					TabIdx: tab.Index,
				})
			}
		}
		for _, f := range m.allSQLFiles {
			if fuzzyMatch(m.input, f.Name) {
				m.results = append(m.results, FuzzySearchResult{
					Type:     FuzzySQLFile,
					Name:     f.Name,
					FilePath: f.FilePath,
				})
			}
		}
	}

	// Clamp selected index
	if m.selectedIdx >= len(m.results) {
		m.selectedIdx = len(m.results) - 1
	}
	if m.selectedIdx < 0 {
		m.selectedIdx = 0
	}
}

// fuzzyMatch performs simple fuzzy matching (subsequence match)
// Returns true if all characters of pattern appear in text in order (case-insensitive)
func fuzzyMatch(pattern, text string) bool {
	pattern = strings.ToLower(pattern)
	text = strings.ToLower(text)

	patIdx := 0
	for i := 0; i < len(text) && patIdx < len(pattern); i++ {
		if text[i] == pattern[patIdx] {
			patIdx++
		}
	}
	return patIdx == len(pattern)
}

// GetSelected returns the currently selected result
func (m FuzzySearchModal) GetSelected() *FuzzySearchResult {
	if m.selectedIdx < 0 || m.selectedIdx >= len(m.results) {
		return nil
	}
	return &m.results[m.selectedIdx]
}

// SelectNext moves selection down
func (m *FuzzySearchModal) SelectNext() {
	if m.selectedIdx < len(m.results)-1 {
		m.selectedIdx++
	}
}

// SelectPrev moves selection up
func (m *FuzzySearchModal) SelectPrev() {
	if m.selectedIdx > 0 {
		m.selectedIdx--
	}
}

// AddChar adds a character to the input
func (m *FuzzySearchModal) AddChar(ch string) {
	m.input += ch
	m.updateResults()
}

// RemoveChar removes the last character from input
func (m *FuzzySearchModal) RemoveChar() {
	if len(m.input) > 0 {
		m.input = m.input[:len(m.input)-1]
		m.updateResults()
	}
}

// SetSize sets the modal dimensions
func (m *FuzzySearchModal) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// Update handles key input
func (m *FuzzySearchModal) Update(msg tea.KeyMsg) (*FuzzySearchModal, tea.Cmd) {
	if !m.visible {
		return m, nil
	}

	switch msg.String() {
	case "esc":
		m.Close()
		return m, nil

	case "down":
		m.SelectNext()
		return m, nil

	case "up":
		m.SelectPrev()
		return m, nil

	case "backspace":
		m.RemoveChar()
		return m, nil

	case "enter":
		// Caller will handle the selection
		return m, nil

	default:
		if msg.Type == tea.KeyRunes {
			for _, r := range msg.Runes {
				m.AddChar(string(r))
			}
		}
		return m, nil
	}
}

// View renders the search modal
func (m FuzzySearchModal) View() string {
	if !m.visible {
		return ""
	}

	modalWidth := 70
	if m.width > 120 {
		modalWidth = 80
	}
	if m.width < 74 {
		modalWidth = m.width - 6
	}
	innerWidth := modalWidth - 6 // account for border + padding

	var sb strings.Builder

	// Search input with prompt icon
	inputStyle := lipgloss.NewStyle().
		Foreground(m.theme.ColorFg).
		Background(m.theme.ColorPrimary).
		Bold(true).
		Padding(0, 1)
	inputFieldStyle := lipgloss.NewStyle().
		Foreground(m.theme.ColorFg).
		Padding(0, 1)
	promptIcon := inputStyle.Render(" ❯ ")
	inputField := inputFieldStyle.Render(m.input + "█")
	sb.WriteString(promptIcon + inputField)
	sb.WriteString("\n")

	// Thin separator
	sepStyle := lipgloss.NewStyle().Foreground(m.theme.ColorFgDim)
	sb.WriteString(sepStyle.Render(strings.Repeat("━", innerWidth)))
	sb.WriteString("\n")

	// Dynamic max results — fill available modal space
	// Modal is ~80% of height, minus ~6 lines for input/separator/footer/border
	maxResults := m.height*8/10 - 6
	if maxResults < 6 {
		maxResults = 6
	}

	if len(m.results) == 0 {
		emptyStyle := lipgloss.NewStyle().
			Foreground(m.theme.ColorFgDim).
			Italic(true).
			Padding(1, 1)
		sb.WriteString(emptyStyle.Render("No matching results"))
		sb.WriteString("\n")
	} else {
		// Scrolling: keep selected item visible
		startIdx := 0
		if m.selectedIdx >= maxResults {
			startIdx = m.selectedIdx - maxResults + 1
		}
		endIdx := startIdx + maxResults
		if endIdx > len(m.results) {
			endIdx = len(m.results)
		}

		for i := startIdx; i < endIdx; i++ {
			res := m.results[i]

			// Type badge + label
			var badge, label string
			switch res.Type {
			case FuzzyDatabase:
				badge = "▣"
				label = res.Name
			case FuzzyTable:
				badge = "≡"
				label = res.Name
			case FuzzyTab:
				badge = "◇"
				label = res.Name
			case FuzzySQLFile:
				badge = "✦"
				label = res.Name
			}

			// Badge style
			badgeStyle := lipgloss.NewStyle().
				Foreground(lipgloss.Color("#F9FAFB")).
				Background(m.theme.ColorPrimary).
				Bold(true).
				Padding(0, 1)
			labelStyle := lipgloss.NewStyle().
				Foreground(m.theme.ColorFg).
				Padding(0, 1)

			// Truncate long labels
			maxLabelW := innerWidth - lipgloss.Width(badgeStyle.Render(badge)) - 4
			if len(label) > maxLabelW && maxLabelW > 3 {
				label = label[:maxLabelW-1] + "…"
			}

			if i == m.selectedIdx {
				// Selected row: invert colors
				badgeStyle = badgeStyle.
					Background(m.theme.ColorSecondary)
				labelStyle = labelStyle.
					Bold(true).
					Background(lipgloss.Color("#374151"))
				// Pad label to fill row width
				labelText := label
				padW := innerWidth - lipgloss.Width(badgeStyle.Render(badge)) - 2
				if padW > len(labelText) {
					labelText += strings.Repeat(" ", padW-len(labelText))
				}
				sb.WriteString("  " + badgeStyle.Render(badge) + labelStyle.Render(labelText))
			} else {
				sb.WriteString("  " + badgeStyle.Render(badge) + labelStyle.Render(label))
			}
			sb.WriteString("\n")
		}

		// Scroll indicator
		if len(m.results) > maxResults {
			countStyle := lipgloss.NewStyle().
				Foreground(m.theme.ColorFgDim).
				Italic(true).
				Padding(0, 1)
			sb.WriteString(countStyle.Render(fmt.Sprintf("  %d / %d", m.selectedIdx+1, len(m.results))))
			sb.WriteString("\n")
		}
	}

	// Footer hints
	sb.WriteString("\n")
	hintStyle := lipgloss.NewStyle().
		Foreground(m.theme.ColorFgDim).
		Padding(0, 1)
	sb.WriteString(hintStyle.Render("↑↓ navigate  ⏎ select  esc close"))

	// Modal container with centered positioning
	return m.renderCenteredModal(sb.String(), modalWidth)
}

// renderCenteredModal centers the modal content on screen
func (m FuzzySearchModal) renderCenteredModal(content string, modalWidth int) string {
	modalHeight := m.height * 8 / 10
	if modalHeight < 12 {
		modalHeight = 12
	}
	if modalHeight > m.height-4 {
		modalHeight = m.height - 4
	}

	bordered := m.theme.DialogBorder.
		Width(modalWidth).
		Height(modalHeight).
		Render(content)

	verticalPad := (m.height - modalHeight) / 2
	horizontalPad := (m.width - modalWidth) / 2
	if horizontalPad < 0 {
		horizontalPad = 0
	}
	if verticalPad < 0 {
		verticalPad = 0
	}

	margin := strings.Repeat(" ", horizontalPad)
	lines := strings.Split(bordered, "\n")

	var result strings.Builder
	for i := 0; i < verticalPad; i++ {
		result.WriteString("\n")
	}
	for _, line := range lines {
		result.WriteString(margin)
		result.WriteString(line)
		result.WriteString("\n")
	}
	return result.String()
}
