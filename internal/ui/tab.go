package ui

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

// TabType represents the kind of tab.
type TabType int

const (
	TabData TabType = iota
	TabSQL
)

// Tab holds the state of a single tab.
type Tab struct {
	tabType       TabType
	title         string
	schemaName    string
	tableName     string
	sqlFilePath   string // path to saved SQL file (empty if unsaved)
	dataView      DataViewModel
	sqlEditor     SQLEditorModel
	structureView StructureViewModel
	showStructure bool
	showSQLEditor bool

	// Neovim state per tab
	nvimPane    *NvimPane
	nvimEnabled bool
}

// TabBar manages multiple tabs.
type TabBar struct {
	tabs      []Tab
	activeIdx int
	theme     *Theme
}

// NewTabBar creates a new TabBar.
func NewTabBar(theme *Theme) TabBar {
	return TabBar{
		theme: theme,
	}
}

// Len returns the number of tabs.
func (tb TabBar) Len() int {
	return len(tb.tabs)
}

// ActiveIndex returns the active tab index.
func (tb TabBar) ActiveIndex() int {
	return tb.activeIdx
}

// ActiveTab returns a pointer to the active tab, or nil if no tabs.
func (tb *TabBar) ActiveTab() *Tab {
	if len(tb.tabs) == 0 {
		return nil
	}
	return &tb.tabs[tb.activeIdx]
}

// AddTab appends a new tab and makes it active.
func (tb *TabBar) AddTab(tab Tab) int {
	tb.tabs = append(tb.tabs, tab)
	tb.activeIdx = len(tb.tabs) - 1
	return tb.activeIdx
}

// RemoveTab removes the tab at idx and adjusts activeIdx.
func (tb *TabBar) RemoveTab(idx int) {
	if idx < 0 || idx >= len(tb.tabs) {
		return
	}
	tb.tabs = append(tb.tabs[:idx], tb.tabs[idx+1:]...)
	if len(tb.tabs) == 0 {
		tb.activeIdx = 0
		return
	}
	if tb.activeIdx >= len(tb.tabs) {
		tb.activeIdx = len(tb.tabs) - 1
	} else if tb.activeIdx > idx {
		tb.activeIdx--
	}
}

// SetActive sets the active tab index.
func (tb *TabBar) SetActive(idx int) {
	if idx >= 0 && idx < len(tb.tabs) {
		tb.activeIdx = idx
	}
}

// NextTab moves to the next tab (wraps around).
func (tb *TabBar) NextTab() {
	if len(tb.tabs) <= 1 {
		return
	}
	tb.activeIdx = (tb.activeIdx + 1) % len(tb.tabs)
}

// PrevTab moves to the previous tab (wraps around).
func (tb *TabBar) PrevTab() {
	if len(tb.tabs) <= 1 {
		return
	}
	tb.activeIdx = (tb.activeIdx - 1 + len(tb.tabs)) % len(tb.tabs)
}

// FindDataTab returns the index of an existing data tab for the given schema.table, or -1.
func (tb TabBar) FindDataTab(schema, table string) int {
	for i, t := range tb.tabs {
		if t.tabType == TabData && t.schemaName == schema && t.tableName == table {
			return i
		}
	}
	return -1
}

// View renders the tab bar.
func (tb TabBar) View(width int) string {
	if len(tb.tabs) == 0 {
		return ""
	}

	// Styles
	activeStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(tb.theme.ColorFg).
		Background(tb.theme.ColorPrimary).
		Padding(0, 1)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(tb.theme.ColorFgDim).
		Padding(0, 1)

	sepStyle := lipgloss.NewStyle().
		Foreground(tb.theme.ColorFgDim)

	var parts []string
	for i, t := range tb.tabs {
		label := t.title
		if len([]rune(label)) > 20 {
			label = string([]rune(label)[:19]) + "…"
		}
		// Tab number prefix
		prefix := fmt.Sprintf("%d:", i+1)
		tabLabel := prefix + label

		if i == tb.activeIdx {
			parts = append(parts, activeStyle.Render(tabLabel))
		} else {
			parts = append(parts, inactiveStyle.Render(tabLabel))
		}
		if i < len(tb.tabs)-1 {
			parts = append(parts, sepStyle.Render("│"))
		}
	}

	bar := lipgloss.JoinHorizontal(lipgloss.Top, parts...)

	// Constrain to width and single line
	barStyle := lipgloss.NewStyle().
		MaxWidth(width).
		Width(width).
		MaxHeight(1).
		Height(1)

	return barStyle.Render(bar)
}
