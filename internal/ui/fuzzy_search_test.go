package ui

import (
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/db"
)

// TestFuzzySearchModalCreation tests creating a new fuzzy search modal
func TestFuzzySearchModalCreation(t *testing.T) {
	theme := &Theme{
		DialogTitle: lipgloss.NewStyle(),
		SQLInput:    lipgloss.NewStyle(),
		DataRow:     lipgloss.NewStyle(),
		DataSelected: lipgloss.NewStyle(),
		HelpDesc:    lipgloss.NewStyle(),
		DialogBorder: lipgloss.NewStyle(),
	}

	modal := NewFuzzySearchModal(theme)

	if modal.visible {
		t.Errorf("Expected modal to be not visible on creation")
	}
	if modal.input != "" {
		t.Errorf("Expected empty input on creation, got %q", modal.input)
	}
	if modal.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx to be 0, got %d", modal.selectedIdx)
	}
}

// TestFuzzySearchModalOpen tests opening the modal
func TestFuzzySearchModalOpen(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	modal.Open()

	if !modal.visible {
		t.Errorf("Expected modal to be visible after Open()")
	}
	if modal.input != "" {
		t.Errorf("Expected input to be reset on Open(), got %q", modal.input)
	}
	if modal.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx to be 0 after Open(), got %d", modal.selectedIdx)
	}
}

// TestFuzzySearchModalClose tests closing the modal
func TestFuzzySearchModalClose(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	modal.Open()
	modal.Close()

	if modal.visible {
		t.Errorf("Expected modal to be hidden after Close()")
	}
}

// TestFuzzyMatch tests the fuzzy matching algorithm
func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		pattern  string
		text     string
		expected bool
	}{
		{"u", "users", true},
		{"ue", "users", true},
		{"ers", "users", true},
		{"usr", "users", true},
		{"uuuu", "users", false},
		{"", "anything", true},
		{"a", "", false},
		{"Users", "users", true},
		{"USERS", "users", true},
	}

	for _, tt := range tests {
		result := fuzzyMatch(tt.pattern, tt.text)
		if result != tt.expected {
			t.Errorf("fuzzyMatch(%q, %q) = %v, want %v", tt.pattern, tt.text, result, tt.expected)
		}
	}
}

// TestFuzzySearchRefresh tests the refresh functionality
func TestFuzzySearchRefresh(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	databases := []string{"db1", "db2"}
	schemas := []string{"public"}
	tables := map[string][]db.TableInfo{
		"public": {
			{Name: "users"},
			{Name: "products"},
		},
	}
	tabs := []Tab{
		{title: "users_data"},
	}

	modal.Refresh(nil, databases, "db1", schemas, tables, tabs)

	if len(modal.allDatabases) != 2 {
		t.Errorf("Expected 2 databases, got %d", len(modal.allDatabases))
	}
	if len(modal.allTables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(modal.allTables))
	}
	if len(modal.allTabs) != 1 {
		t.Errorf("Expected 1 tab, got %d", len(modal.allTabs))
	}
}

// TestFuzzySearchResults tests the filtering logic
func TestFuzzySearchResults(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	databases := []string{"test_db", "main_db"}
	schemas := []string{"public"}
	tables := map[string][]db.TableInfo{
		"public": {
			{Name: "users"},
			{Name: "products"},
		},
	}
	tabs := []Tab{
		{title: "users_data"},
	}

	modal.Refresh(nil, databases, "test_db", schemas, tables, tabs)

	// Test empty input shows all results
	if len(modal.results) != 5 {
		t.Errorf("Expected 5 results (2 databases + 2 tables + 1 tab) with empty input, got %d", len(modal.results))
	}

	// Test filtering
	modal.input = "user"
	modal.updateResults()
	if len(modal.results) != 2 {
		t.Errorf("Expected 2 results for 'user' (users table + users_data tab), got %d", len(modal.results))
	}

	// Test filtering for database
	modal.input = "test_db"
	modal.updateResults()
	if len(modal.results) != 1 {
		t.Errorf("Expected 1 result for 'test_db', got %d", len(modal.results))
	}
	if modal.results[0].Type != FuzzyDatabase {
		t.Errorf("Expected FuzzyDatabase type, got %v", modal.results[0].Type)
	}
}

// TestFuzzySearchNavigation tests selection navigation
func TestFuzzySearchNavigation(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	databases := []string{"db1", "db2"}
	modal.Refresh(nil, databases, "db1", []string{}, map[string][]db.TableInfo{}, []Tab{})

	if modal.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx to start at 0, got %d", modal.selectedIdx)
	}

	modal.SelectNext()
	if modal.selectedIdx != 1 {
		t.Errorf("Expected selectedIdx to be 1 after SelectNext(), got %d", modal.selectedIdx)
	}

	modal.SelectNext()
	if modal.selectedIdx != 1 {
		t.Errorf("Expected selectedIdx to stay at 1 (last item), got %d", modal.selectedIdx)
	}

	modal.SelectPrev()
	if modal.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx to be 0 after SelectPrev(), got %d", modal.selectedIdx)
	}

	modal.SelectPrev()
	if modal.selectedIdx != 0 {
		t.Errorf("Expected selectedIdx to stay at 0 (first item), got %d", modal.selectedIdx)
	}
}

// TestFuzzySearchGetSelected tests getting the selected result
func TestFuzzySearchGetSelected(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	databases := []string{"db1", "db2"}
	modal.Refresh(nil, databases, "db1", []string{}, map[string][]db.TableInfo{}, []Tab{})

	selected := modal.GetSelected()
	if selected == nil {
		t.Errorf("Expected non-nil selected result")
	}
	if selected.Name != "db1" {
		t.Errorf("Expected selected name to be 'db1', got %q", selected.Name)
	}
	if selected.Type != FuzzyDatabase {
		t.Errorf("Expected selected type to be FuzzyDatabase, got %v", selected.Type)
	}
}

// TestFuzzySearchCharacterInput tests adding/removing characters
func TestFuzzySearchCharacterInput(t *testing.T) {
	theme := &Theme{}
	modal := NewFuzzySearchModal(theme)

	modal.AddChar("h")
	modal.AddChar("e")
	modal.AddChar("l")

	if modal.input != "hel" {
		t.Errorf("Expected input to be 'hel', got %q", modal.input)
	}

	modal.RemoveChar()
	if modal.input != "he" {
		t.Errorf("Expected input to be 'he' after RemoveChar(), got %q", modal.input)
	}

	modal.RemoveChar()
	modal.RemoveChar()
	if modal.input != "" {
		t.Errorf("Expected empty input, got %q", modal.input)
	}

	// Should not error when removing from empty
	modal.RemoveChar()
	if modal.input != "" {
		t.Errorf("Expected empty input to stay empty")
	}
}
