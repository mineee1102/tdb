package ui

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/minee/tdb/internal/config"
	"gopkg.in/yaml.v3"
)

const maxHistoryEntries = 100

// QueryHistory manages SQL query history.
type QueryHistory struct {
	Queries []string `yaml:"queries"`
}

// historyFilePath returns the full path to the history file.
func historyFilePath() string {
	return filepath.Join(config.ConfigDir(), "history.yaml")
}

// LoadHistory reads query history from disk.
func LoadHistory() *QueryHistory {
	h := &QueryHistory{}
	data, err := os.ReadFile(historyFilePath())
	if err != nil {
		return h
	}
	_ = yaml.Unmarshal(data, h)
	return h
}

// Save writes query history to disk.
func (h *QueryHistory) Save() {
	dir := config.ConfigDir()
	_ = os.MkdirAll(dir, 0o700)

	data, err := yaml.Marshal(h)
	if err != nil {
		return
	}
	_ = os.WriteFile(historyFilePath(), data, 0o600)
}

// Add appends a query to history (deduplicating consecutive duplicates).
func (h *QueryHistory) Add(query string) {
	if query == "" {
		return
	}
	// Don't add if it's the same as the last entry
	if len(h.Queries) > 0 && h.Queries[len(h.Queries)-1] == query {
		return
	}
	h.Queries = append(h.Queries, query)
	// Trim to max size
	if len(h.Queries) > maxHistoryEntries {
		h.Queries = h.Queries[len(h.Queries)-maxHistoryEntries:]
	}
}

// Get returns the query at the given index (0 = oldest).
func (h *QueryHistory) Get(index int) string {
	if index < 0 || index >= len(h.Queries) {
		return ""
	}
	return h.Queries[index]
}

// Len returns the number of history entries.
func (h *QueryHistory) Len() int {
	return len(h.Queries)
}

// Search returns indices of queries matching the search string (case-insensitive).
// Results are ordered from most recent to oldest.
func (h *QueryHistory) Search(query string) []int {
	if query == "" {
		// Return all indices in reverse order
		result := make([]int, h.Len())
		for i := range result {
			result[i] = h.Len() - 1 - i
		}
		return result
	}

	lower := strings.ToLower(query)
	var result []int
	for i := h.Len() - 1; i >= 0; i-- {
		if strings.Contains(strings.ToLower(h.Queries[i]), lower) {
			result = append(result, i)
		}
	}
	return result
}
