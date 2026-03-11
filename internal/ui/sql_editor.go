package ui

import (
	"context"
	"strings"
	"time"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/db"
)

// ── SQL Editor Messages ───────────────────────────────────────────

// SQLExecuteRequestMsg is sent when the user requests SQL execution.
type SQLExecuteRequestMsg struct {
	Query string
}

// extractStatementAtLine extracts the SQL statement surrounding the given
// 0-based cursor line from a multi-statement SQL text. Statements are
// delimited by lines ending with ';'. Returns the trimmed statement.
func extractStatementAtLine(text string, cursorLine int) string {
	lines := strings.Split(text, "\n")
	if len(lines) == 0 {
		return ""
	}
	if cursorLine < 0 {
		cursorLine = 0
	}
	if cursorLine >= len(lines) {
		cursorLine = len(lines) - 1
	}

	// Find start: scan backward to find end of previous statement (line ending with ';')
	start := 0
	for i := cursorLine - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasSuffix(trimmed, ";") {
			start = i + 1
			break
		}
	}

	// Find end: scan forward to find end of current statement (line ending with ';')
	end := len(lines) - 1
	for i := cursorLine; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasSuffix(trimmed, ";") {
			end = i
			break
		}
	}

	stmt := strings.TrimSpace(strings.Join(lines[start:end+1], "\n"))
	// Remove trailing semicolon for execution
	stmt = strings.TrimRight(stmt, ";")
	return strings.TrimSpace(stmt)
}

// SQLExecuteResultMsg holds the result of SQL execution.
type SQLExecuteResultMsg struct {
	Query        string
	Result       *db.QueryResult
	RowsAffected int64
	IsQuery      bool // true for SELECT-like queries
	Err          error
}

// ── SQL Editor Commands ───────────────────────────────────────────

// ExecuteSQLCmd executes a SQL query against the database.
func ExecuteSQLCmd(driver db.Driver, query string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		trimmed := strings.TrimSpace(query)
		upper := strings.ToUpper(trimmed)

		// Determine if this is a query (returns results) or a statement
		isQuery := strings.HasPrefix(upper, "SELECT") ||
			strings.HasPrefix(upper, "SHOW") ||
			strings.HasPrefix(upper, "DESCRIBE") ||
			strings.HasPrefix(upper, "EXPLAIN") ||
			strings.HasPrefix(upper, "PRAGMA") ||
			strings.HasPrefix(upper, "WITH")

		if isQuery {
			result, err := driver.Query(ctx, query)
			return SQLExecuteResultMsg{
				Query:   query,
				Result:  result,
				IsQuery: true,
				Err:     err,
			}
		}

		rowsAffected, err := driver.Exec(ctx, query)
		return SQLExecuteResultMsg{
			Query:        query,
			RowsAffected: rowsAffected,
			IsQuery:      false,
			Err:          err,
		}
	}
}

// ── SQL Keywords ──────────────────────────────────────────────────

var sqlKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true,
	"UPDATE": true, "DELETE": true, "CREATE": true, "ALTER": true,
	"DROP": true, "JOIN": true, "INNER": true, "LEFT": true,
	"RIGHT": true, "OUTER": true, "FULL": true, "CROSS": true,
	"ON": true, "AND": true, "OR": true, "NOT": true,
	"IN": true, "LIKE": true, "BETWEEN": true, "ORDER": true,
	"BY": true, "GROUP": true, "HAVING": true, "LIMIT": true,
	"OFFSET": true, "SET": true, "VALUES": true, "INTO": true,
	"TABLE": true, "INDEX": true, "NULL": true, "AS": true,
	"IS": true, "EXISTS": true, "DISTINCT": true, "UNION": true,
	"ALL": true, "ANY": true, "CASE": true, "WHEN": true,
	"THEN": true, "ELSE": true, "END": true, "BEGIN": true,
	"COMMIT": true, "ROLLBACK": true, "TRANSACTION": true,
	"ASC": true, "DESC": true, "COUNT": true, "SUM": true,
	"AVG": true, "MIN": true, "MAX": true, "CAST": true,
	"COALESCE": true, "IF": true, "PRIMARY": true, "KEY": true,
	"FOREIGN": true, "REFERENCES": true, "CONSTRAINT": true,
	"DEFAULT": true, "CHECK": true, "UNIQUE": true, "ADD": true,
	"COLUMN": true, "WITH": true, "RECURSIVE": true, "OVER": true,
	"PARTITION": true, "ROW": true, "ROWS": true, "FETCH": true,
	"NEXT": true, "ONLY": true, "FIRST": true, "LAST": true,
	"TRUE": true, "FALSE": true, "SHOW": true, "DESCRIBE": true,
	"EXPLAIN": true, "PRAGMA": true, "REPLACE": true, "TRUNCATE": true,
	"VIEW": true, "TRIGGER": true, "PROCEDURE": true, "FUNCTION": true,
	"RETURNING": true, "EXCEPT": true, "INTERSECT": true,
	"USING": true, "NATURAL": true, "WINDOW": true, "LATERAL": true,
}

// ── Token types for syntax highlighting ───────────────────────────

type sqlTokenType int

const (
	tokenNormal sqlTokenType = iota
	tokenKeyword
	tokenString
	tokenNumber
	tokenComment
)

type sqlToken struct {
	text string
	typ  sqlTokenType
}

// tokenizeSQL splits a line into syntax tokens.
func tokenizeSQL(line string) []sqlToken {
	var tokens []sqlToken
	runes := []rune(line)
	i := 0

	for i < len(runes) {
		ch := runes[i]

		// Comment: -- until end of line
		if ch == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			tokens = append(tokens, sqlToken{text: string(runes[i:]), typ: tokenComment})
			return tokens
		}

		// String literal: 'single-quoted'
		if ch == '\'' {
			j := i + 1
			for j < len(runes) {
				if runes[j] == '\'' {
					if j+1 < len(runes) && runes[j+1] == '\'' {
						j += 2 // escaped quote
						continue
					}
					j++
					break
				}
				j++
			}
			tokens = append(tokens, sqlToken{text: string(runes[i:j]), typ: tokenString})
			i = j
			continue
		}

		// Number literal
		if unicode.IsDigit(ch) || (ch == '.' && i+1 < len(runes) && unicode.IsDigit(runes[i+1])) {
			// Check it's not part of a word
			if i > 0 && (unicode.IsLetter(runes[i-1]) || runes[i-1] == '_') {
				// Part of identifier
				j := i
				for j < len(runes) && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
					j++
				}
				tokens = append(tokens, sqlToken{text: string(runes[i:j]), typ: tokenNormal})
				i = j
				continue
			}
			j := i
			hasDot := false
			for j < len(runes) && (unicode.IsDigit(runes[j]) || (runes[j] == '.' && !hasDot)) {
				if runes[j] == '.' {
					hasDot = true
				}
				j++
			}
			tokens = append(tokens, sqlToken{text: string(runes[i:j]), typ: tokenNumber})
			i = j
			continue
		}

		// Word (keyword or identifier)
		if unicode.IsLetter(ch) || ch == '_' {
			j := i
			for j < len(runes) && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
				j++
			}
			word := string(runes[i:j])
			if sqlKeywords[strings.ToUpper(word)] {
				tokens = append(tokens, sqlToken{text: word, typ: tokenKeyword})
			} else {
				tokens = append(tokens, sqlToken{text: word, typ: tokenNormal})
			}
			i = j
			continue
		}

		// Normal character (operators, whitespace, etc.)
		tokens = append(tokens, sqlToken{text: string(ch), typ: tokenNormal})
		i++
	}

	return tokens
}

// ── SQLEditorModel ────────────────────────────────────────────────

// SQLEditorModel manages the SQL editor pane with multi-line support.
type SQLEditorModel struct {
	lines     []string // lines of text
	cursorRow int      // cursor line
	cursorCol int      // cursor column

	focused bool
	height  int
	width   int
	theme   *Theme

	scrollOffset int

	// Query history
	history      *QueryHistory
	historyIndex int // -1 means not browsing history
	savedInput   []string // saved current input when browsing history

	// History search mode
	historySearchMode   bool
	historySearchQuery  string
	historySearchResult []int // indices into history.Queries that match

	// Execution state
	executing bool

	// Autocomplete state
	completionActive  bool
	completionItems   []string
	completionIndex   int
	completionPrefix  string

	// Cached table/column names for autocomplete
	cachedTableNames  []string
	cachedColumnNames map[string][]string // table -> columns
}

// NewSQLEditorModel creates a new SQLEditorModel.
func NewSQLEditorModel(theme *Theme) SQLEditorModel {
	return SQLEditorModel{
		lines:             []string{""},
		height:            3,
		theme:             theme,
		history:           LoadHistory(),
		historyIndex:      -1,
		cachedColumnNames: make(map[string][]string),
	}
}

// UpdateTableCache updates the cached table names from the loaded schemas.
func (m *SQLEditorModel) UpdateTableCache(schemas []string, tables map[string][]db.TableInfo) {
	m.cachedTableNames = nil
	for _, s := range schemas {
		for _, t := range tables[s] {
			m.cachedTableNames = append(m.cachedTableNames, t.Name)
		}
	}
}

// UpdateColumnCache updates the cached column names from a table description.
func (m *SQLEditorModel) UpdateColumnCache(desc *db.TableDescription) {
	if desc == nil {
		return
	}
	key := desc.Name
	cols := make([]string, len(desc.Columns))
	for i, c := range desc.Columns {
		cols[i] = c.Name
	}
	m.cachedColumnNames[key] = cols
}

// SetFocused sets whether this pane is focused.
func (m *SQLEditorModel) SetFocused(f bool) {
	m.focused = f
}

// SetSize updates the pane size.
func (m *SQLEditorModel) SetSize(w, h int) {
	m.width = w
	m.height = h
}

// GetInput returns the full SQL input as a single string.
func (m SQLEditorModel) GetInput() string {
	return strings.Join(m.lines, "\n")
}

// SetInput sets the editor content.
func (m *SQLEditorModel) SetInput(s string) {
	m.lines = strings.Split(s, "\n")
	if len(m.lines) == 0 {
		m.lines = []string{""}
	}
	m.cursorRow = len(m.lines) - 1
	m.cursorCol = len(m.lines[m.cursorRow])
}

// SetContent replaces the editor content and resets cursor to start.
func (m *SQLEditorModel) SetContent(s string) {
	m.lines = strings.Split(s, "\n")
	if len(m.lines) == 0 {
		m.lines = []string{""}
	}
	// Remove trailing empty line from file read
	if len(m.lines) > 1 && m.lines[len(m.lines)-1] == "" {
		m.lines = m.lines[:len(m.lines)-1]
	}
	m.cursorRow = 0
	m.cursorCol = 0
	m.scrollOffset = 0
	m.completionActive = false
}
func (m *SQLEditorModel) clampCursor() {
	if m.cursorRow < 0 {
		m.cursorRow = 0
	}
	if m.cursorRow >= len(m.lines) {
		m.cursorRow = len(m.lines) - 1
	}
	lineLen := len([]rune(m.lines[m.cursorRow]))
	if m.cursorCol > lineLen {
		m.cursorCol = lineLen
	}
	if m.cursorCol < 0 {
		m.cursorCol = 0
	}
}

// adjustScroll adjusts scroll offset so cursor line is visible.
func (m *SQLEditorModel) adjustScroll() {
	visibleLines := m.height - 2 // borders
	if visibleLines < 1 {
		visibleLines = 1
	}
	if m.cursorRow < m.scrollOffset {
		m.scrollOffset = m.cursorRow
	}
	if m.cursorRow >= m.scrollOffset+visibleLines {
		m.scrollOffset = m.cursorRow - visibleLines + 1
	}
}

// Update handles key input for the SQL editor.
func (m SQLEditorModel) Update(msg tea.Msg) (SQLEditorModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyInput(msg)
	}
	return m, nil
}

func (m SQLEditorModel) handleKeyInput(msg tea.KeyMsg) (SQLEditorModel, tea.Cmd) {
	key := msg.String()

	// History search mode
	if m.historySearchMode {
		return m.handleHistorySearch(msg)
	}

	// Autocomplete mode
	if m.completionActive {
		switch key {
		case "tab":
			// Accept completion
			if m.completionIndex < len(m.completionItems) {
				m.insertCompletion(m.completionItems[m.completionIndex])
			}
			m.completionActive = false
			return m, nil
		case "down":
			if m.completionIndex < len(m.completionItems)-1 {
				m.completionIndex++
			}
			return m, nil
		case "up":
			if m.completionIndex > 0 {
				m.completionIndex--
			}
			return m, nil
		case "esc":
			m.completionActive = false
			return m, nil
		case "enter":
			// Accept completion (don't execute SQL)
			if m.completionIndex < len(m.completionItems) {
				m.insertCompletion(m.completionItems[m.completionIndex])
			}
			m.completionActive = false
			return m, nil
		case " ":
			// Space closes completion and inserts space
			m.completionActive = false
		default:
			// Other keys: fall through to normal handling (tryAutoComplete will update)
		}
	}

	switch key {
	case "enter":
		// If input ends with ";", execute SQL. Otherwise insert newline.
		input := strings.TrimSpace(m.GetInput())
		if strings.HasSuffix(input, ";") && input != "" && !m.executing {
			m.executing = true
			m.history.Add(input)
			m.history.Save()
			m.historyIndex = -1
			m.completionActive = false
			return m, func() tea.Msg {
				return SQLExecuteRequestMsg{Query: input}
			}
		}
		// Insert newline
		runes := []rune(m.lines[m.cursorRow])
		before := string(runes[:m.cursorCol])
		after := string(runes[m.cursorCol:])
		m.lines[m.cursorRow] = before
		newLines := make([]string, len(m.lines)+1)
		copy(newLines, m.lines[:m.cursorRow+1])
		newLines[m.cursorRow+1] = after
		copy(newLines[m.cursorRow+2:], m.lines[m.cursorRow+1:])
		m.lines = newLines
		m.cursorRow++
		m.cursorCol = 0
		m.adjustScroll()
		return m, nil

	case "ctrl+r":
		// Enter history search mode
		m.historySearchMode = true
		m.historySearchQuery = ""
		m.historySearchResult = nil
		m.updateHistorySearch()
		return m, nil

	case "ctrl+up":
		// Navigate history backward
		if m.history.Len() == 0 {
			return m, nil
		}
		if m.historyIndex == -1 {
			// Save current input
			m.savedInput = make([]string, len(m.lines))
			copy(m.savedInput, m.lines)
			m.historyIndex = m.history.Len() - 1
		} else if m.historyIndex > 0 {
			m.historyIndex--
		}
		query := m.history.Get(m.historyIndex)
		m.lines = strings.Split(query, "\n")
		m.cursorRow = len(m.lines) - 1
		m.cursorCol = len([]rune(m.lines[m.cursorRow]))
		m.adjustScroll()
		return m, nil

	case "ctrl+down":
		// Navigate history forward
		if m.historyIndex == -1 {
			return m, nil
		}
		m.historyIndex++
		if m.historyIndex >= m.history.Len() {
			// Restore saved input
			m.historyIndex = -1
			if m.savedInput != nil {
				m.lines = m.savedInput
				m.savedInput = nil
			} else {
				m.lines = []string{""}
			}
		} else {
			query := m.history.Get(m.historyIndex)
			m.lines = strings.Split(query, "\n")
		}
		m.cursorRow = len(m.lines) - 1
		m.cursorCol = len([]rune(m.lines[m.cursorRow]))
		m.adjustScroll()
		return m, nil

	case "backspace":
		if m.cursorCol > 0 {
			runes := []rune(m.lines[m.cursorRow])
			m.lines[m.cursorRow] = string(runes[:m.cursorCol-1]) + string(runes[m.cursorCol:])
			m.cursorCol--
		} else if m.cursorRow > 0 {
			// Merge with previous line
			prevLen := len([]rune(m.lines[m.cursorRow-1]))
			m.lines[m.cursorRow-1] += m.lines[m.cursorRow]
			m.lines = append(m.lines[:m.cursorRow], m.lines[m.cursorRow+1:]...)
			m.cursorRow--
			m.cursorCol = prevLen
			m.adjustScroll()
		}
		m.tryAutoComplete()
		return m, nil

	case "delete":
		runes := []rune(m.lines[m.cursorRow])
		if m.cursorCol < len(runes) {
			m.lines[m.cursorRow] = string(runes[:m.cursorCol]) + string(runes[m.cursorCol+1:])
		} else if m.cursorRow < len(m.lines)-1 {
			// Merge with next line
			m.lines[m.cursorRow] += m.lines[m.cursorRow+1]
			m.lines = append(m.lines[:m.cursorRow+1], m.lines[m.cursorRow+2:]...)
		}
		return m, nil

	case "left":
		m.completionActive = false
		if m.cursorCol > 0 {
			m.cursorCol--
		} else if m.cursorRow > 0 {
			m.cursorRow--
			m.cursorCol = len([]rune(m.lines[m.cursorRow]))
			m.adjustScroll()
		}
		return m, nil

	case "right":
		m.completionActive = false
		lineLen := len([]rune(m.lines[m.cursorRow]))
		if m.cursorCol < lineLen {
			m.cursorCol++
		} else if m.cursorRow < len(m.lines)-1 {
			m.cursorRow++
			m.cursorCol = 0
			m.adjustScroll()
		}
		return m, nil

	case "up":
		m.completionActive = false
		if m.cursorRow > 0 {
			m.cursorRow--
			m.clampCursor()
			m.adjustScroll()
		}
		return m, nil

	case "down":
		m.completionActive = false
		if m.cursorRow < len(m.lines)-1 {
			m.cursorRow++
			m.clampCursor()
			m.adjustScroll()
		}
		return m, nil

	case "home":
		m.cursorCol = 0
		return m, nil

	case "end":
		m.cursorCol = len([]rune(m.lines[m.cursorRow]))
		return m, nil

	case "ctrl+a":
		m.cursorCol = 0
		return m, nil

	case "ctrl+k":
		// Delete from cursor to end of line
		runes := []rune(m.lines[m.cursorRow])
		m.lines[m.cursorRow] = string(runes[:m.cursorCol])
		return m, nil

	case "tab":
		// Accept current completion or trigger autocomplete
		if m.completionActive && len(m.completionItems) > 0 {
			m.insertCompletion(m.completionItems[m.completionIndex])
			m.completionActive = false
			return m, nil
		}
		prefix := m.getWordBeforeCursor()
		if prefix != "" {
			candidates := m.findCompletions(prefix)
			if len(candidates) == 1 {
				m.insertCompletion(candidates[0])
				m.completionActive = false
				return m, nil
			} else if len(candidates) > 0 {
				m.completionActive = true
				m.completionItems = candidates
				m.completionIndex = 0
				m.completionPrefix = prefix
				return m, nil
			}
		}
		// No completions: insert tab as spaces
		runes := []rune(m.lines[m.cursorRow])
		m.lines[m.cursorRow] = string(runes[:m.cursorCol]) + "  " + string(runes[m.cursorCol:])
		m.cursorCol += 2
		return m, nil

	default:
		if msg.Type == tea.KeyRunes {
			// Insert characters
			runes := []rune(m.lines[m.cursorRow])
			insertRunes := msg.Runes
			newLine := string(runes[:m.cursorCol]) + string(insertRunes) + string(runes[m.cursorCol:])
			m.lines[m.cursorRow] = newLine
			m.cursorCol += len(insertRunes)
			// Auto-trigger completion after typing
			m.tryAutoComplete()
			return m, nil
		}
	}

	return m, nil
}

// handleHistorySearch handles key input in history search mode (Ctrl+R).
func (m SQLEditorModel) handleHistorySearch(msg tea.KeyMsg) (SQLEditorModel, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.historySearchMode = false
		return m, nil

	case "enter":
		// Select the current match
		m.historySearchMode = false
		if len(m.historySearchResult) > 0 {
			idx := m.historySearchResult[0]
			query := m.history.Get(idx)
			m.lines = strings.Split(query, "\n")
			m.cursorRow = len(m.lines) - 1
			m.cursorCol = len([]rune(m.lines[m.cursorRow]))
			m.adjustScroll()
		}
		return m, nil

	case "ctrl+r":
		// Cycle to next match
		if len(m.historySearchResult) > 1 {
			m.historySearchResult = m.historySearchResult[1:]
		}
		return m, nil

	case "backspace":
		if len(m.historySearchQuery) > 0 {
			m.historySearchQuery = m.historySearchQuery[:len(m.historySearchQuery)-1]
			m.updateHistorySearch()
		}
		return m, nil

	default:
		if msg.Type == tea.KeyRunes {
			m.historySearchQuery += string(msg.Runes)
			m.updateHistorySearch()
		}
		return m, nil
	}
}

// updateHistorySearch updates the search results based on the current query.
func (m *SQLEditorModel) updateHistorySearch() {
	m.historySearchResult = nil
	if m.historySearchQuery == "" {
		// Show all history entries (most recent first)
		for i := m.history.Len() - 1; i >= 0; i-- {
			m.historySearchResult = append(m.historySearchResult, i)
		}
		return
	}
	query := strings.ToLower(m.historySearchQuery)
	for i := m.history.Len() - 1; i >= 0; i-- {
		if strings.Contains(strings.ToLower(m.history.Get(i)), query) {
			m.historySearchResult = append(m.historySearchResult, i)
		}
	}
}

// getWordBeforeCursor returns the word (identifier) before the cursor.
func (m SQLEditorModel) getWordBeforeCursor() string {
	if m.cursorRow >= len(m.lines) {
		return ""
	}
	line := m.lines[m.cursorRow]
	runes := []rune(line)
	if m.cursorCol > len(runes) {
		return ""
	}

	// Walk backward from cursor
	end := m.cursorCol
	start := end
	for start > 0 {
		ch := runes[start-1]
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' {
			start--
		} else {
			break
		}
	}
	if start == end {
		return ""
	}
	return string(runes[start:end])
}

// tryAutoComplete triggers auto-completion if the current word prefix is long enough.
func (m *SQLEditorModel) tryAutoComplete() {
	prefix := m.getWordBeforeCursor()
	if len([]rune(prefix)) >= 2 {
		candidates := m.findCompletions(prefix)
		if len(candidates) > 0 {
			m.completionActive = true
			m.completionItems = candidates
			m.completionIndex = 0
			m.completionPrefix = prefix
			return
		}
	}
	m.completionActive = false
}

// findCompletions returns matching table/column names for the given prefix.
func (m SQLEditorModel) findCompletions(prefix string) []string {
	prefixLower := strings.ToLower(prefix)
	seen := make(map[string]bool)
	var candidates []string

	// Search table names
	for _, name := range m.cachedTableNames {
		if strings.HasPrefix(strings.ToLower(name), prefixLower) && !seen[name] {
			candidates = append(candidates, name)
			seen[name] = true
		}
	}

	// Search column names
	for _, cols := range m.cachedColumnNames {
		for _, name := range cols {
			if strings.HasPrefix(strings.ToLower(name), prefixLower) && !seen[name] {
				candidates = append(candidates, name)
				seen[name] = true
			}
		}
	}

	// Search SQL keywords (exclude short ones like ON, AS, BY, IF, IN, IS, OR)
	for kw := range sqlKeywords {
		if len(kw) >= 3 && strings.HasPrefix(strings.ToLower(kw), prefixLower) && !seen[kw] {
			candidates = append(candidates, kw)
			seen[kw] = true
		}
	}

	return candidates
}

// FindCompletions returns completion candidates for a given prefix.
// This is used by the nvim pane overlay to show suggestions.
func (m *SQLEditorModel) FindCompletions(prefix string) ([]string, string) {
	candidates := m.findCompletions(prefix)
	// Filter out very short candidates (like "ms", "id") to reduce noise
	filtered := candidates[:0]
	for _, c := range candidates {
		if len(c) >= 3 {
			filtered = append(filtered, c)
		}
	}
	return filtered, prefix
}

// insertCompletion inserts the completion, replacing the current prefix.
func (m *SQLEditorModel) insertCompletion(completion string) {
	prefix := m.getWordBeforeCursor()
	if prefix == "" {
		return
	}
	runes := []rune(m.lines[m.cursorRow])
	start := m.cursorCol - len([]rune(prefix))
	m.lines[m.cursorRow] = string(runes[:start]) + completion + string(runes[m.cursorCol:])
	m.cursorCol = start + len([]rune(completion))
}

// renderHighlightedLine renders a single line with syntax highlighting.
func (m SQLEditorModel) renderHighlightedLine(line string, isCursorLine bool) string {
	tokens := tokenizeSQL(line)

	var parts []string
	pos := 0

	for _, tok := range tokens {
		tokRunes := []rune(tok.text)
		var styledText string

		switch tok.typ {
		case tokenKeyword:
			styledText = m.theme.SQLKeyword.Render(tok.text)
		case tokenString:
			styledText = m.theme.SQLString.Render(tok.text)
		case tokenNumber:
			styledText = m.theme.SQLNumber.Render(tok.text)
		case tokenComment:
			styledText = m.theme.SQLComment.Render(tok.text)
		default:
			styledText = m.theme.SQLInput.Render(tok.text)
		}

		// If cursor is in this token and this is the cursor line, we need to insert cursor
		if isCursorLine && m.focused && m.cursorCol >= pos && m.cursorCol <= pos+len(tokRunes) {
			// Split token at cursor position
			cursorInTok := m.cursorCol - pos
			beforeCursor := string(tokRunes[:cursorInTok])
			afterCursor := string(tokRunes[cursorInTok:])

			var style lipgloss.Style
			switch tok.typ {
			case tokenKeyword:
				style = m.theme.SQLKeyword
			case tokenString:
				style = m.theme.SQLString
			case tokenNumber:
				style = m.theme.SQLNumber
			case tokenComment:
				style = m.theme.SQLComment
			default:
				style = m.theme.SQLInput
			}

			if cursorInTok < len(tokRunes) {
				cursorChar := string(tokRunes[cursorInTok])
				afterCursor = string(tokRunes[cursorInTok+1:])
				styledText = style.Render(beforeCursor) +
					m.theme.SQLCursor.Render(cursorChar) +
					style.Render(afterCursor)
			} else {
				styledText = style.Render(beforeCursor) +
					m.theme.SQLCursor.Render(" ") +
					style.Render(afterCursor)
			}
		}

		parts = append(parts, styledText)
		pos += len(tokRunes)
	}

	// If cursor is at end of line and this is the cursor line
	if isCursorLine && m.focused && m.cursorCol >= pos {
		parts = append(parts, m.theme.SQLCursor.Render(" "))
	}

	if len(parts) == 0 {
		if isCursorLine && m.focused {
			return m.theme.SQLCursor.Render(" ")
		}
		return ""
	}

	return strings.Join(parts, "")
}

// View renders the SQL editor pane.
func (m SQLEditorModel) View() string {
	var sb strings.Builder

	visibleLines := m.height - 2 // borders
	if visibleLines < 1 {
		visibleLines = 1
	}

	start := m.scrollOffset
	end := start + visibleLines
	if end > len(m.lines) {
		end = len(m.lines)
	}

	for i := start; i < end; i++ {
		isCursorLine := (i == m.cursorRow)
		var linePrefix string
		if i == 0 {
			linePrefix = m.theme.SQLPrompt.Render("SQL> ")
		} else {
			linePrefix = m.theme.SQLPrompt.Render("...  ")
		}

		highlightedLine := m.renderHighlightedLine(m.lines[i], isCursorLine)
		sb.WriteString(linePrefix + highlightedLine)
		if i < end-1 {
			sb.WriteString("\n")
		}
	}

	// Fill remaining lines
	for i := end - start; i < visibleLines; i++ {
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(m.theme.SQLPrompt.Render("     "))
	}

	// Loading indicator
	if m.executing {
		sb.WriteString("\n")
		sb.WriteString(m.theme.SQLComment.Render(" ⏳ Executing..."))
	}

	// Autocomplete dropdown
	if m.completionActive && len(m.completionItems) > 0 {
		sb.WriteString("\n")
		maxShow := 5
		if len(m.completionItems) < maxShow {
			maxShow = len(m.completionItems)
		}
		startIdx := 0
		if m.completionIndex >= maxShow {
			startIdx = m.completionIndex - maxShow + 1
		}
		for i := startIdx; i < startIdx+maxShow && i < len(m.completionItems); i++ {
			item := m.completionItems[i]
			if i == m.completionIndex {
				sb.WriteString(m.theme.DataCellActive.Render(" ▸ " + item + " "))
			} else {
				sb.WriteString(m.theme.DataRow.Render("   " + item + " "))
			}
			sb.WriteString("\n")
		}
	}

	// History search display
	if m.historySearchMode {
		sb.WriteString("\n")
		sb.WriteString(m.theme.SQLPrompt.Render("(reverse-i-search) "))
		sb.WriteString(m.theme.SQLInput.Render(m.historySearchQuery + "▏"))
		if len(m.historySearchResult) > 0 {
			preview := m.history.Get(m.historySearchResult[0])
			if len(preview) > 60 {
				preview = preview[:60] + "..."
			}
			sb.WriteString(" → ")
			sb.WriteString(m.theme.SQLComment.Render(preview))
		}
	}

	// Apply border
	borderStyle := m.theme.Border
	if m.focused {
		borderStyle = m.theme.FocusedBorder
	}

	return borderStyle.
		Width(m.width - 2).
		Height(m.height - 2).
		Render(sb.String())
}

