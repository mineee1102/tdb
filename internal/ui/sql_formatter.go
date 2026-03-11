package ui

import (
	"strings"
	"unicode"
)

// formatSQL formats a SQL string with proper keyword casing and line breaks.
func formatSQL(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return input
	}

	// Process each statement separated by semicolons independently
	statements := splitStatements(input)
	var formatted []string
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		formatted = append(formatted, formatSingleStatement(stmt))
	}
	return strings.Join(formatted, ";\n\n") + "\n"
}

// splitStatements splits SQL text by semicolons, respecting quotes.
func splitStatements(input string) []string {
	var stmts []string
	var current strings.Builder
	inSingle := false
	inDouble := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			current.WriteByte(ch)
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
			current.WriteByte(ch)
		} else if ch == ';' && !inSingle && !inDouble {
			stmts = append(stmts, current.String())
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// Major clause keywords that start a new line (no indent).
var majorClauses = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true,
	"ORDER": true, "GROUP": true, "HAVING": true, "LIMIT": true,
	"OFFSET": true, "UNION": true, "EXCEPT": true, "INTERSECT": true,
	"INSERT": true, "UPDATE": true, "DELETE": true, "SET": true,
	"VALUES": true, "INTO": true, "CREATE": true, "ALTER": true,
	"DROP": true, "TRUNCATE": true, "WITH": true,
}

// Join keywords that start a new line (no indent).
var joinKeywords = map[string]bool{
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true,
	"OUTER": true, "CROSS": true, "FULL": true, "NATURAL": true,
}

// Sub-clause keywords indented under their parent clause.
var subClauses = map[string]bool{
	"AND": true, "OR": true, "ON": true,
}

// All SQL keywords to uppercase.
var allKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true,
	"NOT": true, "IN": true, "IS": true, "NULL": true, "AS": true,
	"ON": true, "JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true,
	"OUTER": true, "CROSS": true, "FULL": true, "NATURAL": true,
	"ORDER": true, "BY": true, "GROUP": true, "HAVING": true,
	"LIMIT": true, "OFFSET": true, "UNION": true, "ALL": true,
	"DISTINCT": true, "BETWEEN": true, "LIKE": true, "EXISTS": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"ASC": true, "DESC": true, "INSERT": true, "INTO": true,
	"VALUES": true, "UPDATE": true, "SET": true, "DELETE": true,
	"CREATE": true, "TABLE": true, "ALTER": true, "DROP": true,
	"INDEX": true, "VIEW": true, "IF": true, "CASCADE": true,
	"CONSTRAINT": true, "PRIMARY": true, "KEY": true, "FOREIGN": true,
	"REFERENCES": true, "UNIQUE": true, "DEFAULT": true, "CHECK": true,
	"TRUNCATE": true, "WITH": true, "RECURSIVE": true,
	"TRUE": true, "FALSE": true, "EXCEPT": true, "INTERSECT": true,
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	"COALESCE": true, "CAST": true, "OVER": true, "PARTITION": true,
	"ROW_NUMBER": true, "RANK": true, "DENSE_RANK": true,
	"LAG": true, "LEAD": true, "FIRST_VALUE": true, "LAST_VALUE": true,
	"ROWS": true, "RANGE": true, "UNBOUNDED": true, "PRECEDING": true,
	"FOLLOWING": true, "CURRENT": true, "ROW": true,
}

// tokenize splits SQL into tokens preserving strings and identifiers.
func tokenize(sql string) []token {
	var tokens []token
	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Skip whitespace, record it
		if unicode.IsSpace(rune(ch)) {
			j := i
			for j < len(sql) && unicode.IsSpace(rune(sql[j])) {
				j++
			}
			tokens = append(tokens, token{typ: tokSpace, val: sql[i:j]})
			i = j
			continue
		}

		// Single-quoted string
		if ch == '\'' {
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			tokens = append(tokens, token{typ: tokString, val: sql[i:j]})
			i = j
			continue
		}

		// Double-quoted identifier
		if ch == '"' {
			j := i + 1
			for j < len(sql) && sql[j] != '"' {
				j++
			}
			if j < len(sql) {
				j++
			}
			tokens = append(tokens, token{typ: tokIdent, val: sql[i:j]})
			i = j
			continue
		}

		// Backtick-quoted identifier
		if ch == '`' {
			j := i + 1
			for j < len(sql) && sql[j] != '`' {
				j++
			}
			if j < len(sql) {
				j++
			}
			tokens = append(tokens, token{typ: tokIdent, val: sql[i:j]})
			i = j
			continue
		}

		// Line comment
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			j := i
			for j < len(sql) && sql[j] != '\n' {
				j++
			}
			tokens = append(tokens, token{typ: tokComment, val: sql[i:j]})
			i = j
			continue
		}

		// Block comment
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			j := i + 2
			for j+1 < len(sql) && !(sql[j] == '*' && sql[j+1] == '/') {
				j++
			}
			if j+1 < len(sql) {
				j += 2
			}
			tokens = append(tokens, token{typ: tokComment, val: sql[i:j]})
			i = j
			continue
		}

		// Parentheses
		if ch == '(' {
			tokens = append(tokens, token{typ: tokOpen, val: "("})
			i++
			continue
		}
		if ch == ')' {
			tokens = append(tokens, token{typ: tokClose, val: ")"})
			i++
			continue
		}

		// Comma
		if ch == ',' {
			tokens = append(tokens, token{typ: tokComma, val: ","})
			i++
			continue
		}

		// Operators and punctuation
		if ch == '.' || ch == '*' || ch == '+' || ch == '-' || ch == '/' ||
			ch == '%' || ch == '=' || ch == '<' || ch == '>' || ch == '!' ||
			ch == '|' || ch == '&' || ch == '^' || ch == '~' || ch == ':' || ch == '@' {
			j := i + 1
			// Multi-char operators
			for j < len(sql) && (sql[j] == '=' || sql[j] == '>' || sql[j] == '<' || sql[j] == '|') {
				j++
				if j-i > 2 {
					break
				}
			}
			tokens = append(tokens, token{typ: tokOp, val: sql[i:j]})
			i = j
			continue
		}

		// Word (keyword or identifier)
		if isWordChar(ch) {
			j := i
			for j < len(sql) && isWordChar(sql[j]) {
				j++
			}
			tokens = append(tokens, token{typ: tokWord, val: sql[i:j]})
			i = j
			continue
		}

		// Anything else
		tokens = append(tokens, token{typ: tokOp, val: string(ch)})
		i++
	}
	return tokens
}

type tokenType int

const (
	tokSpace tokenType = iota
	tokWord
	tokString
	tokIdent
	tokComment
	tokOpen
	tokClose
	tokComma
	tokOp
)

type token struct {
	typ tokenType
	val string
}

func isWordChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') || ch == '_'
}

func formatSingleStatement(sql string) string {
	tokens := tokenize(strings.TrimSpace(sql))
	if len(tokens) == 0 {
		return sql
	}

	var out strings.Builder
	indent := 0
	parenDepth := 0
	needNewline := false
	needIndent := false
	prevWord := ""
	lineStart := true

	writeNewline := func(extraIndent int) {
		out.WriteByte('\n')
		for i := 0; i < indent+extraIndent; i++ {
			out.WriteString("  ")
		}
		lineStart = true
	}

	for i, tok := range tokens {
		switch tok.typ {
		case tokSpace:
			if !lineStart {
				out.WriteByte(' ')
			}
			continue

		case tokComment:
			if !lineStart {
				out.WriteByte(' ')
			}
			out.WriteString(tok.val)
			writeNewline(0)
			continue

		case tokWord:
			upper := strings.ToUpper(tok.val)
			word := tok.val
			if allKeywords[upper] {
				word = upper
			}

			// Check for multi-word clauses: ORDER BY, GROUP BY, INSERT INTO, etc.
			isJoinStart := joinKeywords[upper] && upper != "JOIN"
			isJoinWord := upper == "JOIN"

			if isJoinWord && parenDepth == 0 {
				// If preceded by LEFT/RIGHT/etc., they already printed on a new line
				if joinKeywords[prevWord] {
					// Just continue on same line
				} else {
					// Plain JOIN
					writeNewline(0)
					lineStart = false
				}
				needNewline = false
				needIndent = false
			} else if isJoinStart && parenDepth == 0 && lookAheadForJoin(tokens, i) {
				writeNewline(0)
				lineStart = false
				needNewline = false
				needIndent = false
			}

			if majorClauses[upper] && parenDepth == 0 && !isJoinStart && !isJoinWord {
				// Skip newline for the very first token
				if i > 0 && !isFirstNonSpace(tokens, i) {
					// INTO after INSERT stays on same line
					if upper == "INTO" && prevWord == "INSERT" {
						// stay on same line
					} else if upper == "BY" && (prevWord == "ORDER" || prevWord == "GROUP" || prevWord == "PARTITION") {
						// stay on same line
					} else if upper == "ALL" && (prevWord == "UNION" || prevWord == "EXCEPT" || prevWord == "INTERSECT") {
						// stay on same line
					} else {
						writeNewline(0)
						lineStart = false
					}
				}
				needNewline = false
				needIndent = false
			} else if subClauses[upper] && parenDepth == 0 {
				if needNewline {
					writeNewline(1)
					lineStart = false
					needNewline = false
				}
			}

			if needNewline {
				writeNewline(1)
				lineStart = false
				needNewline = false
			}

			if !lineStart {
				// already have space from tokSpace handling
			}
			out.WriteString(word)
			lineStart = false
			prevWord = upper
			_ = needIndent

			if subClauses[upper] && parenDepth == 0 {
				needNewline = false
			}

		case tokString, tokIdent:
			if needNewline {
				writeNewline(1)
				lineStart = false
				needNewline = false
			}
			out.WriteString(tok.val)
			lineStart = false

		case tokOpen:
			out.WriteString("(")
			parenDepth++
			lineStart = false

		case tokClose:
			parenDepth--
			if parenDepth < 0 {
				parenDepth = 0
			}
			out.WriteString(")")
			lineStart = false

		case tokComma:
			out.WriteString(",")
			if parenDepth == 0 {
				// After comma in SELECT clause, break to new line
				if isInSelectClause(tokens, i) {
					needNewline = true
				}
			}
			lineStart = false

		case tokOp:
			out.WriteString(tok.val)
			lineStart = false
		}
	}

	return out.String()
}

// lookAheadForJoin checks if a join-related keyword (LEFT, RIGHT, etc.) is followed by JOIN.
func lookAheadForJoin(tokens []token, idx int) bool {
	for j := idx + 1; j < len(tokens); j++ {
		if tokens[j].typ == tokSpace {
			continue
		}
		if tokens[j].typ == tokWord && strings.ToUpper(tokens[j].val) == "JOIN" {
			return true
		}
		return false
	}
	return false
}

// isFirstNonSpace checks if token at idx is the first non-space token.
func isFirstNonSpace(tokens []token, idx int) bool {
	for i := 0; i < idx; i++ {
		if tokens[i].typ != tokSpace {
			return false
		}
	}
	return true
}

// isInSelectClause checks if the comma at idx is inside a SELECT clause (not in VALUES, etc.).
func isInSelectClause(tokens []token, idx int) bool {
	lastClause := ""
	depth := 0
	for i := 0; i < idx; i++ {
		if tokens[i].typ == tokOpen {
			depth++
		} else if tokens[i].typ == tokClose {
			depth--
		} else if tokens[i].typ == tokWord && depth == 0 {
			upper := strings.ToUpper(tokens[i].val)
			if majorClauses[upper] {
				lastClause = upper
			}
		}
	}
	return lastClause == "SELECT" || lastClause == ""
}
