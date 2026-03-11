package ui

import (
	"strings"
	"testing"
)

func TestFormatSQL_SimpleSelect(t *testing.T) {
	input := "select id, name, email from users where id > 1 and name = 'test' order by id desc"
	result := formatSQL(input)
	if !strings.Contains(result, "SELECT") {
		t.Error("Expected keywords to be uppercased")
	}
	if !strings.Contains(result, "\nFROM") {
		t.Error("Expected FROM on new line")
	}
	if !strings.Contains(result, "\nWHERE") {
		t.Error("Expected WHERE on new line")
	}
	if !strings.Contains(result, "\nORDER BY") {
		t.Error("Expected ORDER BY on new line")
	}
}

func TestFormatSQL_MultipleStatements(t *testing.T) {
	input := "select 1; select 2"
	result := formatSQL(input)
	parts := strings.Split(strings.TrimSpace(result), ";\n\n")
	if len(parts) != 2 {
		t.Errorf("Expected 2 statements, got %d: %q", len(parts), result)
	}
}

func TestFormatSQL_PreservesStrings(t *testing.T) {
	input := "select * from users where name = 'hello world'"
	result := formatSQL(input)
	if !strings.Contains(result, "'hello world'") {
		t.Errorf("Expected string literal preserved, got: %s", result)
	}
}

func TestFormatSQL_JoinClause(t *testing.T) {
	input := "select u.id from users u left join orders o on u.id = o.user_id"
	result := formatSQL(input)
	if !strings.Contains(result, "\nLEFT JOIN") {
		t.Errorf("Expected LEFT JOIN on new line, got: %s", result)
	}
}

func TestFormatSQL_Empty(t *testing.T) {
	result := formatSQL("")
	if result != "" {
		t.Errorf("Expected empty result for empty input, got: %q", result)
	}
}
