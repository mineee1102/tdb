package ui

import "testing"

func TestExtractStatementAtLine(t *testing.T) {
sql := "SELECT * FROM users;\nSELECT * FROM orders;\nSELECT * FROM products;"

tests := []struct {
name   string
line   int
expect string
}{
{"first statement", 0, "SELECT * FROM users"},
{"second statement", 1, "SELECT * FROM orders"},
{"third statement", 2, "SELECT * FROM products"},
}
for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
got := extractStatementAtLine(sql, tt.line)
if got != tt.expect {
t.Errorf("extractStatementAtLine(line=%d) = %q, want %q", tt.line, got, tt.expect)
}
})
}
}

func TestExtractStatementMultiLine(t *testing.T) {
sql := "SELECT *\nFROM users\nWHERE id = 1;\n\nSELECT *\nFROM orders;"

got := extractStatementAtLine(sql, 1) // cursor on "FROM users"
expect := "SELECT *\nFROM users\nWHERE id = 1"
if got != expect {
t.Errorf("got %q, want %q", got, expect)
}

got2 := extractStatementAtLine(sql, 4) // cursor on "SELECT *" (2nd query)
expect2 := "SELECT *\nFROM orders"
if got2 != expect2 {
t.Errorf("got %q, want %q", got2, expect2)
}
}
