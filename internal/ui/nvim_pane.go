package ui

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/charmbracelet/lipgloss"
	"github.com/creack/pty"
	"github.com/hinshun/vt10x"
)

// NvimPane manages an embedded neovim instance running inside a PTY,
// rendering its output via a vt10x virtual terminal.
type NvimPane struct {
	mu          sync.Mutex
	cmd         *exec.Cmd
	ptmx        *os.File
	term        vt10x.Terminal
	cols, rows  int
	running     bool
	exited      bool
	exitErr     error
	lastContent string
	tmpPath     string
	theme       *Theme
}

// NewNvimPane creates a new NvimPane with default dimensions.
func NewNvimPane(theme *Theme) *NvimPane {
	return &NvimPane{theme: theme, cols: 80, rows: 24}
}

// Start launches neovim in a PTY with the given initial SQL content.
// Creates a temp .sql file, starts nvim with TERM=xterm-256color.
// Spawns a goroutine to read PTY output and feed to vt10x terminal.
// On neovim exit, reads the file content back.
func (n *NvimPane) Start(initialContent string, cols, rows int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return nil
	}

	// Create temporary SQL file
	tmpFile, err := os.CreateTemp("", "tdb-*.sql")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	if _, err := tmpFile.WriteString(initialContent); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	tmpFile.Close()
	n.tmpPath = tmpFile.Name()

	if cols < 10 {
		cols = 80
	}
	if rows < 3 {
		rows = 24
	}
	n.cols = cols
	n.rows = rows

	// Initialize vt10x terminal
	n.term = vt10x.New(vt10x.WithSize(cols, rows))

	// Build neovim command — clean (no plugins), hide ~ on empty lines, enable SQL syntax highlight.
	// Disable all quit/exit commands — the user closes the tab with gw.
	disableQuit := `cnoremap <expr> <CR> getcmdtype()==':'&&getcmdline()=~#'^\s*\(w\?q\|qa\|wqa\|x\|xa\|exit\|quit\)\!*\s*$'?"\<C-u>echo 'Use gw to close tab'\<CR>":"\<CR>"`
	// SQL syntax highlighting colors for --clean mode
	sqlHighlight := strings.Join([]string{
		"syntax on",
		"set filetype=sql",
		"set termguicolors",
		"hi Statement guifg=#C084FC gui=bold",  // keywords: SELECT, FROM, etc.
		"hi Type guifg=#60A5FA",                 // types: INT, VARCHAR, etc.
		"hi String guifg=#34D399",               // strings
		"hi Number guifg=#FB923C",               // numbers
		"hi Comment guifg=#6B7280 gui=italic",   // comments
		"hi Identifier guifg=#F9FAFB",           // identifiers
		"hi Operator guifg=#F9FAFB",             // operators
		"hi Special guifg=#06B6D4",              // special chars
		"hi Constant guifg=#FB923C",             // constants (NULL, TRUE)
		"hi PreProc guifg=#C084FC",              // preprocessor
		"hi Normal guibg=NONE",                  // transparent background
	}, " | ")
	n.cmd = exec.Command("nvim",
		"--clean",
		"-c", "set fillchars+=eob:\\ ",
		"-c", "set laststatus=0",
		"-c", sqlHighlight,
		"-c", disableQuit,
		"-c", "nnoremap ZZ <Nop>",
		"-c", "nnoremap ZQ <Nop>",
		n.tmpPath,
	)
	n.cmd.Env = append(os.Environ(), "TERM=xterm-256color")

	// Start with PTY
	n.ptmx, err = pty.StartWithSize(n.cmd, &pty.Winsize{
		Cols: uint16(cols),
		Rows: uint16(rows),
	})
	if err != nil {
		os.Remove(n.tmpPath)
		n.tmpPath = ""
		return fmt.Errorf("failed to start nvim: %w", err)
	}

	n.running = true
	n.exited = false
	n.exitErr = nil
	n.lastContent = ""

	// Goroutine: read PTY output and feed to vt10x
	go func() {
		// Capture references before the loop to avoid races with Stop()
		cmdLocal := n.cmd
		tmpPathLocal := n.tmpPath
		ptmxLocal := n.ptmx

		reader := bufio.NewReaderSize(ptmxLocal, 4096)
		buf := make([]byte, 4096)
		for {
			nr, err := reader.Read(buf)
			if nr > 0 {
				n.mu.Lock()
				n.term.Write(buf[:nr])
				n.mu.Unlock()
			}
			if err != nil {
				break
			}
		}

		// Neovim exited — read back the file content
		var content string
		if tmpPathLocal != "" {
			data, readErr := os.ReadFile(tmpPathLocal)
			if readErr == nil {
				content = string(data)
			}
		}

		var waitErr error
		if cmdLocal != nil {
			waitErr = cmdLocal.Wait()
		}

		n.mu.Lock()
		n.running = false
		n.exited = true
		n.exitErr = waitErr
		n.lastContent = content
		n.mu.Unlock()
	}()

	return nil
}

// Write sends raw bytes to neovim's PTY input.
func (n *NvimPane) Write(p []byte) (int, error) {
	n.mu.Lock()
	ptmx := n.ptmx
	running := n.running
	n.mu.Unlock()

	if !running || ptmx == nil {
		return 0, io.ErrClosedPipe
	}
	return ptmx.Write(p)
}

// Resize resizes both the PTY and VT terminal.
func (n *NvimPane) Resize(cols, rows int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if cols < 2 {
		cols = 2
	}
	if rows < 2 {
		rows = 2
	}

	if cols == n.cols && rows == n.rows {
		return
	}
	n.cols = cols
	n.rows = rows

	if n.term != nil {
		n.term.Resize(cols, rows)
	}
	if n.ptmx != nil {
		_ = pty.Setsize(n.ptmx, &pty.Winsize{
			Cols: uint16(cols),
			Rows: uint16(rows),
		})
	}
}

// IsRunning returns true if neovim is currently running.
func (n *NvimPane) IsRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// HasExited returns true if neovim has exited.
func (n *NvimPane) HasExited() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.exited
}

// ConsumeExit returns the file content from the exited neovim session
// and resets the exit state.
func (n *NvimPane) ConsumeExit() (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	content := n.lastContent
	err := n.exitErr
	n.exited = false
	n.exitErr = nil
	n.lastContent = ""
	return content, err
}

// Stop terminates the neovim process and cleans up resources.
func (n *NvimPane) Stop() {
	n.mu.Lock()
	cmd := n.cmd
	ptmx := n.ptmx
	tmpPath := n.tmpPath
	n.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	if ptmx != nil {
		_ = ptmx.Close()
	}
	if tmpPath != "" {
		_ = os.Remove(tmpPath)
	}

	n.mu.Lock()
	n.running = false
	n.ptmx = nil
	n.cmd = nil
	n.tmpPath = ""
	n.mu.Unlock()
}

// TmpPath returns the path to the temporary SQL file.
func (n *NvimPane) TmpPath() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tmpPath
}

// CursorPos returns the cursor position in the VT terminal.
func (n *NvimPane) CursorPos() (x, y int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term == nil {
		return 0, 0
	}
	c := n.term.Cursor()
	return c.X, c.Y
}

// CurrentWord returns the word being typed at the cursor position
// by reading characters to the left of the cursor on the current line.
func (n *NvimPane) CurrentWord() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term == nil {
		return ""
	}
	c := n.term.Cursor()
	var word []rune
	for x := c.X - 1; x >= 0; x-- {
		g := n.term.Cell(x, c.Y)
		ch := g.Char
		if ch == 0 || ch == ' ' || ch == '(' || ch == ')' || ch == ',' || ch == ';' {
			break
		}
		word = append([]rune{ch}, word...)
	}
	return string(word)
}

// Render renders the VT terminal cells as a styled string.
// Iterates vt10x cells, applies FG/BG colors, cursor highlight (reverse).
func (n *NvimPane) Render(width, height int) string {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.term == nil {
		return ""
	}

	cur := n.term.Cursor()
	var lines []string

	renderRows := height
	if renderRows > n.rows {
		renderRows = n.rows
	}
	renderCols := width
	if renderCols > n.cols {
		renderCols = n.cols
	}

	for y := 0; y < renderRows; y++ {
		var lineBuilder strings.Builder
		for x := 0; x < renderCols; x++ {
			cell := n.term.Cell(x, y)
			ch := cell.Char
			if ch == 0 {
				ch = ' '
			}

			isCursor := (x == cur.X && y == cur.Y)

			fg := vtColorToLipgloss(cell.FG)
			bg := vtColorToLipgloss(cell.BG)

			style := lipgloss.NewStyle()

			// Apply text attributes from vt10x
			if cell.Mode&(1<<2) != 0 { // attrBold
				style = style.Bold(true)
			}
			if cell.Mode&(1<<3) != 0 { // attrItalic
				style = style.Italic(true)
			}
			if cell.Mode&(1<<1) != 0 { // attrUnderline
				style = style.Underline(true)
			}

			isReversed := cell.Mode&1 != 0 // attrReverse

			if isCursor {
				if n.isNormalModeLocked() {
					// Normal mode: block cursor (reverse video)
					style = style.Reverse(true)
				} else {
					// Insert mode: vertical bar cursor
					// Replace the character with a thin bar glyph
					ch = '▏'
					style = style.Foreground(lipgloss.Color("#FFFFFF"))
				}
			} else if isReversed {
				// Visual mode selection: swap fg/bg
				if bg != "" {
					style = style.Foreground(lipgloss.Color(bg))
				}
				if fg != "" {
					style = style.Background(lipgloss.Color(fg))
				}
				if fg == "" && bg == "" {
					style = style.Reverse(true)
				}
			} else {
				if fg != "" {
					style = style.Foreground(lipgloss.Color(fg))
				}
				if bg != "" {
					style = style.Background(lipgloss.Color(bg))
				}
			}

			lineBuilder.WriteString(style.Render(string(ch)))
		}
		lines = append(lines, lineBuilder.String())
	}

	return strings.Join(lines, "\n")
}

// IsNormalMode returns true if neovim appears to be in normal mode
// by checking the last line of the terminal for mode indicators.
func (n *NvimPane) IsNormalMode() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term == nil || !n.running {
		return true
	}
	// Read the last row (nvim's command/status line)
	lastRow := n.rows - 1
	var line []rune
	for x := 0; x < n.cols; x++ {
		ch := n.term.Cell(x, lastRow).Char
		if ch == 0 {
			ch = ' '
		}
		line = append(line, ch)
	}
	s := strings.TrimSpace(string(line))
	// Insert, visual, replace, command modes show indicators
	if strings.HasPrefix(s, "-- INSERT") ||
		strings.HasPrefix(s, "-- VISUAL") ||
		strings.HasPrefix(s, "-- REPLACE") ||
		strings.HasPrefix(s, "-- SELECT") ||
		(len(s) > 0 && s[0] == ':') {
		return false
	}
	return true
}

// IsInsertMode returns true if neovim is in insert mode.
func (n *NvimPane) IsInsertMode() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isInsertModeLocked()
}

func (n *NvimPane) isInsertModeLocked() bool {
	if n.term == nil || !n.running {
		return false
	}
	lastRow := n.rows - 1
	var line []rune
	for x := 0; x < n.cols; x++ {
		ch := n.term.Cell(x, lastRow).Char
		if ch == 0 {
			ch = ' '
		}
		line = append(line, ch)
	}
	s := strings.TrimSpace(string(line))
	return strings.HasPrefix(s, "-- INSERT")
}

// isNormalModeLocked is like IsNormalMode but assumes the mutex is already held.
func (n *NvimPane) isNormalModeLocked() bool {
	if n.term == nil || !n.running {
		return true
	}
	lastRow := n.rows - 1
	var line []rune
	for x := 0; x < n.cols; x++ {
		ch := n.term.Cell(x, lastRow).Char
		if ch == 0 {
			ch = ' '
		}
		line = append(line, ch)
	}
	s := strings.TrimSpace(string(line))
	if strings.HasPrefix(s, "-- INSERT") ||
		strings.HasPrefix(s, "-- VISUAL") ||
		strings.HasPrefix(s, "-- REPLACE") ||
		strings.HasPrefix(s, "-- SELECT") ||
		(len(s) > 0 && s[0] == ':') {
		return false
	}
	return true
}

// vtColorToLipgloss converts a vt10x color to a lipgloss color string.
func vtColorToLipgloss(c vt10x.Color) string {
	// vt10x default colors
	switch c {
	case vt10x.DefaultFG, vt10x.DefaultBG:
		return ""
	}

	idx := uint32(c)

	// ANSI 256-color palette: colors 0-255
	if idx <= 255 {
		return fmt.Sprintf("%d", idx)
	}

	// 24-bit RGB: vt10x stores as r<<16 | g<<8 | b
	// DefaultFG/BG/Cursor are at 1<<24+, so skip those (already handled above)
	if idx > 255 && idx < (1<<24) {
		r := (idx >> 16) & 0xFF
		g := (idx >> 8) & 0xFF
		b := idx & 0xFF
		return fmt.Sprintf("#%02x%02x%02x", r, g, b)
	}

	return ""
}
