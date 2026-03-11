package ui

import "github.com/charmbracelet/lipgloss"

// ThemeName identifies a colour theme.
type ThemeName string

const (
	ThemeDark  ThemeName = "dark"
	ThemeLight ThemeName = "light"
)

// ThemeColors holds the raw colour palette for a theme.
// Each Theme is built from a ThemeColors instance.
type ThemeColors struct {
	Primary    lipgloss.Color
	Secondary  lipgloss.Color
	Border     lipgloss.Color
	BorderFoc  lipgloss.Color
	Bg         lipgloss.Color
	BgAlt      lipgloss.Color
	Fg         lipgloss.Color
	FgDim      lipgloss.Color
	Selected   lipgloss.Color
	ActiveCell lipgloss.Color
	Null       lipgloss.Color
	Error      lipgloss.Color
	Success    lipgloss.Color
	Header     lipgloss.Color
	StatusBg   lipgloss.Color
	Modified   lipgloss.Color
	ModifiedFg lipgloss.Color
	NewRow     lipgloss.Color
	NewRowFg   lipgloss.Color
	DeletedRow lipgloss.Color
	DeletedFg  lipgloss.Color
	Editing    lipgloss.Color
	SQLKeyword lipgloss.Color
	SQLString  lipgloss.Color
	SQLNumber  lipgloss.Color
	SQLComment lipgloss.Color
	DialogBg   lipgloss.Color
	DialogBtn  lipgloss.Color
	DialogBtnA lipgloss.Color
}

// Theme holds all lipgloss styles for the application.
type Theme struct {
	// Name of the active theme
	Name ThemeName

	// Window / border
	Border           lipgloss.Style
	FocusedBorder    lipgloss.Style
	TitleBar         lipgloss.Style
	StatusBar        lipgloss.Style
	StatusBarError   lipgloss.Style
	StatusBarSuccess lipgloss.Style

	// Table list pane
	TableListHeader      lipgloss.Style
	TableListItem        lipgloss.Style
	TableListActive      lipgloss.Style
	TableListFilter      lipgloss.Style
	TableListSearchPopup lipgloss.Style

	// Data view pane
	DataHeader       lipgloss.Style
	DataRow          lipgloss.Style
	DataRowAlt       lipgloss.Style
	DataSelected     lipgloss.Style
	DataCellActive   lipgloss.Style
	DataNull         lipgloss.Style
	DataRowNumber    lipgloss.Style
	DataFooter       lipgloss.Style
	DataCellModified lipgloss.Style
	DataRowNew       lipgloss.Style
	DataRowDeleted   lipgloss.Style
	DataCellEditing  lipgloss.Style
	DataFilterInput  lipgloss.Style

	// SQL editor
	SQLPrompt  lipgloss.Style
	SQLInput   lipgloss.Style
	SQLKeyword lipgloss.Style
	SQLString  lipgloss.Style
	SQLNumber  lipgloss.Style
	SQLComment lipgloss.Style
	SQLCursor  lipgloss.Style

	// Dialog
	DialogBorder       lipgloss.Style
	DialogTitle        lipgloss.Style
	DialogButton       lipgloss.Style
	DialogButtonActive lipgloss.Style

	// Help
	HelpKey  lipgloss.Style
	HelpDesc lipgloss.Style
	HelpBox  lipgloss.Style

	// Raw colour values (for inline styles in forms etc.)
	ColorPrimary   lipgloss.Color
	ColorSecondary lipgloss.Color
	ColorFg        lipgloss.Color
	ColorFgDim     lipgloss.Color
	ColorError     lipgloss.Color
	ColorSuccess   lipgloss.Color
}

// ── Dark theme colours ────────────────────────────────────────────

var darkColors = ThemeColors{
	Primary:    lipgloss.Color("#7C3AED"), // violet
	Secondary:  lipgloss.Color("#06B6D4"), // cyan
	Border:     lipgloss.Color("#4B5563"), // gray-600
	BorderFoc:  lipgloss.Color("#8B5CF6"), // violet-400
	Bg:         lipgloss.Color("#1F2937"), // gray-800
	BgAlt:      lipgloss.Color("#111827"), // gray-900
	Fg:         lipgloss.Color("#F9FAFB"), // gray-50
	FgDim:      lipgloss.Color("#9CA3AF"), // gray-400
	Selected:   lipgloss.Color("#374151"), // gray-700
	ActiveCell: lipgloss.Color("#4C1D95"), // violet-900
	Null:       lipgloss.Color("#6B7280"), // gray-500
	Error:      lipgloss.Color("#EF4444"), // red-500
	Success:    lipgloss.Color("#10B981"), // green-500
	Header:     lipgloss.Color("#60A5FA"), // blue-400
	StatusBg:   lipgloss.Color("#111827"), // gray-900
	Modified:   lipgloss.Color("#92400E"), // amber-800
	ModifiedFg: lipgloss.Color("#FDE68A"), // amber-200
	NewRow:     lipgloss.Color("#065F46"), // emerald-800
	NewRowFg:   lipgloss.Color("#A7F3D0"), // emerald-200
	DeletedRow: lipgloss.Color("#991B1B"), // red-800
	DeletedFg:  lipgloss.Color("#FCA5A5"), // red-300
	Editing:    lipgloss.Color("#1E3A5F"), // blue-900
	SQLKeyword: lipgloss.Color("#C084FC"), // purple-400
	SQLString:  lipgloss.Color("#34D399"), // emerald-400
	SQLNumber:  lipgloss.Color("#FB923C"), // orange-400
	SQLComment: lipgloss.Color("#6B7280"), // gray-500
	DialogBg:   lipgloss.Color("#1F2937"), // gray-800
	DialogBtn:  lipgloss.Color("#374151"), // gray-700
	DialogBtnA: lipgloss.Color("#7C3AED"), // violet
}

// ── Light theme colours ───────────────────────────────────────────

var lightColors = ThemeColors{
	Primary:    lipgloss.Color("#7C3AED"), // violet
	Secondary:  lipgloss.Color("#0891B2"), // cyan-600
	Border:     lipgloss.Color("#D1D5DB"), // gray-300
	BorderFoc:  lipgloss.Color("#8B5CF6"), // violet-400
	Bg:         lipgloss.Color("#FFFFFF"), // white
	BgAlt:      lipgloss.Color("#F3F4F6"), // gray-100
	Fg:         lipgloss.Color("#111827"), // gray-900
	FgDim:      lipgloss.Color("#6B7280"), // gray-500
	Selected:   lipgloss.Color("#E5E7EB"), // gray-200
	ActiveCell: lipgloss.Color("#EDE9FE"), // violet-100
	Null:       lipgloss.Color("#9CA3AF"), // gray-400
	Error:      lipgloss.Color("#DC2626"), // red-600
	Success:    lipgloss.Color("#059669"), // emerald-600
	Header:     lipgloss.Color("#2563EB"), // blue-600
	StatusBg:   lipgloss.Color("#F3F4F6"), // gray-100
	Modified:   lipgloss.Color("#FEF3C7"), // amber-100
	ModifiedFg: lipgloss.Color("#92400E"), // amber-800
	NewRow:     lipgloss.Color("#D1FAE5"), // emerald-100
	NewRowFg:   lipgloss.Color("#065F46"), // emerald-800
	DeletedRow: lipgloss.Color("#FEE2E2"), // red-100
	DeletedFg:  lipgloss.Color("#991B1B"), // red-800
	Editing:    lipgloss.Color("#DBEAFE"), // blue-100
	SQLKeyword: lipgloss.Color("#7C3AED"), // violet
	SQLString:  lipgloss.Color("#059669"), // emerald-600
	SQLNumber:  lipgloss.Color("#EA580C"), // orange-600
	SQLComment: lipgloss.Color("#9CA3AF"), // gray-400
	DialogBg:   lipgloss.Color("#FFFFFF"), // white
	DialogBtn:  lipgloss.Color("#E5E7EB"), // gray-200
	DialogBtnA: lipgloss.Color("#7C3AED"), // violet
}

// ── Theme constructors ────────────────────────────────────────────

// buildTheme constructs a Theme from a colour palette and a name.
func buildTheme(name ThemeName, c ThemeColors) *Theme {
	return &Theme{
		Name: name,

		Border: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(c.Border),
		FocusedBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(c.BorderFoc),
		TitleBar: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#F9FAFB")). // always light text on primary
			Background(c.Primary).
			Padding(0, 1),
		StatusBar: lipgloss.NewStyle().
			Foreground(c.FgDim).
			Background(c.StatusBg).
			Padding(0, 1),
		StatusBarError: lipgloss.NewStyle().
			Foreground(c.Error).
			Background(c.StatusBg).
			Padding(0, 1),
		StatusBarSuccess: lipgloss.NewStyle().
			Foreground(c.Success).
			Background(c.StatusBg).
			Padding(0, 1),

		// Table list pane
		TableListHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Header).
			Padding(0, 1),
		TableListItem: lipgloss.NewStyle().
			Foreground(c.Fg).
			Padding(0, 1),
		TableListActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Fg).
			Background(c.Selected).
			Padding(0, 1),
		TableListFilter: lipgloss.NewStyle().
			Foreground(c.Secondary).
			Padding(0, 1),
		TableListSearchPopup: lipgloss.NewStyle().
			Foreground(c.Fg).
			Background(c.DialogBg).
			Bold(true).
			Padding(0, 1).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(c.Primary),

		// Data view pane
		DataHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Header).
			PaddingLeft(1).PaddingRight(2),
		DataRow: lipgloss.NewStyle().
			Foreground(c.Fg).
			PaddingLeft(1).PaddingRight(2),
		DataRowAlt: lipgloss.NewStyle().
			Foreground(c.Fg).
			Background(c.BgAlt).
			PaddingLeft(1).PaddingRight(2),
		DataSelected: lipgloss.NewStyle().
			Foreground(c.Fg).
			Background(c.Selected).
			PaddingLeft(1).PaddingRight(2),
		DataCellActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Fg).
			Background(c.ActiveCell).
			PaddingLeft(1).PaddingRight(2),
		DataNull: lipgloss.NewStyle().
			Foreground(c.Null).
			Italic(true).
			PaddingLeft(1).PaddingRight(2),
		DataRowNumber: lipgloss.NewStyle().
			Foreground(c.FgDim).
			Padding(0, 1).
			Align(lipgloss.Right),
		DataFooter: lipgloss.NewStyle().
			Foreground(c.FgDim).
			Padding(0, 1),
		DataCellModified: lipgloss.NewStyle().
			Foreground(c.ModifiedFg).
			Background(c.Modified).
			PaddingLeft(1).PaddingRight(2),
		DataRowNew: lipgloss.NewStyle().
			Foreground(c.NewRowFg).
			Background(c.NewRow).
			PaddingLeft(1).PaddingRight(2),
		DataRowDeleted: lipgloss.NewStyle().
			Foreground(c.DeletedFg).
			Background(c.DeletedRow).
			Strikethrough(true).
			PaddingLeft(1).PaddingRight(2),
		DataCellEditing: lipgloss.NewStyle().
			Foreground(c.Fg).
			Background(c.Editing).
			Bold(true).
			PaddingLeft(1).PaddingRight(2),
		DataFilterInput: lipgloss.NewStyle().
			Foreground(c.Secondary).
			Padding(0, 1),

		// SQL editor
		SQLPrompt: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Secondary),
		SQLInput: lipgloss.NewStyle().
			Foreground(c.Fg),
		SQLKeyword: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.SQLKeyword),
		SQLString: lipgloss.NewStyle().
			Foreground(c.SQLString),
		SQLNumber: lipgloss.NewStyle().
			Foreground(c.SQLNumber),
		SQLComment: lipgloss.NewStyle().
			Foreground(c.SQLComment).
			Italic(true),
		SQLCursor: lipgloss.NewStyle().
			Reverse(true),

		// Dialog
		DialogBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(c.BorderFoc).
			Background(c.DialogBg).
			Padding(1, 2),
		DialogTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#F9FAFB")).
			Background(c.Primary).
			Padding(0, 1),
		DialogButton: lipgloss.NewStyle().
			Foreground(c.FgDim).
			Background(c.DialogBtn).
			Padding(0, 2),
		DialogButtonActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#F9FAFB")).
			Background(c.DialogBtnA).
			Padding(0, 2),

		// Help
		HelpKey: lipgloss.NewStyle().
			Bold(true).
			Foreground(c.Secondary).
			Width(14),
		HelpDesc: lipgloss.NewStyle().
			Foreground(c.FgDim),
		HelpBox: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(c.BorderFoc).
			Padding(1, 2),

		// Raw colours
		ColorPrimary:   c.Primary,
		ColorSecondary: c.Secondary,
		ColorFg:        c.Fg,
		ColorFgDim:     c.FgDim,
		ColorError:     c.Error,
		ColorSuccess:   c.Success,
	}
}

// DarkTheme returns the default dark theme.
func DarkTheme() *Theme {
	return buildTheme(ThemeDark, darkColors)
}

// LightTheme returns the light theme.
func LightTheme() *Theme {
	return buildTheme(ThemeLight, lightColors)
}

// ThemeByName returns a Theme for the given name. Defaults to dark.
func ThemeByName(name string) *Theme {
	switch ThemeName(name) {
	case ThemeLight:
		return LightTheme()
	default:
		return DarkTheme()
	}
}

// ToggleThemeName returns the opposite theme name.
func ToggleThemeName(current ThemeName) ThemeName {
	if current == ThemeDark {
		return ThemeLight
	}
	return ThemeDark
}
