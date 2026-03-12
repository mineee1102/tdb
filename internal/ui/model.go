package ui

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minee/tdb/internal/config"
	"github.com/minee/tdb/internal/db"
	"github.com/minee/tdb/internal/export"
)

// AppScreen represents which top-level screen is active.
type AppScreen int

const (
	ScreenConnection AppScreen = iota
	ScreenMain
)

// Model is the top-level Bubble Tea model for tdb.
type Model struct {
	screen     AppScreen
	activePane ActivePane

	connectionFormModel  ConnectionFormModel
	tableListModel       TableListModel
	dataViewModel        DataViewModel
	sqlEditorModel       SQLEditorModel
	statusBarModel       StatusBarModel
	helpModel            HelpModel
	dialogModel          DialogModel
	structureViewModel   StructureViewModel

	// Tab management
	tabBar TabBar

	driver db.Driver
	config *config.Config

	width  int
	height int
	err    error
	theme  *Theme

	connectionInfo string

	// Pending commit statements (stored while dialog is open)
	pendingCommitStmts []commitStatement

	// Structure view state
	showStructure bool

	// SQL editor toggle state (right pane shows SQL editor instead of DataView)
	showSQLEditor bool

	// Export state
	exportMenuVisible bool
	exportPathInput   bool
	exportFormat      string // "csv", "json", "sql"
	exportPath        string

	// SQL file save state
	sqlSaveInput bool   // showing save name prompt
	sqlSaveName  string // current input for save name

	// Pending DDL dialog
	pendingDDL string

	// Neovim embedded editor
	nvimPane              *NvimPane
	nvimEnabled           bool
	nvimCompletions       []string
	nvimCompletionIndex   int
	nvimCompletionPrefix  string

	// g-prefix pending for tab navigation (gh / gl)
	gPending bool

	// Shared SQL autocomplete cache (survives tab creation/destruction)
	cachedTableNames  []string
	cachedColumnNames map[string][]string

	// Fuzzy search modal
	fuzzySearchModal *FuzzySearchModal
	spacePressTime   time.Time // track space press for double-tap detection
}

// nvimTickMsg triggers periodic re-rendering of the nvim pane.
type nvimTickMsg struct{}

// nvimSaveAndExecMsg is sent after neovim saves the file, to execute the SQL.
type nvimSaveAndExecMsg struct{}

// nvimSaveAndFormatMsg is sent after neovim saves the file, to format the SQL.
type nvimSaveAndFormatMsg struct{}

// nvimSaveFileMsg is sent after neovim saves the file, to persist to a SQL file.
type nvimSaveFileMsg struct {
	Name string
}

// NewModel creates a new top-level Model with an active driver (DSN specified).
func NewModel(driver db.Driver, cfg *config.Config, connectionInfo string) Model {
	themeName := ""
	if cfg != nil {
		themeName = cfg.Theme
	}
	theme := ThemeByName(themeName)

	m := Model{
		screen:             ScreenMain,
		activePane:         PaneTableList,
		tableListModel:     NewTableListModel(driver, theme),
		dataViewModel:      NewDataViewModel(driver, theme),
		sqlEditorModel:     NewSQLEditorModel(theme),
		statusBarModel:     NewStatusBarModel(theme),
		helpModel:          NewHelpModel(theme),
		dialogModel:        NewDialogModel(theme),
		structureViewModel: NewStructureViewModel(driver, theme),
		tabBar:             NewTabBar(theme),
		driver:             driver,
		config:             cfg,
		theme:              theme,
		connectionInfo:     connectionInfo,
		nvimPane:           NewNvimPane(theme),
		fuzzySearchModal:   &FuzzySearchModal{theme: theme},
	}

	m.tableListModel.SetFocused(true)
	m.statusBarModel.SetConnectionInfo(connectionInfo)

	return m
}

// NewConnectionModel creates a new top-level Model starting at the connection screen.
func NewConnectionModel(cfg *config.Config) Model {
	themeName := ""
	if cfg != nil {
		themeName = cfg.Theme
	}
	theme := ThemeByName(themeName)

	m := Model{
		screen:               ScreenConnection,
		activePane:           PaneTableList,
		connectionFormModel:  NewConnectionFormModel(cfg, theme),
		tableListModel:       NewTableListModel(nil, theme),
		dataViewModel:        NewDataViewModel(nil, theme),
		sqlEditorModel:       NewSQLEditorModel(theme),
		statusBarModel:       NewStatusBarModel(theme),
		helpModel:            NewHelpModel(theme),
		dialogModel:          NewDialogModel(theme),
		structureViewModel:   NewStructureViewModel(nil, theme),
		tabBar:               NewTabBar(theme),
		config:               cfg,
		theme:                theme,
		nvimPane:             NewNvimPane(theme),
		fuzzySearchModal:     &FuzzySearchModal{theme: theme},
	}

	return m
}

// transitionToMain switches from connection screen to main screen after connection.
func (m *Model) transitionToMain(driver db.Driver, connInfo string, conn config.Connection) tea.Cmd {
	m.screen = ScreenMain
	m.driver = driver
	m.connectionInfo = connInfo

	// Clean up old tabs (stop nvim panes)
	m.clearAllTabs()

	// Re-initialize child models with the new driver
	m.tableListModel = NewTableListModel(driver, m.theme)
	m.dataViewModel = NewDataViewModel(driver, m.theme)
	m.sqlEditorModel = NewSQLEditorModel(m.theme)
	m.structureViewModel = NewStructureViewModel(driver, m.theme)

	m.tableListModel.SetFocused(true)
	m.showStructure = false
	m.showSQLEditor = false
	m.nvimPane = NewNvimPane(m.theme)
	m.nvimEnabled = false
	m.nvimCompletions = nil
	m.statusBarModel.SetConnectionInfo(connInfo)
	m.activePane = PaneTableList
	m.tabBar = NewTabBar(m.theme)

	// Recalculate layout and load tables + databases
	m.recalcLayout()

	// If no database was specified and the driver is not SQLite, enter DB select mode.
	// Only load databases list (skip LoadTablesCmd since no DB is selected yet).
	if conn.Database == "" && conn.Type != "sqlite" {
		m.tableListModel.dbSelectMode = true
		return LoadDatabasesCmd(driver)
	}

	return tea.Batch(LoadTablesCmd(driver), LoadDatabasesCmd(driver))
}

// Init initializes the model and returns the initial command.
func (m Model) Init() tea.Cmd {
	if m.screen == ScreenConnection {
		return m.connectionFormModel.Init()
	}
	return tea.Batch(LoadTablesCmd(m.driver), LoadDatabasesCmd(m.driver))
}

// Update handles all incoming messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.dialogModel.SetSize(msg.Width, msg.Height)
		m.fuzzySearchModal.SetSize(msg.Width, msg.Height)
		if m.screen == ScreenConnection {
			m.connectionFormModel.SetSize(msg.Width, msg.Height)
			return m, nil
		}
		m.recalcLayout()
		return m, nil

	case ConnectionEstablishedMsg:
		// Transition from connection screen to main screen
		connInfo := msg.Connection.Name + " (" + msg.Connection.Type + ")"
		cmd := m.transitionToMain(msg.Driver, connInfo, msg.Connection)
		return m, cmd

	case ConnectionTestResultMsg:
		if m.screen == ScreenConnection {
			m.connectionFormModel, _ = m.connectionFormModel.Update(msg)
			return m, nil
		}

	case tea.KeyMsg:
		// Route to connection screen if active
		if m.screen == ScreenConnection {
			var cmd tea.Cmd
			m.connectionFormModel, cmd = m.connectionFormModel.Update(msg)
			return m, cmd
		}

		// Handle fuzzy search modal if visible
		if m.fuzzySearchModal.IsVisible() {
			if msg.String() == "enter" {
				// Handle selection
				selected := m.fuzzySearchModal.GetSelected()
				if selected != nil {
					switch selected.Type {
					case FuzzyDatabase:
						// Switch to database
						m.fuzzySearchModal.Close()
						if m.driver != nil {
							m.tableListModel.dbSelectIdx = 0
							// Find the database in the list
							for i, db := range m.tableListModel.databases {
								if db == selected.Name {
									m.tableListModel.dbSelectIdx = i
									break
								}
							}
							m.tableListModel.dbSwitching = true
							return m, SwitchDatabaseCmd(m.driver, selected.Name)
						}
						return m, nil
					case FuzzyTable:
						// Open table — same logic as TableSelectedMsg
						m.fuzzySearchModal.Close()
						isNew := m.openDataTab(selected.Schema, selected.Name)
						if !isNew {
							m.statusBarModel.SetMessage(selected.Schema+"."+selected.Name, "info")
							m.statusBarModel.SetRowCount(m.dataViewModel.RowCount())
							m.updateStatusBarExtras()
							m.setActivePane(PaneDataView)
							return m, nil
						}
						m.statusBarModel.SetMessage("Loading "+selected.Schema+"."+selected.Name+"...", "info")
						m.dataViewModel.loading = true
						m.dataViewModel.page = 0
						m.dataViewModel.sortColumn = ""
						m.dataViewModel.sortDir = ""
						m.dataViewModel.activeFilter = ""
						m.dataViewModel.filterInput = ""
						m.dataViewModel.sqlResultMode = false
						m.showStructure = false
						m.showSQLEditor = false
						m.recalcLayout()
						return m, tea.Batch(
							LoadDataCmd(m.driver, selected.Schema, selected.Name, 0, m.dataViewModel.pageSize),
							DescribeTableCmd(m.driver, selected.Schema, selected.Name),
						)
					case FuzzyTab:
						// Switch to tab
						m.saveCurrentTab()
						m.loadTab(selected.TabIdx)
						m.fuzzySearchModal.Close()
						return m, nil
					case FuzzySQLFile:
						m.fuzzySearchModal.Close()
						// Check if this file is already open in a tab
						existingIdx := -1
						for i, t := range m.tabBar.tabs {
							if t.sqlFilePath == selected.FilePath {
								existingIdx = i
								break
							}
						}
						if existingIdx >= 0 {
							// Focus existing tab
							m.saveCurrentTab()
							m.loadTab(existingIdx)
							return m, nil
						}
						// Open saved SQL file in new SQL tab
						data, err := os.ReadFile(selected.FilePath)
						if err != nil {
							m.statusBarModel.SetMessage("Failed to load SQL file", "error")
							return m, nil
						}
						m.openSQLTab()
						if tab := m.tabBar.ActiveTab(); tab != nil {
							tab.title = selected.Name
							tab.sqlFilePath = selected.FilePath
						}
						content := string(data)
						m.sqlEditorModel.SetContent(content)
						if !m.nvimPane.IsRunning() && !m.nvimPane.HasExited() {
							sqlW := m.dataViewModel.width - 2
							sqlH := m.dataViewModel.height - 2
							if sqlW < 10 {
								sqlW = 80
							}
							if sqlH < 3 {
								sqlH = 5
							}
							if err := m.nvimPane.Start(content, sqlW, sqlH); err == nil {
								m.nvimEnabled = true
							}
						}
						m.recalcLayout()
						if m.nvimEnabled {
							return m, tea.Tick(50*time.Millisecond, func(time.Time) tea.Msg { return nvimTickMsg{} })
						}
						return m, nil
					}
				}
				return m, nil
			} else if msg.String() == "esc" {
				m.fuzzySearchModal.Close()
				return m, nil
			} else {
				var cmd tea.Cmd
				m.fuzzySearchModal, cmd = m.fuzzySearchModal.Update(msg)
				return m, cmd
			}
		}

		// If dialog is visible, route to dialog
		if m.dialogModel.IsVisible() {
			var cmd tea.Cmd
			m.dialogModel, cmd = m.dialogModel.Update(msg)
			return m, cmd
		}

		// Export path input mode
		if m.exportPathInput {
			return m.handleExportPathInput(msg)
		}

		// SQL save name input mode
		if m.sqlSaveInput {
			return m.handleSQLSaveInput(msg)
		}

		// Export menu visible
		if m.exportMenuVisible {
			return m.handleExportMenu(msg)
		}

		// Help toggle (global, highest priority)
		if msg.String() == "?" {
			// Only toggle help if NOT in filter mode, edit mode, or SQL editor input
			if m.activePane != PaneSQLEditor && !m.isInputMode() {
				m.helpModel.Toggle()
				return m, nil
			}
		}

		// If help is visible, any key other than ? closes it
		if m.helpModel.IsVisible() {
			m.helpModel.Toggle()
			return m, nil
		}

		// Global quit — show confirmation
		if msg.String() == "ctrl+c" {
			m.dialogModel.Show("Quit", "Are you sure you want to quit?", "quit")
			return m, nil
		}

		// Quit with 'q' only when not in input mode — show confirmation
		if msg.String() == "q" && m.activePane != PaneSQLEditor && !m.isInputMode() {
			m.dialogModel.Show("Quit", "Are you sure you want to quit?", "quit")
			return m, nil
		}

		// Ctrl+Q: open new SQL editor tab
		if msg.String() == "ctrl+q" && !m.isInputMode() {
			m.openSQLTab()
			// Start neovim in the new tab
			if !m.nvimPane.IsRunning() && !m.nvimPane.HasExited() {
				content := m.sqlEditorModel.GetInput()
				sqlW := m.dataViewModel.width - 2
				sqlH := m.dataViewModel.height - 2
				if sqlW < 10 {
					sqlW = 80
				}
				if sqlH < 3 {
					sqlH = 5
				}
				err := m.nvimPane.Start(content, sqlW, sqlH)
				if err == nil {
					m.nvimEnabled = true
				}
			}
			m.recalcLayout()
			if m.nvimEnabled {
				return m, tea.Tick(50*time.Millisecond, func(time.Time) tea.Msg { return nvimTickMsg{} })
			}
			return m, nil
		}

		// Ctrl+W: close current tab (keep as alternative)
		if msg.String() == "ctrl+w" && !m.isInputMode() {
			if m.tabBar.Len() > 0 {
				m.closeCurrentTab()
			}
			return m, nil
		}

		// g-prefix for tab navigation (gh = prev, gl = next, gw = close)
		if msg.String() == "g" && !m.isInputMode() && m.activePane != PaneSQLEditor {
			if m.tabBar.Len() > 0 {
				m.gPending = true
				return m, nil
			}
			// No tabs — fall through to let data view handle 'g'
		}
		if m.gPending {
			m.gPending = false
			if msg.String() == "l" && m.tabBar.Len() > 1 {
				cmd := m.switchTab(1)
				return m, cmd
			}
			if msg.String() == "h" && m.tabBar.Len() > 1 {
				cmd := m.switchTab(-1)
				return m, cmd
			}
			if msg.String() == "w" && m.tabBar.Len() > 0 {
				m.closeCurrentTab()
				return m, nil
			}
			// Not h/l/w — replay the 'g' action on data view, then fall through for current key
			if m.activePane == PaneDataView {
				m.dataViewModel.cursorRow = 0
				m.dataViewModel.scrollOffsetY = 0
			}
		}

		// Ctrl+T: toggle dark / light theme
		if msg.String() == "ctrl+t" && !m.isInputMode() {
			m.toggleTheme()
			themeName := string(m.theme.Name)
			m.statusBarModel.SetMessage("Theme: "+themeName, "success")
			return m, nil
		}

		// Ctrl+F: open fuzzy search modal
		if msg.String() == "ctrl+f" {
			m.fuzzySearchModal.Open()
			m.fuzzySearchModal.Refresh(
				m.driver,
				m.tableListModel.databases,
				m.tableListModel.currentDBName,
				m.tableListModel.schemas,
				m.tableListModel.tables,
				m.tabBar.tabs,
			)
			return m, nil
		}

		// Space key for fuzzy search trigger (double-tap within 200ms)
		if msg.String() == "space" {
			now := time.Now()
			if !m.spacePressTime.IsZero() && now.Sub(m.spacePressTime) < 200*time.Millisecond && !m.isInputMode() {
				// Double space! Open fuzzy search (but not in input mode like filter)
				m.fuzzySearchModal.Open()
				m.fuzzySearchModal.Refresh(
					m.driver,
					m.tableListModel.databases,
					m.tableListModel.currentDBName,
					m.tableListModel.schemas,
					m.tableListModel.tables,
					m.tabBar.tabs,
				)
				m.spacePressTime = time.Time{} // Reset for next double-tap
				return m, nil
			}
			m.spacePressTime = now // Record this space press for next comparison
			// Continue to normal key handling for first space (or if in input mode)
		}

		// Ctrl+S: save SQL file when SQL editor is visible and focused
		if msg.String() == "ctrl+s" && m.showSQLEditor && m.activePane == PaneSQLEditor {
			tab := m.tabBar.ActiveTab()
			if tab != nil && tab.sqlFilePath != "" {
				// Already saved — overwrite silently
				if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
					m.nvimPane.Write([]byte("\x1b:w\r"))
					name := tab.title
					return m, tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
						return nvimSaveFileMsg{Name: name}
					})
				}
				content := m.sqlEditorModel.GetInput()
				m.saveSQLFile(tab.title, content)
				m.statusBarModel.SetMessage("Saved: "+tab.title+".sql", "info")
				return m, nil
			}
			// New file — show name prompt
			defaultName := ""
			if tab != nil {
				defaultName = tab.title
			}
			m.sqlSaveInput = true
			m.sqlSaveName = defaultName
			return m, nil
		}

		// Ctrl+Shift+L: format SQL when SQL editor is visible and focused
		if msg.String() == "ctrl+shift+l" && m.showSQLEditor && m.activePane == PaneSQLEditor {
			if m.nvimEnabled && m.nvimPane.IsRunning() {
				m.nvimPane.Write([]byte("\x1b:w\r"))
				return m, tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
					return nvimSaveAndFormatMsg{}
				})
			}
			return m, nil
		}

		// Ctrl+G: execute SQL when SQL editor is visible and focused
		if msg.String() == "ctrl+g" && m.showSQLEditor && m.activePane == PaneSQLEditor {
			if m.nvimEnabled && m.nvimPane.IsRunning() {
				// Send :w to save, then read file content and execute
				m.nvimPane.Write([]byte("\x1b:w\r"))
				return m, tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
					return nvimSaveAndExecMsg{}
				})
			}
			// Built-in editor execution
			fullInput := m.sqlEditorModel.GetInput()
			input := extractStatementAtLine(fullInput, m.sqlEditorModel.cursorRow)
			if input != "" && !m.sqlEditorModel.executing {
				m.sqlEditorModel.executing = true
				m.sqlEditorModel.history.Add(input)
				m.sqlEditorModel.history.Save()
				m.sqlEditorModel.historyIndex = -1
				m.sqlEditorModel.completionActive = false
				return m, func() tea.Msg {
					return SQLExecuteRequestMsg{Query: input}
				}
			}
			return m, nil
		}

		// Structure view (after global keys, before tab/pane-switching)
		if m.showStructure {
			return m.handleStructureViewKey(msg)
		}

		// Forward all keys to neovim when active and focused
		if m.nvimEnabled && m.nvimPane.IsRunning() && m.activePane == PaneSQLEditor {
			// Clear completions in normal mode (before checking if completions exist)
			if m.nvimPane.IsNormalMode() && len(m.nvimCompletions) > 0 {
				m.nvimCompletions = nil
				m.nvimCompletionIndex = 0
			}

			// Handle completion selection first
			if len(m.nvimCompletions) > 0 {
				switch msg.String() {
				case "tab", "enter":
					// Accept completion: delete prefix chars and type the selected completion
					if m.nvimCompletionIndex < len(m.nvimCompletions) {
						selected := m.nvimCompletions[m.nvimCompletionIndex]
						prefix := m.nvimCompletionPrefix
						for i := 0; i < len([]rune(prefix)); i++ {
							m.nvimPane.Write([]byte{0x08})
						}
						m.nvimPane.Write([]byte(selected))
						m.nvimCompletions = nil
					}
					return m, nil
				case "ctrl+n", "down":
					if m.nvimCompletionIndex < len(m.nvimCompletions)-1 {
						m.nvimCompletionIndex++
					}
					return m, nil
				case "ctrl+p", "up":
					if m.nvimCompletionIndex > 0 {
						m.nvimCompletionIndex--
					}
					return m, nil
				case "esc":
					// Dismiss suggestions and forward esc to nvim (enter normal mode)
					m.nvimCompletions = nil
					m.nvimPane.Write([]byte{0x1b})
					return m, nil
				default:
					// Any other key: close completions and forward to neovim
					m.nvimCompletions = nil
					raw := keyToBytes(msg)
					if len(raw) > 0 {
						m.nvimPane.Write(raw)
					}
					return m, nil
				}
			}

			// Tab for pane switching when no completions
			if msg.String() == "tab" && len(m.nvimCompletions) == 0 {
				m.switchPane()
				return m, nil
			}

			// g-prefix tab navigation in nvim normal mode
			if msg.String() == "g" && m.nvimPane.IsNormalMode() && m.tabBar.Len() > 0 {
				m.gPending = true
				return m, nil
			}
			if m.gPending {
				m.gPending = false
				if msg.String() == "h" && m.tabBar.Len() > 1 {
					cmd := m.switchTab(-1)
					return m, cmd
				}
				if msg.String() == "l" && m.tabBar.Len() > 1 {
					cmd := m.switchTab(1)
					return m, cmd
				}
				if msg.String() == "w" && m.tabBar.Len() > 0 {
					m.closeCurrentTab()
					return m, nil
				}
				// Not h/l/w — forward the buffered 'g' then current key to nvim
				m.nvimPane.Write([]byte("g"))
				raw := keyToBytes(msg)
				if len(raw) > 0 {
					m.nvimPane.Write(raw)
				}
				return m, nil
			}

			// Forward all other keys to neovim
			raw := keyToBytes(msg)
			if len(raw) > 0 {
				m.nvimPane.Write(raw)
			}
			return m, nil
		}

		// Global pane switching (not in input mode, and not when SQL editor has completions)
		if msg.String() == "tab" && !m.isInputMode() {
			if m.activePane == PaneSQLEditor && m.sqlEditorModel.completionActive {
				// Let SQL editor handle Tab for completion
			} else {
				m.switchPane()
				return m, nil
			}
		}

		// Ctrl+O: export menu (when data is available)
		if msg.String() == "ctrl+o" && m.activePane == PaneDataView && !m.isInputMode() {
			if len(m.dataViewModel.columns) > 0 {
				m.exportMenuVisible = true
				return m, nil
			}
		}

		// y: copy cell value (OSC 52) — only when not in visual/yank mode
		if msg.String() == "y" && m.activePane == PaneDataView && !m.isInputMode() &&
			!m.dataViewModel.visualMode && !m.dataViewModel.yankPending {
			return m.handleCellCopy()
		}

		// Dispatch to active pane
		return m.dispatchKeyToPane(msg)

	// ── Messages from child models ──

	case TablesLoadedMsg:
		if msg.Err != nil {
			m.err = msg.Err
			m.dialogModel.ShowError("Connection Error", msg.Err.Error())
			return m, nil
		}
		m.tableListModel, _ = m.tableListModel.Update(msg)
		// Update shared table cache and propagate to active SQL editor
		m.cachedTableNames = nil
		for _, s := range msg.Schemas {
			for _, t := range msg.Tables[s] {
				m.cachedTableNames = append(m.cachedTableNames, t.Name)
			}
		}
		m.sqlEditorModel.UpdateTableCache(msg.Schemas, msg.Tables)
		m.statusBarModel.SetMessage("Tables loaded", "success")
		return m, nil

	case DatabasesLoadedMsg:
		if msg.Err != nil {
			// Non-fatal: just log to status bar; DB switching won't be available
			m.statusBarModel.SetMessage("DB list: "+msg.Err.Error(), "error")
			return m, nil
		}
		m.tableListModel, _ = m.tableListModel.Update(msg)
		return m, nil

	case SwitchDatabaseMsg:
		m.statusBarModel.SetMessage("Switching to database: "+msg.Database+"...", "info")
		return m, SwitchDatabaseCmd(m.driver, msg.Database)

	case DatabaseSwitchedMsg:
		if msg.Err != nil {
			m.dialogModel.ShowError("Switch DB Error", msg.Err.Error())
			m.tableListModel.dbSwitching = false
			return m, nil
		}
		if m.tableListModel.dbSelectMode {
			m.tableListModel.dbSelectMode = false
		}
		// Clear table list filter and old data immediately
		m.tableListModel.filterInput = ""
		m.tableListModel.filtering = false
		m.tableListModel.schemas = nil
		m.tableListModel.tables = make(map[string][]db.TableInfo)
		m.tableListModel.selectedSchema = 0
		m.tableListModel.selectedTable = 0
		m.updateConnectionInfoDB(msg.Database)
		// Clear all tabs (data belongs to old DB)
		m.clearAllTabs()
		m.showStructure = false
		m.statusBarModel.SetRowCount(0)
		m.statusBarModel.SetMessage("Switched to database: "+msg.Database, "success")
		m.tableListModel.dbSwitching = false
		return m, tea.Batch(
			LoadTablesCmd(m.driver),
			LoadDatabasesCmd(m.driver),
		)

	case TableSelectedMsg:
		// Open or switch to a tab for this table
		isNew := m.openDataTab(msg.Schema, msg.Table)
		if !isNew {
			// Existing tab — just switch focus, don't reload
			m.statusBarModel.SetMessage(msg.Schema+"."+msg.Table, "info")
			m.statusBarModel.SetRowCount(m.dataViewModel.RowCount())
			m.updateStatusBarExtras()
			m.setActivePane(PaneDataView)
			return m, nil
		}
		// New tab — initialize and load data
		m.statusBarModel.SetMessage("Loading "+msg.Schema+"."+msg.Table+"...", "info")
		m.dataViewModel.loading = true
		m.dataViewModel.page = 0
		m.dataViewModel.sortColumn = ""
		m.dataViewModel.sortDir = ""
		m.dataViewModel.activeFilter = ""
		m.dataViewModel.filterInput = ""
		m.dataViewModel.sqlResultMode = false
		m.showStructure = false
		m.showSQLEditor = false
		m.recalcLayout()
		return m, tea.Batch(
			LoadDataCmd(m.driver, msg.Schema, msg.Table, 0, m.dataViewModel.pageSize),
			DescribeTableCmd(m.driver, msg.Schema, msg.Table),
		)

	case DataLoadedMsg:
		if msg.Err != nil {
			m.err = msg.Err
			m.dialogModel.ShowError("Data Load Error", msg.Err.Error())
			m.dataViewModel.loading = false
			return m, nil
		}
		// Route to correct tab if msg has schema/table info
		if msg.Schema != "" && msg.Table != "" {
			activeTab := m.tabBar.ActiveTab()
			if activeTab == nil || activeTab.schemaName != msg.Schema || activeTab.tableName != msg.Table {
				// Data belongs to a different tab — find and update it
				if idx := m.tabBar.FindDataTab(msg.Schema, msg.Table); idx >= 0 {
					tab := &m.tabBar.tabs[idx]
					tab.dataView, _ = tab.dataView.Update(msg)
				}
				return m, nil
			}
		}
		m.dataViewModel, _ = m.dataViewModel.Update(msg)
		m.statusBarModel.SetRowCount(m.dataViewModel.RowCount())
		m.statusBarModel.SetMessage("", "info")
		// Update status with sort/filter info
		m.updateStatusBarExtras()
		// Auto-switch to data view after loading
		m.setActivePane(PaneDataView)
		return m, nil

	case TableDescribedMsg:
		// Route to correct tab if schema/table info is present
		if msg.Schema != "" && msg.Table != "" {
			activeTab := m.tabBar.ActiveTab()
			if activeTab == nil || activeTab.schemaName != msg.Schema || activeTab.tableName != msg.Table {
				if idx := m.tabBar.FindDataTab(msg.Schema, msg.Table); idx >= 0 {
					tab := &m.tabBar.tabs[idx]
					tab.dataView, _ = tab.dataView.Update(msg)
				}
				return m, nil
			}
		}
		m.dataViewModel, _ = m.dataViewModel.Update(msg)
		if msg.Err != nil {
			m.statusBarModel.SetMessage("Warning: could not describe table", "error")
		} else if msg.Desc != nil {
			// Update shared column cache and active SQL editor
			if m.cachedColumnNames == nil {
				m.cachedColumnNames = make(map[string][]string)
			}
			cols := make([]string, len(msg.Desc.Columns))
			for i, c := range msg.Desc.Columns {
				cols[i] = c.Name
			}
			m.cachedColumnNames[msg.Desc.Name] = cols
			m.sqlEditorModel.UpdateColumnCache(msg.Desc)
		}
		return m, nil

	case RequestCommitMsg:
		// Show commit confirmation dialog
		m.pendingCommitStmts, _ = m.dataViewModel.generateCommitStatements()
		m.dialogModel.Show("Commit Changes", msg.SQLPreview, "commit")
		return m, nil

	case DialogConfirmMsg:
		m.dialogModel.Hide()
		if msg.DialogType == "quit" {
			return m, tea.Quit
		}
		if msg.DialogType == "commit" {
			if len(m.pendingCommitStmts) > 0 {
				m.statusBarModel.SetMessage("Committing changes...", "info")
				stmts := m.pendingCommitStmts
				m.pendingCommitStmts = nil
				return m, CommitChangesCmd(m.driver, m.dataViewModel.schemaName, m.dataViewModel.tableName, stmts)
			}
		}
		if msg.DialogType == "ddl" {
			if m.pendingDDL != "" {
				ddl := m.pendingDDL
				m.pendingDDL = ""
				m.statusBarModel.SetMessage("Executing DDL...", "info")
				return m, ExecuteDDLCmd(m.driver, ddl)
			}
		}
		return m, nil

	case DialogCancelMsg:
		m.dialogModel.Hide()
		m.pendingCommitStmts = nil
		m.pendingDDL = ""
		return m, nil

	case CommitResultMsg:
		if msg.Err != nil {
			m.dialogModel.ShowError("Commit Error", msg.Err.Error())
			return m, nil
		}
		m.statusBarModel.SetMessage(msg.Message, "success")
		// Reload data after successful commit
		m.dataViewModel.loading = true
		return m, m.dataViewModel.reloadDataCmd()

	case CopyResultMsg:
		m.statusBarModel.SetMessage(fmt.Sprintf("Copied %d %s to clipboard", msg.Count, msg.Kind), "success")
		return m, nil

	case SQLExecuteRequestMsg:
		// Execute SQL from the SQL editor
		m.statusBarModel.SetMessage("Executing SQL...", "info")
		return m, ExecuteSQLCmd(m.driver, msg.Query)

	case SQLExecuteResultMsg:
		m.sqlEditorModel.executing = false
		if msg.Err != nil {
			m.dialogModel.ShowError("SQL Error", msg.Err.Error())
			return m, nil
		}
		if msg.IsQuery && msg.Result != nil {
			// Save current SQL tab state before creating result tab
			m.saveCurrentTab()

			// Create a new result tab
			resultView := NewDataViewModel(m.driver, m.theme)
			resultView.sqlResultMode = true
			dataMsg := DataLoadedMsg{
				Columns:     msg.Result.Columns,
				ColumnTypes: msg.Result.ColumnTypes,
				Rows:        msg.Result.Rows,
			}
			resultView, _ = resultView.Update(dataMsg)

			tab := Tab{
				tabType:  TabData,
				title:    "Query Result",
				dataView: resultView,
				sqlEditor: NewSQLEditorModel(m.theme),
				structureView: NewStructureViewModel(m.driver, m.theme),
				nvimPane: NewNvimPane(m.theme),
			}
			idx := m.tabBar.AddTab(tab)
			m.loadTab(idx)

			m.statusBarModel.SetRowCount(len(msg.Result.Rows))
			m.statusBarModel.SetMessage("Query returned "+
				formatCellValue(len(msg.Result.Rows))+" rows", "success")
		} else {
			m.statusBarModel.SetMessage(
				formatCellValue(msg.RowsAffected)+" rows affected", "success")
		}
		return m, nil

	// ── Structure view messages ──

	case ShowStructureMsg:
		m.showStructure = true
		m.showSQLEditor = false // ensure mutual exclusion
		m.structureViewModel.SetTable(msg.Schema, msg.Table)
		m.structureViewModel.SetFocused(true)
		m.structureViewModel.loading = true
		return m, LoadStructureCmd(m.driver, msg.Schema, msg.Table)

	case StructureLoadedMsg:
		m.structureViewModel, _ = m.structureViewModel.Update(msg)
		if msg.Err != nil {
			m.dialogModel.ShowError("Structure Error", msg.Err.Error())
		} else {
			m.statusBarModel.SetMessage("Structure loaded", "success")
		}
		return m, nil

	case DDLExecuteResultMsg:
		if msg.Err != nil {
			m.dialogModel.ShowError("DDL Error", msg.Err.Error())
			return m, nil
		}
		m.statusBarModel.SetMessage(msg.Message, "success")
		// Refresh table list and reload structure
		var reloadCmds []tea.Cmd
		reloadCmds = append(reloadCmds, LoadTablesCmd(m.driver))
		if m.showStructure && m.structureViewModel.table != "" {
			m.structureViewModel.loading = true
			reloadCmds = append(reloadCmds,
				LoadStructureCmd(m.driver, m.structureViewModel.schema, m.structureViewModel.table))
		}
		return m, tea.Batch(reloadCmds...)

	// ── Export result message ──
	case ExportResultMsg:
		if msg.Err != nil {
			m.statusBarModel.SetMessage("Export error: "+msg.Err.Error(), "error")
		} else {
			m.statusBarModel.SetMessage(msg.Message, "success")
		}
		return m, nil

	// ── Neovim tick message ──
	case nvimTickMsg:
		if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
			// Auto-completion: only in insert mode
			if !m.nvimPane.IsInsertMode() {
				m.nvimCompletions = nil
				m.nvimCompletionIndex = 0
				return m, tea.Tick(50*time.Millisecond, func(time.Time) tea.Msg { return nvimTickMsg{} })
			}
			word := m.nvimPane.CurrentWord()
			if len([]rune(word)) >= 2 {
				candidates, prefix := m.sqlEditorModel.FindCompletions(word)
				// Only reset index when candidates actually change
				if !stringSliceEqual(m.nvimCompletions, candidates) {
					m.nvimCompletionIndex = 0
				}
				m.nvimCompletions = candidates
				m.nvimCompletionPrefix = prefix
			} else {
				m.nvimCompletions = nil
				m.nvimCompletionIndex = 0
			}
			return m, tea.Tick(50*time.Millisecond, func(time.Time) tea.Msg { return nvimTickMsg{} })
		}
		if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.HasExited() {
			m.nvimPane.ConsumeExit()
			m.nvimEnabled = false
			m.nvimCompletions = nil
			// Nvim exited unexpectedly — close the tab rather than showing fallback editor
			m.closeCurrentTab()
		}
		return m, nil

	// ── Neovim save and execute message ──
	case nvimSaveAndExecMsg:
		if m.nvimPane != nil && m.nvimPane.TmpPath() != "" {
			data, err := os.ReadFile(m.nvimPane.TmpPath())
			if err == nil {
				fullText := string(data)
				_, cursorY := m.nvimPane.CursorPos()
				input := extractStatementAtLine(fullText, cursorY)
				if input != "" && !m.sqlEditorModel.executing {
					m.sqlEditorModel.executing = true
					m.sqlEditorModel.SetContent(fullText)
					m.sqlEditorModel.history.Add(input)
					m.sqlEditorModel.history.Save()
					return m, func() tea.Msg {
						return SQLExecuteRequestMsg{Query: input}
					}
				}
			}
		}
		return m, nil

	// ── Neovim save and format message ──
	case nvimSaveAndFormatMsg:
		if m.nvimPane != nil && m.nvimPane.TmpPath() != "" {
			_, cursorY := m.nvimPane.CursorPos()
			data, err := os.ReadFile(m.nvimPane.TmpPath())
			if err == nil {
				formatted := formatSQL(string(data))
				if err := os.WriteFile(m.nvimPane.TmpPath(), []byte(formatted), 0600); err == nil {
					// Reload file, go to normal mode, restore cursor line
					cmd := fmt.Sprintf("\x1b:e!\r:%dG\r", cursorY+1)
					m.nvimPane.Write([]byte(cmd))
					m.statusBarModel.SetMessage("SQL formatted", "info")
				}
			}
		}
		return m, nil

	// ── Neovim save to file message ──
	case nvimSaveFileMsg:
		if m.nvimPane != nil && m.nvimPane.TmpPath() != "" {
			data, err := os.ReadFile(m.nvimPane.TmpPath())
			if err == nil {
				savePath := m.saveSQLFile(msg.Name, string(data))
				if savePath != "" {
					if tab := m.tabBar.ActiveTab(); tab != nil {
						tab.title = msg.Name
						tab.sqlFilePath = savePath
					}
					m.statusBarModel.SetMessage("Saved: "+msg.Name+".sql", "info")
				} else {
					m.statusBarModel.SetMessage("Failed to save SQL file", "error")
				}
			}
		}
		return m, nil
	}

	return m, tea.Batch(cmds...)
}

// dispatchKeyToPane forwards key messages to the currently active pane.
// Arrow keys at pane edges trigger pane switching.
func (m Model) dispatchKeyToPane(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Edge-based pane switching with arrow keys (only when not in input/edit mode)
	if !m.isInputMode() {
		switch m.activePane {
		case PaneTableList:
			if msg.String() == "right" && !m.tableListModel.filtering {
				if m.showSQLEditor {
					m.setActivePane(PaneSQLEditor)
				} else {
					m.setActivePane(PaneDataView)
				}
				return m, nil
			}
		case PaneDataView:
			if !m.dataViewModel.editMode && !m.dataViewModel.filterMode {
				switch msg.String() {
				case "left":
					if m.dataViewModel.cursorCol == 0 {
						m.setActivePane(PaneTableList)
						return m, nil
					}
				}
			}
		case PaneSQLEditor:
			switch msg.String() {
			case "left":
				if m.sqlEditorModel.cursorRow == 0 && m.sqlEditorModel.cursorCol == 0 {
					m.setActivePane(PaneTableList)
					return m, nil
				}
			}
		}
	}

	switch m.activePane {
	case PaneTableList:
		// 'i' or Shift+Enter: show structure view for selected table
		if (msg.String() == "i" || msg.String() == "shift+enter") && !m.tableListModel.filtering {
			schema, table := m.tableListModel.SelectedTable()
			if table != "" {
				return m, func() tea.Msg {
					return ShowStructureMsg{Schema: schema, Table: table}
				}
			}
		}

		// 'c' key: create table
		if msg.String() == "c" && !m.tableListModel.filtering {
			m.showStructure = true
			m.structureViewModel.ddlMode = ddlCreateTable
			m.structureViewModel.createTableName = ""
			m.structureViewModel.createTableColumns = nil
			m.structureViewModel.ddlFields = []ddlField{
				{label: "Table Name", value: ""},
			}
			m.structureViewModel.ddlFieldIdx = 0
			schema, _ := m.tableListModel.SelectedTable()
			m.structureViewModel.schema = schema
			return m, nil
		}

		// 'D' key (Shift+d): drop table
		if msg.String() == "D" && !m.tableListModel.filtering {
			schema, table := m.tableListModel.SelectedTable()
			if table != "" {
				m.structureViewModel.schema = schema
				m.structureViewModel.table = table
				ddl := m.structureViewModel.buildDropTableDDL()
				m.pendingDDL = ddl
				m.dialogModel.Show("Drop Table", ddl, "ddl")
				return m, nil
			}
		}

		var cmd tea.Cmd
		m.tableListModel, cmd = m.tableListModel.Update(msg)
		return m, cmd

	case PaneDataView:
		// Esc in data view (when not in edit/filter/special mode) → focus table list
		if msg.String() == "esc" && !m.dataViewModel.IsEditMode() && !m.dataViewModel.IsFilterMode() && !m.dataViewModel.IsCopyFormatMenu() && !m.dataViewModel.visualMode {
			m.setActivePane(PaneTableList)
			return m, nil
		}
		var cmd tea.Cmd
		m.dataViewModel, cmd = m.dataViewModel.Update(msg)
		return m, cmd

	case PaneSQLEditor:
		var cmd tea.Cmd
		m.sqlEditorModel, cmd = m.sqlEditorModel.Update(msg)
		return m, cmd
	}
	return m, nil
}

// handleStructureViewKey handles key input when the structure view is shown.
func (m Model) handleStructureViewKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Esc returns to data view (unless in DDL mode, which Esc cancels first)
	if msg.String() == "esc" && !m.structureViewModel.IsInputMode() && !m.structureViewModel.IsDDLConfirming() {
		m.showStructure = false
		m.structureViewModel.SetFocused(false)
		return m, nil
	}

	var cmd tea.Cmd
	m.structureViewModel, cmd = m.structureViewModel.Update(msg)
	return m, cmd
}

// handleExportMenu handles key input for the export format menu.
func (m Model) handleExportMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "c":
		m.exportMenuVisible = false
		m.exportFormat = "csv"
		m.exportPathInput = true
		m.exportPath = m.defaultExportPath("csv")
		return m, nil
	case "j":
		m.exportMenuVisible = false
		m.exportFormat = "json"
		m.exportPathInput = true
		m.exportPath = m.defaultExportPath("json")
		return m, nil
	case "s":
		m.exportMenuVisible = false
		m.exportFormat = "sql"
		m.exportPathInput = true
		m.exportPath = m.defaultExportPath("sql")
		return m, nil
	case "esc":
		m.exportMenuVisible = false
		return m, nil
	}
	return m, nil
}

// handleExportPathInput handles key input for the export path input.
func (m Model) handleExportPathInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.exportPathInput = false
		m.exportPath = ""
		return m, nil
	case "enter":
		path := m.exportPath
		format := m.exportFormat
		m.exportPathInput = false
		m.exportPath = ""
		m.exportFormat = ""

		// Build a QueryResult from current data view
		result := &db.QueryResult{
			Columns:     m.dataViewModel.columns,
			ColumnTypes: m.dataViewModel.columnTypes,
			Rows:        m.dataViewModel.rows,
		}
		tableName := m.dataViewModel.tableName
		if tableName == "" {
			tableName = "query_results"
		}

		return m, ExportToFileCmd(result, tableName, format, path)
	case "backspace":
		if len(m.exportPath) > 0 {
			m.exportPath = m.exportPath[:len(m.exportPath)-1]
		}
		return m, nil
	default:
		if msg.Type == tea.KeyRunes {
			m.exportPath += string(msg.Runes)
		}
		return m, nil
	}
}

func (m Model) handleSQLSaveInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.sqlSaveInput = false
		m.sqlSaveName = ""
		return m, nil
	case "enter":
		name := strings.TrimSpace(m.sqlSaveName)
		m.sqlSaveInput = false
		m.sqlSaveName = ""
		if name == "" {
			return m, nil
		}

		if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
			// Save nvim first, then read after delay
			m.nvimPane.Write([]byte("\x1b:w\r"))
			saveName := name
			return m, tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
				return nvimSaveFileMsg{Name: saveName}
			})
		}
		// Built-in editor: save directly
		content := m.sqlEditorModel.GetInput()
		savePath := m.saveSQLFile(name, content)
		if savePath != "" {
			if tab := m.tabBar.ActiveTab(); tab != nil {
				tab.title = name
				tab.sqlFilePath = savePath
			}
			m.statusBarModel.SetMessage("Saved: "+name+".sql", "info")
		} else {
			m.statusBarModel.SetMessage("Failed to save SQL file", "error")
		}
		return m, nil
	case "backspace":
		if len(m.sqlSaveName) > 0 {
			runes := []rune(m.sqlSaveName)
			m.sqlSaveName = string(runes[:len(runes)-1])
		}
		return m, nil
	default:
		if msg.Type == tea.KeyRunes {
			m.sqlSaveName += string(msg.Runes)
		}
		return m, nil
	}
}

// saveSQLFile saves SQL content to ~/.config/tdb/queries/<name>.sql
func (m Model) saveSQLFile(name, content string) string {
	dir := filepath.Join(config.ConfigDir(), "queries")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ""
	}
	path := filepath.Join(dir, name+".sql")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return ""
	}
	return path
}

// handleCellCopy copies the current cell value to clipboard via OSC 52.
func (m Model) handleCellCopy() (tea.Model, tea.Cmd) {
	if m.dataViewModel.cursorRow >= len(m.dataViewModel.rows) ||
		m.dataViewModel.cursorCol >= len(m.dataViewModel.columns) {
		return m, nil
	}

	row := m.dataViewModel.rows[m.dataViewModel.cursorRow]
	if m.dataViewModel.cursorCol >= len(row) {
		return m, nil
	}

	val := row[m.dataViewModel.cursorCol]
	text := formatCellValue(val)

	// Update internal yank buffer for paste
	m.dataViewModel.yankedCells = [][]interface{}{{val}}
	m.dataViewModel.yankType = "cell"

	// Use OSC 52 to copy to clipboard
	cmd := copyToClipboardOSC52(text)
	m.statusBarModel.SetMessage("Copied: "+truncate(text, 30), "success")
	return m, cmd
}

// copyToClipboardOSC52 sends an OSC 52 escape sequence to copy text to clipboard.
// NOTE: Ideally this should use tea.Printf or the Bubble Tea output mechanism to
// avoid writing directly to stdout, which may conflict with Bubble Tea's renderer.
// However, Bubble Tea does not currently provide an API for arbitrary escape
// sequence output outside of tea.Println/tea.Printf (which add newlines).
// Writing directly to os.Stdout works in practice for OSC 52 because terminal
// emulators process it independently of the TUI rendering, but this is fragile
// and should be revisited when Bubble Tea gains native clipboard support.
func copyToClipboardOSC52(text string) tea.Cmd {
	return func() tea.Msg {
		encoded := base64.StdEncoding.EncodeToString([]byte(text))
		// Write OSC 52 sequence directly to stdout
		fmt.Fprintf(os.Stdout, "\033]52;c;%s\a", encoded)
		return nil
	}
}

// defaultExportPath returns the default export file path.
func (m Model) defaultExportPath(format string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	name := m.dataViewModel.tableName
	if name == "" {
		name = "query_results"
	}
	return filepath.Join(home, "Downloads", name+"."+format)
}

// ExportResultMsg holds the result of an export operation.
type ExportResultMsg struct {
	Message string
	Err     error
}

// ExportToFileCmd exports data to a file.
func ExportToFileCmd(result *db.QueryResult, tableName, format, path string) tea.Cmd {
	return func() tea.Msg {
		// Expand ~ in path
		if strings.HasPrefix(path, "~/") {
			home, err := os.UserHomeDir()
			if err == nil {
				path = filepath.Join(home, path[2:])
			}
		}

		// Ensure directory exists
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return ExportResultMsg{Err: fmt.Errorf("failed to create directory: %w", err)}
		}

		f, err := os.Create(path)
		if err != nil {
			return ExportResultMsg{Err: fmt.Errorf("failed to create file: %w", err)}
		}
		defer f.Close()

		switch format {
		case "csv":
			err = export.ExportCSV(result, f)
		case "json":
			err = export.ExportJSON(result, f)
		case "sql":
			err = export.ExportSQL(result, tableName, f)
		default:
			err = fmt.Errorf("unsupported format: %s", format)
		}

		if err != nil {
			return ExportResultMsg{Err: err}
		}

		return ExportResultMsg{Message: fmt.Sprintf("Exported to %s", path)}
	}
}

// switchPane cycles the active pane.
func (m *Model) switchPane() {
	if m.showSQLEditor {
		// SQL editor visible: TableList ↔ SQLEditor
		if m.activePane == PaneTableList {
			m.setActivePane(PaneSQLEditor)
		} else {
			m.setActivePane(PaneTableList)
		}
	} else {
		// Normal mode: TableList ↔ DataView
		if m.activePane == PaneTableList {
			m.setActivePane(PaneDataView)
		} else {
			m.setActivePane(PaneTableList)
		}
	}
}

// setActivePane sets the active pane and updates focus states.
func (m *Model) setActivePane(pane ActivePane) {
	m.activePane = pane
	m.tableListModel.SetFocused(pane == PaneTableList)
	m.dataViewModel.SetFocused(pane == PaneDataView)
	m.sqlEditorModel.SetFocused(pane == PaneSQLEditor)
	m.structureViewModel.SetFocused(pane == PaneDataView && m.showStructure)
}

// saveCurrentTab saves the active model states into the current tab.
func (m *Model) saveCurrentTab() {
	tab := m.tabBar.ActiveTab()
	if tab == nil {
		return
	}
	tab.dataView = m.dataViewModel
	tab.sqlEditor = m.sqlEditorModel
	tab.structureView = m.structureViewModel
	tab.showStructure = m.showStructure
	tab.showSQLEditor = m.showSQLEditor
	tab.nvimPane = m.nvimPane
	tab.nvimEnabled = m.nvimEnabled
}

// loadTab loads the tab at idx into the active model fields.
func (m *Model) loadTab(idx int) {
	m.tabBar.SetActive(idx)
	tab := m.tabBar.ActiveTab()
	if tab == nil {
		return
	}
	m.dataViewModel = tab.dataView
	m.sqlEditorModel = tab.sqlEditor
	m.structureViewModel = tab.structureView
	m.showStructure = tab.showStructure
	m.showSQLEditor = tab.showSQLEditor
	m.nvimPane = tab.nvimPane
	m.nvimEnabled = tab.nvimEnabled
	m.nvimCompletions = nil
	m.nvimCompletionIndex = 0

	// Restore focus based on tab type
	if tab.showSQLEditor {
		m.setActivePane(PaneSQLEditor)
	} else {
		m.setActivePane(PaneDataView)
	}
	m.recalcLayout()
}

// switchTab saves current tab and loads the next one. Returns a Cmd if needed (e.g. nvim tick).
func (m *Model) switchTab(direction int) tea.Cmd {
	if m.tabBar.Len() <= 1 {
		return nil
	}
	m.saveCurrentTab()
	if direction > 0 {
		m.tabBar.NextTab()
	} else {
		m.tabBar.PrevTab()
	}
	m.loadTab(m.tabBar.ActiveIndex())

	// If the loaded tab has active nvim, restart the tick
	if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
		return tea.Tick(50*time.Millisecond, func(time.Time) tea.Msg { return nvimTickMsg{} })
	}
	return nil
}

// openDataTab creates a new data tab or switches to existing one.
// Returns true if a NEW tab was created, false if switched to existing.
func (m *Model) openDataTab(schema, table string) bool {
	// Check if a tab for this table already exists
	if idx := m.tabBar.FindDataTab(schema, table); idx >= 0 {
		m.saveCurrentTab()
		m.loadTab(idx)
		return false
	}

	// Save current tab first
	if m.tabBar.Len() > 0 {
		m.saveCurrentTab()
	}

	// Create new data tab
	tab := Tab{
		tabType:       TabData,
		title:         table,
		schemaName:    schema,
		tableName:     table,
		dataView:      NewDataViewModel(m.driver, m.theme),
		sqlEditor:     NewSQLEditorModel(m.theme),
		structureView: NewStructureViewModel(m.driver, m.theme),
		nvimPane:      NewNvimPane(m.theme),
	}
	idx := m.tabBar.AddTab(tab)
	m.loadTab(idx)
	return true
}

// openSQLTab creates a new SQL editor tab.
func (m *Model) openSQLTab() {
	// Save current tab
	if m.tabBar.Len() > 0 {
		m.saveCurrentTab()
	}

	// Find max SQL tab number to avoid duplicates
	sqlNum := 0
	for _, t := range m.tabBar.tabs {
		if t.tabType == TabSQL {
			var n int
			if _, err := fmt.Sscanf(t.title, "SQL-%d", &n); err == nil && n > sqlNum {
				sqlNum = n
			}
		}
	}
	sqlNum++

	sqlEditor := NewSQLEditorModel(m.theme)
	// Populate autocomplete cache from shared model-level cache
	sqlEditor.cachedTableNames = m.cachedTableNames
	if m.cachedColumnNames != nil {
		sqlEditor.cachedColumnNames = make(map[string][]string, len(m.cachedColumnNames))
		for k, v := range m.cachedColumnNames {
			sqlEditor.cachedColumnNames[k] = v
		}
	}

	tab := Tab{
		tabType:       TabSQL,
		title:         fmt.Sprintf("SQL-%d", sqlNum),
		dataView:      NewDataViewModel(m.driver, m.theme),
		sqlEditor:     sqlEditor,
		structureView: NewStructureViewModel(m.driver, m.theme),
		showSQLEditor: true,
		nvimPane:      NewNvimPane(m.theme),
	}
	idx := m.tabBar.AddTab(tab)
	m.loadTab(idx)
}

// closeCurrentTab closes the active tab.
func (m *Model) closeCurrentTab() {
	if m.tabBar.Len() <= 0 {
		return
	}
	// Stop neovim if running in this tab
	if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
		m.nvimPane.Stop()
	}

	idx := m.tabBar.ActiveIndex()
	m.tabBar.RemoveTab(idx)

	if m.tabBar.Len() == 0 {
		// No tabs left — reset to empty state
		m.dataViewModel = NewDataViewModel(m.driver, m.theme)
		m.sqlEditorModel = NewSQLEditorModel(m.theme)
		m.structureViewModel = NewStructureViewModel(m.driver, m.theme)
		m.showStructure = false
		m.showSQLEditor = false
		m.nvimPane = NewNvimPane(m.theme)
		m.nvimEnabled = false
		m.setActivePane(PaneTableList)
		m.recalcLayout()
	} else {
		m.loadTab(m.tabBar.ActiveIndex())
	}
}

// clearAllTabs stops all nvim panes and removes all tabs.
func (m *Model) clearAllTabs() {
	// Stop nvim in the active tab
	if m.nvimEnabled && m.nvimPane != nil && m.nvimPane.IsRunning() {
		m.nvimPane.Stop()
	}
	// Stop nvim in inactive tabs
	for i := range m.tabBar.tabs {
		if m.tabBar.tabs[i].nvimEnabled && m.tabBar.tabs[i].nvimPane != nil && m.tabBar.tabs[i].nvimPane.IsRunning() {
			m.tabBar.tabs[i].nvimPane.Stop()
		}
	}
	m.tabBar = NewTabBar(m.theme)
	m.dataViewModel = NewDataViewModel(m.driver, m.theme)
	m.sqlEditorModel = NewSQLEditorModel(m.theme)
	m.structureViewModel = NewStructureViewModel(m.driver, m.theme)
	m.showStructure = false
	m.showSQLEditor = false
	m.nvimPane = NewNvimPane(m.theme)
	m.nvimEnabled = false
	// Clear shared autocomplete caches (new DB will reload them)
	m.cachedTableNames = nil
	m.cachedColumnNames = nil
	m.setActivePane(PaneTableList)
	m.recalcLayout()
}

// isInputMode returns true if any pane is in an input/edit mode.
func (m Model) isInputMode() bool {
	return m.tableListModel.filtering ||
		m.dataViewModel.IsEditMode() ||
		m.dataViewModel.IsFilterMode() ||
		m.dataViewModel.IsCopyFormatMenu() ||
		m.exportMenuVisible ||
		m.exportPathInput ||
		m.sqlSaveInput ||
		(m.showStructure && m.structureViewModel.IsInputMode())
}

// isFiltering returns true if the table list is in filter mode.
func (m Model) isFiltering() bool {
	return m.tableListModel.filtering
}

// updateStatusBarExtras updates the status bar with sort/filter info.
func (m *Model) updateStatusBarExtras() {
	sortCol, sortDir := m.dataViewModel.SortInfo()
	filter := m.dataViewModel.FilterInfo()
	extras := ""
	if sortCol != "" && sortDir != "" {
		extras += "Sort: " + sortCol + " " + sortDir
	}
	if filter != "" {
		if extras != "" {
			extras += " │ "
		}
		extras += "Filter: " + filter
	}
	if extras != "" {
		m.statusBarModel.SetExtras(extras)
	} else {
		m.statusBarModel.SetExtras("")
	}
}

// updateConnectionInfoDB updates the connection info display with a new database name.
func (m *Model) updateConnectionInfoDB(dbName string) {
	m.tableListModel.currentDBName = dbName
	// Update the connection info string to reflect the new DB
	// Format: "connName (driverType)" — append DB name
	if m.connectionInfo != "" {
		// Remove old DB suffix if present (e.g., " [dbname]")
		if idx := strings.Index(m.connectionInfo, " ["); idx >= 0 {
			m.connectionInfo = m.connectionInfo[:idx]
		}
		m.connectionInfo += " [" + dbName + "]"
	}
	m.statusBarModel.SetConnectionInfo(m.connectionInfo)
}

// recalcLayout recalculates child model sizes based on terminal size.
func (m *Model) recalcLayout() {
	w := m.width
	h := m.height

	// Left pane: 30% width, min 20, max 60
	leftWidth := w * 30 / 100
	if leftWidth < 20 {
		leftWidth = 20
	}
	if leftWidth > 60 {
		leftWidth = 60
	}

	rightWidth := w - leftWidth

	// Layout breakdown:
	//   header line (1) — "Tables" / tab bar (only when tabs exist)
	//   bordered panes (paneHeight)
	//   status bar (1)
	statusBarHeight := 1
	headerHeight := 0
	if m.tabBar.Len() > 0 {
		headerHeight = 1
	}
	paneHeight := h - statusBarHeight - headerHeight

	if paneHeight < 5 {
		paneHeight = 5
	}

	m.tableListModel.SetSize(leftWidth, paneHeight)
	m.dataViewModel.SetSize(rightWidth, paneHeight)
	m.structureViewModel.SetSize(rightWidth, paneHeight)
	m.sqlEditorModel.SetSize(rightWidth, paneHeight)
	m.statusBarModel.SetWidth(w)

	// Resize neovim pane if active
	if m.nvimEnabled && m.nvimPane.IsRunning() {
		m.nvimPane.Resize(rightWidth-2, paneHeight-2)
	}
}

// applyTheme switches all child models to the given theme.
func (m *Model) applyTheme(theme *Theme) {
	m.theme = theme
	m.tableListModel.theme = theme
	m.dataViewModel.theme = theme
	m.sqlEditorModel.theme = theme
	m.statusBarModel.theme = theme
	m.helpModel.theme = theme
	m.dialogModel.theme = theme
	m.structureViewModel.theme = theme
	m.connectionFormModel.theme = theme
	m.tabBar.theme = theme
}

// toggleTheme switches between dark and light themes at runtime.
func (m *Model) toggleTheme() {
	nextName := ToggleThemeName(m.theme.Name)
	newTheme := ThemeByName(string(nextName))
	m.applyTheme(newTheme)

	// Persist to config if available
	if m.config != nil {
		m.config.Theme = string(nextName)
		// Best-effort save; ignore errors
		_ = m.config.Save()
	}
}

// View renders the entire TUI layout.
func (m Model) View() string {
	if m.width == 0 || m.height == 0 {
		return "Initializing..."
	}

	// Connection screen
	if m.screen == ScreenConnection {
		return m.connectionFormModel.View()
	}

	base := m.renderLayout()

	// Dialog overlay
	if m.dialogModel.IsVisible() {
		return m.dialogModel.View(m.width, m.height)
	}

	// Fuzzy search modal overlay
	if m.fuzzySearchModal.IsVisible() {
		return m.fuzzySearchModal.View()
	}

	// Export menu overlay
	if m.exportMenuVisible {
		return m.renderExportMenuOverlay(base)
	}

	// Export path input overlay
	if m.exportPathInput {
		return m.renderExportPathOverlay(base)
	}

	// SQL save name input overlay
	if m.sqlSaveInput {
		return m.renderSQLSaveOverlay(base)
	}

	// Help overlay
	if m.helpModel.IsVisible() {
		return m.helpModel.View(m.activePane, m.width, m.height)
	}

	return base
}

// renderLayout composes the 2-pane layout (or SQL editor in right pane).
func (m Model) renderLayout() string {
	// Left pane: table list
	leftPane := m.tableListModel.View()

	// Right pane: SQL editor, structure view, or data view
	var rightPane string
	if m.showSQLEditor {
		if m.nvimEnabled && m.nvimPane.IsRunning() {
			// Render embedded neovim
			innerW := m.dataViewModel.width - 2
			innerH := m.dataViewModel.height - 2
			if innerW < 1 {
				innerW = 1
			}
			if innerH < 1 {
				innerH = 1
			}
			nvimContent := m.nvimPane.Render(innerW, innerH)

			// Overlay completion popup if available
			if len(m.nvimCompletions) > 0 {
				cx, cy := m.nvimPane.CursorPos()
				var popupLines []string
				maxShow := 5
				if len(m.nvimCompletions) < maxShow {
					maxShow = len(m.nvimCompletions)
				}
				startIdx := 0
				if m.nvimCompletionIndex >= maxShow {
					startIdx = m.nvimCompletionIndex - maxShow + 1
				}
				for i := startIdx; i < startIdx+maxShow && i < len(m.nvimCompletions); i++ {
					if i == m.nvimCompletionIndex {
						popupLines = append(popupLines, m.theme.DataCellActive.Render(" ▸ "+m.nvimCompletions[i]+" "))
					} else {
						popupLines = append(popupLines, m.theme.DataRow.Render("   "+m.nvimCompletions[i]+" "))
					}
				}
				popup := strings.Join(popupLines, "\n")
				nvimContent = overlayAt(nvimContent, popup, cx, cy+1, innerW, innerH)
			}

			borderStyle := m.theme.FocusedBorder
			rightPane = borderStyle.
				Width(innerW).
				Height(innerH).
				Render(nvimContent)
		} else {
			rightPane = m.sqlEditorModel.View()
		}
	} else if m.showStructure {
		rightPane = m.structureViewModel.View()
	} else {
		rightPane = m.dataViewModel.View()
	}

	// Right pane with tab bar + left header to align tops
	var rightSection string
	if m.tabBar.Len() > 0 {
		leftWidth := m.width * 30 / 100
		if leftWidth < 20 {
			leftWidth = 20
		}
		if leftWidth > 60 {
			leftWidth = 60
		}
		rightWidth := m.width - leftWidth

		tabBarView := m.tabBar.View(rightWidth)

		// Add a matching header line above the table list so borders align
		leftHeader := lipgloss.NewStyle().
			Foreground(m.theme.ColorFgDim).
			Width(leftWidth).
			MaxWidth(leftWidth).
			MaxHeight(1).
			Height(1).
			Render(" Tables")

		leftPane = lipgloss.JoinVertical(lipgloss.Left, leftHeader, leftPane)
		rightSection = lipgloss.JoinVertical(lipgloss.Left, tabBarView, rightPane)
	} else {
		rightSection = rightPane
	}

	// Equalize heights before horizontal join so borders align
	leftLines := strings.Count(leftPane, "\n") + 1
	rightLines := strings.Count(rightSection, "\n") + 1
	if leftLines > rightLines {
		rightSection += strings.Repeat("\n", leftLines-rightLines)
	} else if rightLines > leftLines {
		leftPane += strings.Repeat("\n", rightLines-leftLines)
	}

	// Top row: left + right
	topRow := lipgloss.JoinHorizontal(lipgloss.Top, leftPane, rightSection)

	// Status bar
	statusBar := m.statusBarModel.View()

	// Compose vertically
	result := lipgloss.JoinVertical(lipgloss.Left, topRow, statusBar)

	// Strictly constrain to terminal height to prevent any overflow
	lines := strings.Split(result, "\n")
	if len(lines) > m.height {
		lines = lines[:m.height]
	}
	for len(lines) < m.height {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

// overlayAt places an overlay string at position (x, y) on a base string.
// Works with plain line-based positioning; ANSI-aware replacement is complex,
// so we append overlay runes starting at the character offset.
func overlayAt(base, overlay string, x, y, width, height int) string {
	baseLines := strings.Split(base, "\n")
	overlayLines := strings.Split(overlay, "\n")
	for i, ol := range overlayLines {
		targetY := y + i
		if targetY >= 0 && targetY < len(baseLines) {
			baseLine := baseLines[targetY]
			overlayW := lipgloss.Width(ol)

			// Find byte offsets in baseLine corresponding to visual columns x and x+overlayW
			startOff := visualColToByteOffset(baseLine, x)
			endOff := visualColToByteOffset(baseLine, x+overlayW)

			var result strings.Builder
			if startOff <= len(baseLine) {
				result.WriteString(baseLine[:startOff])
			} else {
				result.WriteString(baseLine)
				for pad := lipgloss.Width(baseLine); pad < x; pad++ {
					result.WriteByte(' ')
				}
			}
			// Reset any open ANSI state before overlay
			result.WriteString("\033[0m")
			result.WriteString(ol)
			result.WriteString("\033[0m")
			if endOff < len(baseLine) {
				result.WriteString(baseLine[endOff:])
			}
			baseLines[targetY] = result.String()
		}
	}
	return strings.Join(baseLines, "\n")
}

// visualColToByteOffset walks a string tracking visual column position
// (skipping ANSI escape sequences) and returns the byte offset corresponding
// to the given visual column.
func visualColToByteOffset(s string, col int) int {
	visCol := 0
	i := 0
	for i < len(s) && visCol < col {
		if s[i] == '\033' {
			// Skip ANSI escape sequence: ESC [ ... final_byte
			j := i + 1
			if j < len(s) && s[j] == '[' {
				j++
				for j < len(s) && s[j] >= 0x20 && s[j] <= 0x3F {
					j++ // parameter bytes
				}
				for j < len(s) && s[j] >= 0x20 && s[j] <= 0x2F {
					j++ // intermediate bytes
				}
				if j < len(s) {
					j++ // final byte
				}
			}
			i = j
		} else {
			_, size := utf8.DecodeRuneInString(s[i:])
			visCol++
			i += size
		}
	}
	return i
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// renderExportMenuOverlay renders the export format selection menu as an overlay.
func (m Model) renderExportMenuOverlay(_ string) string {
	var sb strings.Builder
	sb.WriteString(m.theme.DialogTitle.Render(" Export Data "))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.HelpKey.Render("c"))
	sb.WriteString(m.theme.HelpDesc.Render("CSV"))
	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpKey.Render("j"))
	sb.WriteString(m.theme.HelpDesc.Render("JSON"))
	sb.WriteString("\n")
	sb.WriteString(m.theme.HelpKey.Render("s"))
	sb.WriteString(m.theme.HelpDesc.Render("SQL (INSERT)"))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.HelpDesc.Render("Press Esc to cancel"))

	content := m.theme.HelpBox.Render(sb.String())
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, content)
}

// renderExportPathOverlay renders the export file path input as an overlay.
func (m Model) renderExportPathOverlay(_ string) string {
	var sb strings.Builder
	sb.WriteString(m.theme.DialogTitle.Render(fmt.Sprintf(" Export as %s ", strings.ToUpper(m.exportFormat))))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.DataRow.Render("File path: "))
	sb.WriteString(m.theme.SQLInput.Render(m.exportPath + "▏"))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.HelpDesc.Render("Enter: Export | Esc: Cancel"))

	content := m.theme.HelpBox.Render(sb.String())
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, content)
}

// renderSQLSaveOverlay renders the SQL file save name input as an overlay.
func (m Model) renderSQLSaveOverlay(_ string) string {
	var sb strings.Builder
	sb.WriteString(m.theme.DialogTitle.Render(" Save SQL "))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.DataRow.Render("File name: "))
	sb.WriteString(m.theme.SQLInput.Render(m.sqlSaveName + "▏"))
	sb.WriteString(m.theme.HelpDesc.Render(".sql"))
	sb.WriteString("\n\n")
	sb.WriteString(m.theme.HelpDesc.Render("Enter: Save | Esc: Cancel"))

	content := m.theme.HelpBox.Render(sb.String())
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, content)
}

func stringSliceEqual(a, b []string) bool {
if len(a) != len(b) {
return false
}
for i := range a {
if a[i] != b[i] {
return false
}
}
return true
}
