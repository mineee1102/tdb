package app

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minee/tdb/internal/config"
	"github.com/minee/tdb/internal/db"
	"github.com/minee/tdb/internal/ui"
)

// App holds the application state and dependencies.
type App struct {
	Driver db.Driver
	Config *config.Config
	DSN    string
}

// New creates a new App instance with an active driver connection.
func New(driver db.Driver, cfg *config.Config, dsn string) *App {
	return &App{
		Driver: driver,
		Config: cfg,
		DSN:    dsn,
	}
}

// Start launches the Bubble Tea TUI program with an active connection.
func (a *App) Start() error {
	model := ui.NewModel(a.Driver, a.Config, a.DSN)

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	return nil
}

// StartConnectionScreen launches the TUI starting at the connection manager screen.
func StartConnectionScreen(cfg *config.Config) error {
	model := ui.NewConnectionModel(cfg)

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	return nil
}
