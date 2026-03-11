# tdb

A powerful TUI database client inspired by [TablePlus](https://tableplus.com/).

<!-- TODO: スクリーンショット / GIF を追加 -->

## Features

- 🗄️ **Multi-database support**: PostgreSQL, MySQL, SQLite, SQL Server
- 📊 **TablePlus-like data grid** with inline editing
- ✏️ **Full CRUD operations** with transaction support
- 🔍 **Filtering, sorting, and quick search**
- 📝 **SQL editor** with syntax highlighting and auto-completion
- 🏗️ **DDL operations** (create / alter / drop tables)
- 📤 **Export** to CSV, JSON, SQL
- 🔑 **SSH tunnel** support
- 🎨 **Dark / Light theme** (Ctrl+T to toggle)
- ⌨️ **Vim-style keyboard navigation**

## Installation

### Homebrew (macOS / Linux)

```bash
brew tap minee/tap
brew install tdb
```

### From Source

```bash
go install github.com/minee/tdb/cmd/tdb@latest
```

### Binary

[Releases](https://github.com/minee/tdb/releases) ページからダウンロードしてください。

## Quick Start

```bash
# DSN で接続
tdb --dsn "postgres://user:pass@localhost:5432/mydb"

# MySQL
tdb --dsn "user:pass@tcp(localhost:3306)/mydb" --type mysql

# SQLite
tdb --dsn "./mydb.sqlite" --type sqlite

# SQL Server
tdb --dsn "sqlserver://user:pass@localhost:1433?database=mydb" --type mssql

# 接続管理画面から起動
tdb
```

## Key Bindings

### Global

| Key | Action |
|-----|--------|
| `Tab` | Switch pane (Table List ↔ Data View ↔ SQL Editor) |
| `Ctrl+E` | Toggle SQL editor |
| `Ctrl+T` | Toggle dark / light theme |
| `?` | Keyboard shortcuts help |
| `q` / `Ctrl+C` | Quit |

### Table List

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate tables |
| `Enter` | Open table |
| `/` | Filter tables |
| `i` | Table structure |
| `c` | Create table |
| `D` (Shift+d) | Drop table |
| `Esc` | Clear filter |

### Data View

| Key | Action |
|-----|--------|
| `j` / `k` | Move row up / down |
| `h` / `l` | Move column left / right |
| `g` / `G` | First / last row |
| `Ctrl+U` / `Ctrl+D` | Page up / down |
| `n` / `p` | Next / prev data page |
| `Enter` / `e` | Edit cell |
| `a` | Add new row |
| `d` | Delete / undelete row |
| `Ctrl+S` | Commit changes |
| `Ctrl+Z` | Discard all changes |
| `s` | Sort by current column |
| `/` | Filter (WHERE clause) |
| `f` | Quick filter by cell value |
| `Ctrl+/` | Clear filter |
| `Ctrl+O` | Export data |
| `y` | Copy cell to clipboard |

### SQL Editor

| Key | Action |
|-----|--------|
| `Ctrl+Enter` | Execute SQL |
| `Enter` | New line |
| `← / → / ↑ / ↓` | Move cursor |
| `Ctrl+↑ / Ctrl+↓` | Navigate query history |
| `Ctrl+R` | Search history |
| `Tab` | Autocomplete |
| `Ctrl+K` | Delete to end of line |

## Configuration

設定ファイルは `~/.config/tdb/connections.yaml` に保存されます。

> **⚠️ セキュリティに関する注意**
>
> 接続情報は `~/.config/tdb/connections.yaml` に**平文で**保存されます。パスワードが含まれる場合もそのまま記録されるため、以下の点にご注意ください。
>
> - このファイルを Git リポジトリにコミットしないでください（`.gitignore` への追加を推奨）
> - ファイルのパーミッションを適切に設定してください（`chmod 600`）
> - 将来的に OS キーチェーン（macOS Keychain / GNOME Keyring 等）連携を予定しています

```yaml
connections:
  - name: local-postgres
    type: postgres
    host: localhost
    port: 5432
    user: postgres
    password: secret
    database: mydb
    ssl_mode: disable

  - name: production
    type: mysql
    dsn: "user:pass@tcp(db.example.com:3306)/prod"
    ssh:
      host: bastion.example.com
      port: 22
      user: deploy
      private_key: ~/.ssh/id_rsa

theme: dark   # "dark" or "light"
```

## Building from Source

```bash
git clone https://github.com/minee/tdb.git
cd tdb
make build    # → bin/tdb
make test     # run tests
make lint     # go vet
```

## Dependencies

| Library | Purpose |
|---------|---------|
| [bubbletea](https://github.com/charmbracelet/bubbletea) | TUI framework (Elm architecture) |
| [lipgloss](https://github.com/charmbracelet/lipgloss) | Terminal styling / layout |
| [cobra](https://github.com/spf13/cobra) | CLI command framework |
| [pgx](https://github.com/jackc/pgx) | PostgreSQL driver |
| [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) | MySQL driver |
| [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) | SQLite driver (pure Go) |
| [go-mssqldb](https://github.com/microsoft/go-mssqldb) | SQL Server driver |
| [creack/pty](https://github.com/creack/pty) | PTY for embedded neovim |
| [hinshun/vt10x](https://github.com/hinshun/vt10x) | VT terminal emulator |
| [go-runewidth](https://github.com/mattn/go-runewidth) | Unicode display width |
| [golang.org/x/crypto](https://pkg.go.dev/golang.org/x/crypto) | SSH tunnel support |
| [yaml.v3](https://github.com/go-yaml/yaml) | Configuration file parsing |

## License

[MIT](LICENSE)
