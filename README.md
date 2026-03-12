# tdb

A powerful TUI database client inspired by [TablePlus](https://tableplus.com/), built with Go.

<!-- TODO: スクリーンショット / GIF を追加 -->

## Features

- 🗄️ **Multi-database support** — PostgreSQL, MySQL, SQLite, SQL Server
- 📊 **Data grid** — Inline cell editing, add/delete rows, sorting, filtering, transaction commit
- 📝 **Embedded Neovim SQL editor** — Syntax highlighting, auto-completion, vim keybindings
- 🔍 **Fuzzy search** — Databases, tables, open tabs, saved SQL files (`Space` × 2)
- 🏗️ **DDL operations** — Create / alter / drop tables, columns, indexes
- 📤 **Export** — CSV, JSON, SQL (INSERT statements)
- 💾 **SQL file management** — Save queries to `~/.config/tdb/queries/`, load via fuzzy search
- 🔑 **SSH tunnel** — Password / private key authentication
- 🎨 **Dark / Light theme** — `Ctrl+T` to toggle
- ⌨️ **Vim-style navigation** — Full vim keybindings throughout the app
- 📑 **Tab system** — Multiple SQL editors with `Ctrl+Q` / `gw` / `gl` / `gh`
- 📜 **Query history** — Persistent history with browse (`Ctrl+↑/↓`) and search (`Ctrl+R`)

## Prerequisites

- **Neovim** (`nvim`) — Required for the embedded SQL editor

## Installation

### Homebrew (macOS / Linux)

```bash
brew install mineee1102/tap/tdb
```

Neovim is installed automatically as a dependency.

### Binary

[Releases](https://github.com/mineee1102/tdb/releases) ページからダウンロードしてください。

### From Source

```bash
git clone https://github.com/mineee1102/tdb.git
cd tdb
make build    # → bin/tdb
```

## Quick Start

```bash
# 接続管理画面から起動
tdb

# DSN で直接接続
tdb --dsn "postgres://user:pass@localhost:5432/mydb"

# MySQL
tdb --dsn "user:pass@tcp(localhost:3306)/mydb" --type mysql

# SQLite
tdb --dsn "./mydb.sqlite" --type sqlite

# SQL Server
tdb --dsn "sqlserver://user:pass@localhost:1433?database=mydb" --type mssql
```

## Key Bindings

### Global

| Key | Action |
|-----|--------|
| `Tab` | Switch pane (Table List ↔ Data View ↔ SQL Editor) |
| `Ctrl+Q` | Open new SQL tab |
| `gl` / `gh` | Next / prev tab |
| `gw` | Close current tab |
| `Space` × 2 | Fuzzy search (databases, tables, tabs, SQL files) |
| `Ctrl+T` | Toggle dark / light theme |
| `?` | Keyboard shortcuts help |
| `q` / `Ctrl+C` | Quit (with confirmation) |

### Table List

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate tables |
| `g` / `G` | First / last table |
| `Enter` | Open table |
| `/` | Filter tables |
| `Esc` | Clear filter |
| `[` / `]` | Switch database |
| `i` / `Shift+Enter` | Table structure |
| `c` | Create table |
| `D` | Drop table |

### Data View

| Key | Action |
|-----|--------|
| `j` / `k` | Move row up / down |
| `h` / `l` | Move column left / right |
| `g` / `G` | First / last row |
| `Ctrl+U` / `Ctrl+D` | Page up / down |
| `n` / `P` | Next / prev data page |
| `Enter` / `e` | Edit cell |
| `a` | Add new row |
| `d` | Delete / undelete row |
| `Ctrl+S` | Commit changes (with SQL preview) |
| `Ctrl+Z` / `Ctrl+Y` | Undo / redo |
| `s` | Sort by current column (ASC / DESC / clear) |
| `/` | Filter (WHERE clause) |
| `f` | Quick filter by cell value |
| `Ctrl+/` | Clear filter |
| `Ctrl+O` | Export data (CSV / JSON / SQL) |
| `y` | Copy cell to clipboard |
| `yy` | Copy entire row |
| `V` | Visual line select (multi-row) |
| `p` | Paste |

### SQL Editor (Neovim)

The SQL editor is a fully embedded Neovim instance with vim keybindings.

| Key | Action |
|-----|--------|
| `Ctrl+G` | Execute SQL |
| `Ctrl+S` | Save SQL file |
| `Ctrl+Shift+L` | Format SQL |
| `Ctrl+↑` / `Ctrl+↓` | Navigate query history |
| `Ctrl+R` | Search query history |
| `Tab` | Accept auto-completion suggestion |
| `i` / `a` / `o` | Enter insert mode (standard vim) |
| `Esc` | Return to normal mode |

## Configuration

設定ファイルは `~/.config/tdb/` に保存されます。

| Path | Description |
|------|-------------|
| `~/.config/tdb/connections.yaml` | 接続情報 |
| `~/.config/tdb/queries/` | 保存された SQL ファイル |
| `~/.config/tdb/history` | クエリ履歴 |

> **⚠️ セキュリティに関する注意**
>
> 接続情報は `connections.yaml` に**平文で**保存されます。パスワードが含まれる場合もそのまま記録されるため、以下の点にご注意ください。
>
> - このファイルを Git リポジトリにコミットしないでください
> - ファイルのパーミッションを適切に設定してください（`chmod 600`）

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
git clone https://github.com/mineee1102/tdb.git
cd tdb
make build    # → bin/tdb
make test     # run tests
make lint     # go vet
```

## License

[MIT](LICENSE)
