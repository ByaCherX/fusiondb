//! Interactive REPL for FusionDB.
//!
//! Provides a readline-based interactive SQL shell with dot-commands,
//! colored output, and pretty-printed query results.

use colored::Colorize;
use comfy_table::{Cell, Color, ContentArrangement, Table as PrettyTable};
use fusiondb::Database;
use rustyline::error::ReadlineError;
use rustyline::completion::Completer;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Editor, Helper};
use std::time::Instant;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "GROUP BY", "HAVING", "ORDER BY",
    "LIMIT", "INSERT", "INTO", "VALUES", "CREATE", "TABLE", "DROP", "ALTER",
    "ADD", "COLUMN", "AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL", "TRUE",
    "FALSE", "AS", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "USING", "UNION",
    "ALL", "EXISTS", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST",
    "WITH", "DESCRIBE", "EXPLAIN", "SHOW", "TABLES",
];

const DATA_TYPES: &[&str] = &[
    "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
    "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
    "VARCHAR", "CHAR", "TEXT", "BOOLEAN", "BOOL",
    "DATE", "TIME", "TIMESTAMP",
];

const FUNCTIONS: &[&str] = &[
    // Aggregates
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    // String
    "CONCAT", "LOWER", "UPPER", "SUBSTR", "SUBSTRING", "LENGTH", "TRIM", "LTRIM", "RTRIM", "REPLACE", "COALESCE",
    // Math
    "ABS", "CEIL", "FLOOR", "ROUND", "SQRT", "POWER", "EXP", "LN", "LOG", "MOD",
    // Date/Time
    "NOW", "CURDATE", "CURTIME", "DATE_ADD", "DATE_SUB", "DATEDIFF",
];

const DOT_COMMANDS: &[&str] = &[
    ".help", ".tables", ".schema", ".stats", ".count", ".quit", ".exit",
];

struct ReplHelper<'a> {
    db: &'a Database,
}

impl<'a> Helper for ReplHelper<'a> {}

impl<'a> Completer for ReplHelper<'a> {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let slice = &line[..pos];
        
        // Find the start of the word being completed.
        let word_start = slice
            .rfind(|c: char| c.is_ascii_whitespace() || c == ',' || c == '(' || c == ')' || c == ';' || c == '=')
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let word = &slice[word_start..];

        if word.is_empty() {
            return Ok((pos, Vec::new()));
        }

        let mut suggestions = Vec::new();

        // If the word starts with '.', complete dot-commands
        if word.starts_with('.') {
            for cmd in DOT_COMMANDS {
                if cmd.starts_with(word) {
                    suggestions.push(cmd.to_string());
                }
            }
            suggestions.sort();
            suggestions.dedup();
            return Ok((word_start, suggestions));
        }

        let word_upper = word.to_uppercase();

        // 1. SQL Keywords
        for kw in SQL_KEYWORDS {
            if kw.starts_with(&word_upper) {
                suggestions.push(kw.to_string());
            }
        }

        // 2. Data Types
        for dt in DATA_TYPES {
            if dt.starts_with(&word_upper) {
                suggestions.push(dt.to_string());
            }
        }

        // 3. Functions
        for func in FUNCTIONS {
            if func.starts_with(&word_upper) {
                suggestions.push(func.to_string());
            }
        }

        // 4. Tables and Columns from the Database
        if let Ok(tables) = self.db.list_tables() {
            for table in &tables {
                if table.to_uppercase().starts_with(&word_upper) {
                    suggestions.push(table.clone());
                }

                // Also check columns of this table
                if let Ok(tbl) = self.db.table(table) {
                    let schema = tbl.schema();
                    let arrow_schema = schema.arrow_schema();
                    for field in arrow_schema.fields() {
                        let field_name = field.name();
                        if field_name.to_uppercase().starts_with(&word_upper) {
                            suggestions.push(field_name.clone());
                        }
                    }
                }
            }
        }

        suggestions.sort();
        suggestions.dedup();

        Ok((word_start, suggestions))
    }
}

impl<'a> Hinter for ReplHelper<'a> {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> Option<Self::Hint> {
        if pos < line.len() {
            // Only show hints when the cursor is at the end of the line
            return None;
        }
        let slice = &line[..pos];
        let word_start = slice
            .rfind(|c: char| c.is_ascii_whitespace() || c == ',' || c == '(' || c == ')' || c == ';' || c == '=')
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let word = &slice[word_start..];

        if word.is_empty() {
            return None;
        }

        let mut suggestions = Vec::new();

        // 1. Dot Commands
        if word.starts_with('.') {
            for cmd in DOT_COMMANDS {
                if cmd.starts_with(word) {
                    suggestions.push(cmd.to_string());
                }
            }
        } else {
            let word_upper = word.to_uppercase();

            // 2. SQL Keywords
            for kw in SQL_KEYWORDS {
                if kw.starts_with(&word_upper) {
                    suggestions.push(kw.to_string());
                }
            }

            // 3. Data Types
            for dt in DATA_TYPES {
                if dt.starts_with(&word_upper) {
                    suggestions.push(dt.to_string());
                }
            }

            // 4. Functions
            for func in FUNCTIONS {
                if func.starts_with(&word_upper) {
                    suggestions.push(func.to_string());
                }
            }

            // 5. Tables and Columns from the Database
            if let Ok(tables) = self.db.list_tables() {
                for table in &tables {
                    if table.to_uppercase().starts_with(&word_upper) {
                        suggestions.push(table.clone());
                    }

                    if let Ok(tbl) = self.db.table(table) {
                        let schema = tbl.schema();
                        let arrow_schema = schema.arrow_schema();
                        for field in arrow_schema.fields() {
                            let field_name = field.name();
                            if field_name.to_uppercase().starts_with(&word_upper) {
                                suggestions.push(field_name.clone());
                            }
                        }
                    }
                }
            }
        }

        suggestions.sort();
        suggestions.dedup();

        if suggestions.is_empty() {
            return None;
        }

        // Pick the first suggestion and return the suffix
        let best_match = &suggestions[0];
        if best_match.len() > word.len() {
            let suffix = &best_match[word.len()..];
            Some(suffix.to_string())
        } else {
            None
        }
    }
}

impl<'a> Highlighter for ReplHelper<'a> {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        if default {
            std::borrow::Cow::Owned(prompt.cyan().bold().to_string())
        } else {
            std::borrow::Cow::Borrowed(prompt)
        }
    }

    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        std::borrow::Cow::Owned(highlight_sql(line))
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        true
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Owned(format!("\x1b[90m{}\x1b[0m", hint))
    }
}

impl<'a> Validator for ReplHelper<'a> {}

/// Real-time syntax highlighter for SQL queries in the REPL.
fn highlight_sql(line: &str) -> String {
    let mut result = String::with_capacity(line.len() * 2);
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    
    // Dot commands
    if line.starts_with('.') {
        let mut parts = line.splitn(2, ' ');
        if let Some(cmd) = parts.next() {
            result.push_str("\x1b[1;35m"); // Bold Magenta
            result.push_str(cmd);
            result.push_str("\x1b[0m");
            if let Some(rest) = parts.next() {
                result.push(' ');
                result.push_str(rest);
            }
        }
        return result;
    }
    
    while i < chars.len() {
        // Line comment
        if chars[i] == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            result.push_str("\x1b[90m"); // Gray
            while i < chars.len() {
                result.push(chars[i]);
                i += 1;
            }
            result.push_str("\x1b[0m");
            break;
        }

        // Single quoted string
        if chars[i] == '\'' {
            result.push_str("\x1b[32m"); // Green
            result.push(chars[i]);
            i += 1;
            let mut escaped = false;
            while i < chars.len() {
                let c = chars[i];
                result.push(c);
                i += 1;
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '\'' {
                    break;
                }
            }
            result.push_str("\x1b[0m");
            continue;
        }

        // Double quoted string
        if chars[i] == '"' {
            result.push_str("\x1b[32m"); // Green
            result.push(chars[i]);
            i += 1;
            let mut escaped = false;
            while i < chars.len() {
                let c = chars[i];
                result.push(c);
                i += 1;
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    break;
                }
            }
            result.push_str("\x1b[0m");
            continue;
        }

        // MySQL backticks
        if chars[i] == '`' {
            result.push_str("\x1b[36m"); // Cyan
            result.push(chars[i]);
            i += 1;
            while i < chars.len() {
                let c = chars[i];
                result.push(c);
                i += 1;
                if c == '`' {
                    break;
                }
            }
            result.push_str("\x1b[0m");
            continue;
        }

        // Numbers
        if chars[i].is_ascii_digit() {
            result.push_str("\x1b[35m"); // Magenta
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                result.push(chars[i]);
                i += 1;
            }
            result.push_str("\x1b[0m");
            continue;
        }

        // Words
        if chars[i].is_alphanumeric() || chars[i] == '_' {
            let mut word = String::new();
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                word.push(chars[i]);
                i += 1;
            }
            
            let upper_word = word.to_uppercase();
            if SQL_KEYWORDS.contains(&upper_word.as_str()) {
                result.push_str("\x1b[1;33m"); // Bold Yellow
                result.push_str(&word);
                result.push_str("\x1b[0m");
            } else if DATA_TYPES.contains(&upper_word.as_str()) {
                result.push_str("\x1b[1;34m"); // Bold Blue
                result.push_str(&word);
                result.push_str("\x1b[0m");
            } else if FUNCTIONS.contains(&upper_word.as_str()) {
                result.push_str("\x1b[1;36m"); // Bold Cyan
                result.push_str(&word);
                result.push_str("\x1b[0m");
            } else {
                result.push_str(&word);
            }
            continue;
        }

        // Normal characters
        result.push(chars[i]);
        i += 1;
    }
    
    result
}

/// Run the interactive REPL loop.
pub fn run_repl(db: &Database) {
    print_banner();

    let helper = ReplHelper { db };
    let mut rl = match Editor::<ReplHelper<'_>, _>::new() {
        Ok(mut editor) => {
            editor.set_helper(Some(helper));
            editor
        }
        Err(e) => {
            eprintln!("{} Failed to initialize REPL: {}", "error:".red().bold(), e);
            return;
        }
    };

    // Load command history if available
    let history_path = dirs_hint();
    let _ = rl.load_history(&history_path);

    loop {
        let prompt = "fusiondb> ";
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                // Handle dot-commands
                if trimmed.starts_with('.') {
                    if handle_dot_command(db, trimmed) {
                        break; // .quit or .exit
                    }
                    continue;
                }

                // Execute SQL query
                execute_sql(db, trimmed);
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", "Use .quit or .exit to leave".dimmed());
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye!".green().bold());
                break;
            }
            Err(err) => {
                eprintln!("{} {:?}", "error:".red().bold(), err);
                break;
            }
        }
    }

    // Save command history
    let _ = rl.save_history(&history_path);
}

/// Print the welcome banner.
fn print_banner() {
    println!();
    println!("{}", "╔══════════════════════════════════════════════════╗".cyan());
    println!("{}", "║         FusionDB Interactive Shell               ║".cyan());
    println!("{}", "║  Type SQL queries or use dot-commands (.help)    ║".cyan());
    println!("{}", "╚══════════════════════════════════════════════════╝".cyan());
    println!();
}

/// Handle dot-commands. Returns true if the REPL should exit.
fn handle_dot_command(db: &Database, cmd: &str) -> bool {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    let command = parts[0].to_lowercase();
    let arg = parts.get(1).map(|s| s.trim());

    match command.as_str() {
        ".quit" | ".exit" | ".q" => {
            println!("{}", "Goodbye!".green().bold());
            return true;
        }
        ".help" | ".h" => {
            print_help();
        }
        ".tables" => {
            show_tables(db);
        }
        ".schema" => {
            if let Some(table_name) = arg {
                show_schema(db, table_name);
            } else {
                eprintln!("{} Usage: .schema <table_name>", "error:".red().bold());
            }
        }
        ".stats" => {
            show_stats(db);
        }
        ".count" => {
            if let Some(table_name) = arg {
                show_count(db, table_name);
            } else {
                eprintln!("{} Usage: .count <table_name>", "error:".red().bold());
            }
        }
        _ => {
            eprintln!(
                "{} Unknown command '{}'. Type .help for available commands.",
                "error:".red().bold(),
                cmd
            );
        }
    }

    false
}

/// Print available dot-commands.
fn print_help() {
    let mut table = PrettyTable::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        Cell::new("Command").fg(Color::Cyan),
        Cell::new("Description").fg(Color::Cyan),
    ]);

    let commands = [
        (".help", "Show this help message"),
        (".tables", "List all tables"),
        (".schema <table>", "Show table schema"),
        (".stats", "Show database statistics"),
        (".count <table>", "Show row count for a table"),
        (".quit / .exit", "Exit the REPL"),
    ];

    for (cmd, desc) in &commands {
        table.add_row(vec![Cell::new(cmd), Cell::new(desc)]);
    }

    println!("{table}");
}

/// Execute a SQL query and print results.
pub fn execute_sql(db: &Database, sql: &str) {
    let start = Instant::now();

    match db.query(sql) {
        Ok(batches) => {
            let elapsed = start.elapsed();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            if batches.is_empty() || total_rows == 0 {
                println!("{}", "(0 rows)".dimmed());
                println!(
                    "{}",
                    format!("Time: {}", fusiondb::utils::format::format_duration(elapsed)).dimmed()
                );
                return;
            }

            // Build a pretty table from the first batch's schema
            let schema = batches[0].schema();

            let mut table = PrettyTable::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);

            // Header row
            let header: Vec<Cell> = schema
                .fields()
                .iter()
                .map(|f| Cell::new(f.name()).fg(Color::Cyan))
                .collect();
            table.set_header(header);

            // Data rows
            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let mut row_cells = Vec::new();
                    for col_idx in 0..batch.num_columns() {
                        let col = batch.column(col_idx);
                        let value = arrow::util::display::array_value_to_string(col, row_idx)
                            .unwrap_or_else(|_| "NULL".to_string());
                        row_cells.push(Cell::new(&value));
                    }
                    table.add_row(row_cells);
                }
            }

            println!("{table}");
            println!(
                "{}",
                format!(
                    "({} row{}) Time: {}",
                    total_rows,
                    if total_rows != 1 { "s" } else { "" },
                    fusiondb::utils::format::format_duration(elapsed)
                )
                .dimmed()
            );
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
        }
    }
}

/// Show all tables.
pub fn show_tables(db: &Database) {
    match db.list_tables() {
        Ok(tables) => {
            if tables.is_empty() {
                println!("{}", "(no tables)".dimmed());
                return;
            }

            let mut table = PrettyTable::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("#").fg(Color::DarkGrey),
                Cell::new("Table Name").fg(Color::Cyan),
                Cell::new("Rows").fg(Color::Cyan),
            ]);

            for (i, name) in tables.iter().enumerate() {
                let row_count = db
                    .table(name)
                    .and_then(|t| t.count())
                    .map(|c| c.to_string())
                    .unwrap_or_else(|_| "?".to_string());

                table.add_row(vec![
                    Cell::new(i + 1),
                    Cell::new(name),
                    Cell::new(row_count),
                ]);
            }

            println!("{table}");
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
        }
    }
}

/// Show schema for a specific table.
pub fn show_schema(db: &Database, table_name: &str) {
    match db.table(table_name) {
        Ok(tbl) => {
            let schema = tbl.schema();
            let arrow_schema = schema.arrow_schema();

            println!(
                "{}",
                format!("Table: {}", table_name).cyan().bold()
            );
            println!(
                "{}",
                format!("Primary Key: {}", schema.primary_key_column())
                    .yellow()
            );
            println!();

            let mut table = PrettyTable::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("#").fg(Color::DarkGrey),
                Cell::new("Column").fg(Color::Cyan),
                Cell::new("Type").fg(Color::Cyan),
                Cell::new("Nullable").fg(Color::Cyan),
                Cell::new("PK").fg(Color::Cyan),
            ]);

            for (i, field) in arrow_schema.fields().iter().enumerate() {
                let is_pk = i == schema.primary_key_index();
                table.add_row(vec![
                    Cell::new(i),
                    Cell::new(field.name()),
                    Cell::new(format!("{:?}", field.data_type())),
                    Cell::new(if field.is_nullable() { "YES" } else { "NO" }),
                    Cell::new(if is_pk { "✓" } else { "" })
                        .fg(if is_pk { Color::Green } else { Color::Reset }),
                ]);
            }

            println!("{table}");
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
        }
    }
}

/// Show database statistics.
pub fn show_stats(db: &Database) {
    // Table summary
    match db.list_tables() {
        Ok(tables) => {
            println!("{}", "Database Statistics".cyan().bold());
            println!("{}", "─".repeat(40));
            println!("  Tables: {}", tables.len());

            let mut total_rows: u64 = 0;
            for name in &tables {
                if let Ok(tbl) = db.table(name) {
                    if let Ok(count) = tbl.count() {
                        total_rows += count;
                    }
                }
            }
            println!("  Total rows: {}", total_rows);
            if let Ok(size) = db.disk_usage() {
                println!("  Disk Usage: {}", fusiondb::utils::format::format_bytes(size));
            }
            println!("  Path: {}", db.path().display());
            println!();
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
        }
    }

    // RocksDB statistics
    if let Some(stats) = db.get_statistics() {
        println!("{}", "RocksDB Statistics".cyan().bold());
        println!("{}", "─".repeat(40));
        // Print only the most interesting stats (first 30 lines)
        for (i, line) in stats.lines().enumerate() {
            if i >= 30 {
                println!("  ... ({} more lines)", stats.lines().count() - 30);
                break;
            }
            println!("  {}", line);
        }
    }
}

/// Show row count for a table.
fn show_count(db: &Database, table_name: &str) {
    match db.table(table_name) {
        Ok(tbl) => match tbl.count() {
            Ok(count) => {
                println!("{}: {} rows", table_name.cyan(), count);
            }
            Err(e) => {
                eprintln!("{} {}", "error:".red().bold(), e);
            }
        },
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
        }
    }
}

/// Get the history file path hint.
fn dirs_hint() -> String {
    if let Some(home) = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
    {
        let mut path = std::path::PathBuf::from(home);
        path.push(".fusiondb_history");
        path.to_string_lossy().to_string()
    } else {
        ".fusiondb_history".to_string()
    }
}
