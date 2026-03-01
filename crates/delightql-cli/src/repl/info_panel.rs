/// Result data from an emit stream (name + column names + rows)
#[derive(Clone, Debug, Default)]
pub struct EmitResult {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Primary query result data (columns + rows)
#[derive(Clone, Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// A successful query + its generated SQL, for the history ring in Window C
#[derive(Clone, Debug)]
pub struct QueryHistoryEntry {
    pub dql: String,
    pub sql: String,
}

/// Shared state between REPL and info panel
#[derive(Clone, Debug)]
pub struct SharedReplState {
    pub last_input: String,
    pub last_sql: Option<String>,
    pub query_count: usize,
    pub last_window_position: Option<crate::repl::multi_pane_tui::WindowId>,

    /// Emit stream results from the last query
    pub last_emit_results: Vec<EmitResult>,
    /// Primary query results from the last query
    pub last_results: Option<QueryResult>,

    // Window C: query history ring
    pub query_history: Vec<QueryHistoryEntry>,

    // Cu1: process info (set once at init)
    pub cli_args: Vec<String>,
    pub db_path: Option<String>,

    // Cd1: config snapshot (synced before each TUI launch)
    pub config_output_format: String,
    pub config_target_stage: String,
    pub config_sql_mode: bool,
    pub config_zebra_mode: Option<usize>,
    pub config_no_headers: bool,
    pub config_multiline: bool,
}

impl SharedReplState {
    pub fn new(cli_args: Vec<String>, db_path: Option<String>) -> Self {
        Self {
            last_input: String::new(),
            last_sql: None,
            query_count: 0,
            last_window_position: None,
            last_emit_results: Vec::new(),
            last_results: None,
            query_history: Vec::new(),
            cli_args,
            db_path,
            config_output_format: String::new(),
            config_target_stage: String::new(),
            config_sql_mode: false,
            config_zebra_mode: None,
            config_no_headers: false,
            config_multiline: false,
        }
    }

    pub fn update(&mut self, input: &str, sql: Option<String>) {
        self.last_input = input.to_string();
        self.last_sql = sql;
        self.query_count += 1;
    }

    /// Update with full query execution results
    pub fn update_full(
        &mut self,
        input: &str,
        sql: Option<String>,
        results: Option<QueryResult>,
        emit_results: Vec<EmitResult>,
    ) {
        self.last_input = input.to_string();
        self.last_sql = sql;
        self.query_count += 1;
        self.last_results = results;
        self.last_emit_results = emit_results;
    }

    /// Push a successful query + SQL pair to the history ring (capped at 50)
    pub fn push_history(&mut self, dql: String, sql: String) {
        self.query_history.push(QueryHistoryEntry { dql, sql });
        if self.query_history.len() > 50 {
            self.query_history.remove(0);
        }
    }

    /// Sync REPL configuration into the snapshot fields.
    pub fn sync_config(
        &mut self,
        output_format: &str,
        target_stage: &str,
        sql_mode: bool,
        zebra_mode: Option<usize>,
        no_headers: bool,
        multiline: bool,
    ) {
        self.config_output_format = output_format.to_string();
        self.config_target_stage = target_stage.to_string();
        self.config_sql_mode = sql_mode;
        self.config_zebra_mode = zebra_mode;
        self.config_no_headers = no_headers;
        self.config_multiline = multiline;
    }
}
