use std::cell::RefCell;
use std::collections::BTreeMap;

struct Stats {
    current: usize,
    max: usize,
}

thread_local! {
    static STATS: RefCell<BTreeMap<&'static str, Stats>> = RefCell::new(BTreeMap::new());
}

pub fn enter(name: &'static str) {
    STATS.with(|s| {
        let mut map = s.borrow_mut();
        let stats = map.entry(name).or_insert(Stats { current: 0, max: 0 });
        stats.current += 1;
        if stats.current > stats.max {
            stats.max = stats.current;
        }
    });
}

pub fn exit(name: &'static str) {
    STATS.with(|s| {
        let mut map = s.borrow_mut();
        if let Some(stats) = map.get_mut(name) {
            stats.current = stats.current.saturating_sub(1);
        }
    });
}

pub fn report() -> Vec<(String, usize)> {
    STATS.with(|s| {
        s.borrow()
            .iter()
            .map(|(name, stats)| (name.to_string(), stats.max))
            .collect()
    })
}

pub fn reset() {
    STATS.with(|s| {
        s.borrow_mut().clear();
    });
}

/// Flush current stats to the bootstrap `compilation` and `stack` tables,
/// then reset for the next query.
///
/// Called after every compilation attempt (success or error).
pub fn flush_to_db(
    conn: &rusqlite::Connection,
    dql_input: &str,
    sql_output: Option<&str>,
    cte_count: Option<i32>,
    error: Option<&str>,
) {
    let sql_length = sql_output.map(|s| s.len() as i32);

    // Insert compilation row
    let insert_result = conn.execute(
        "INSERT INTO compilation (dql_input, sql_output, sql_length, cte_count, error)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![dql_input, sql_output, sql_length, cte_count, error],
    );

    if let Ok(_) = insert_result {
        let compilation_id = conn.last_insert_rowid();

        // Insert stack rows from current stats
        let stats = report();
        for (function_name, max_depth) in &stats {
            let _ = conn.execute(
                "INSERT INTO stack (compilation_id, function_name, max_depth)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![compilation_id, function_name, max_depth],
            );
        }
    }

    // Always reset for next query
    reset();
}
