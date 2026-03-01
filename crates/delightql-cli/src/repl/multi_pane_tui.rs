/// Full-screen window manager for DelightQL
///
/// This module provides a windowed interface with full-screen windows:
/// - Ctrl+T to enter TUI mode
/// - Ctrl+H/J/K/L for window navigation
/// - 9 full-screen windows total: A, B, C, Cu1-Cu3, Cd1-Cd3
/// - Each window uses the entire terminal when active
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    prelude::Margin,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph},
    Frame, Terminal,
};
use std::io;
use std::sync::{Arc, Mutex};

/// Window identifiers for the 9-pane layout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowId {
    A,
    B,
    C,
    Cu1,
    Cu2,
    Cu3,
    Cd1,
    Cd2,
    Cd3,
}

impl WindowId {
    /// Get all windows in navigation order
    fn all_windows() -> Vec<WindowId> {
        vec![
            WindowId::A,
            WindowId::B,
            WindowId::Cu3,
            WindowId::Cu2,
            WindowId::Cu1,
            WindowId::C,
            WindowId::Cd1,
            WindowId::Cd2,
            WindowId::Cd3,
        ]
    }

    /// Get window name for display
    fn name(&self) -> &'static str {
        match self {
            WindowId::A => "A",
            WindowId::B => "B",
            WindowId::C => "C",
            WindowId::Cu1 => "Cu1",
            WindowId::Cu2 => "Cu2",
            WindowId::Cu3 => "Cu3",
            WindowId::Cd1 => "Cd1",
            WindowId::Cd2 => "Cd2",
            WindowId::Cd3 => "Cd3",
        }
    }
}

/// Focus state within Window B (namespace list vs entity list)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BFocus {
    Left,
    Right,
}

/// State for the multi-pane TUI
pub struct MultiPaneTui {
    /// Currently focused window
    focused_window: WindowId,
    /// Shared REPL state for displaying information
    repl_state: crate::repl::info_panel::SharedReplState,
    /// Whether the window map is currently visible
    show_map: bool,
    /// Which pane is selected in the map (for navigation within map)
    map_focused_pane: WindowId,

    /// Handle access for DQL pipeline queries
    handle: Arc<Mutex<Box<dyn delightql_core::api::DqlHandle>>>,
    /// Database connection for DQL execution
    _connection: crate::connection::ConnectionManager,

    // Window B state
    namespaces: Vec<String>,
    b_selected_ns: usize,
    b_entities: Vec<(String, String)>, // (name, type_label)
    b_selected_entity: usize,
    b_columns: Vec<(String, String)>, // (name, type_name)
    b_focus: BFocus,

    // Window C state
    c_selected_query: usize,
    c_scroll_offset: usize,

    // Window A: cached entities for primary DB
    a_main_entities: Vec<(String, String)>, // (name, type_label)
}

impl MultiPaneTui {
    pub fn new(
        repl_state: crate::repl::info_panel::SharedReplState,
        handle: Arc<Mutex<Box<dyn delightql_core::api::DqlHandle>>>,
        connection: crate::connection::ConnectionManager,
    ) -> Self {
        let focused_window = repl_state.last_window_position.unwrap_or(WindowId::A);

        let mut tui = Self {
            focused_window,
            repl_state,
            show_map: false,
            map_focused_pane: focused_window,
            handle,
            _connection: connection,
            namespaces: Vec::new(),
            b_selected_ns: 0,
            b_entities: Vec::new(),
            b_selected_entity: 0,
            b_columns: Vec::new(),
            b_focus: BFocus::Left,
            c_selected_query: 0,
            c_scroll_offset: 0,
            a_main_entities: Vec::new(),
        };

        // Stdlib modules load lazily on first access via DQL queries.
        // The namespace browser below triggers this by querying sys::ns.*.

        // Initialize data from bootstrap
        tui.refresh_namespaces();
        tui.refresh_entities();
        tui.refresh_columns();
        tui.refresh_main_entities();

        // Set initial query selection to last entry
        if !tui.repl_state.query_history.is_empty() {
            tui.c_selected_query = tui.repl_state.query_history.len() - 1;
        }

        tui
    }

    /// Run a DQL query through the pipeline and return results
    fn run_dql(&self, dql: &str) -> Option<delightql_backends::QueryResults> {
        let mut h = self.handle.lock().ok()?;
        let mut session = h.session().ok()?;
        crate::exec_ng::run_dql_query(dql, &mut *session).ok()
    }

    fn refresh_namespaces(&mut self) {
        let dql = r#"sys::ns.namespace(*) |> (fq_name) |> #(fq_name)"#;
        if let Some(results) = self.run_dql(dql) {
            self.namespaces = results
                .rows
                .into_iter()
                .filter_map(|r| r.into_iter().next())
                .collect();
        }
    }

    fn refresh_entities(&mut self) {
        self.b_entities.clear();
        self.b_selected_entity = 0;

        let ns_name = match self.namespaces.get(self.b_selected_ns) {
            Some(n) => n.clone(),
            None => return,
        };

        // Join activated_entity + entity + namespace + entity_type_enum, filter by fq_name
        let dql = format!(
            r#"sys::ns.activated_entity(*) as ae, sys::entities.entity(*) as e, ae.entity_id = e.id, sys::ns.namespace(*) as n, ae.namespace_id = n.id, sys::entities.entity_type_enum(*) as ete, e.type = ete.id, n.fq_name = "{}" |> (e.name as entity_name, ete.variant as entity_type) |> #(entity_name)"#,
            ns_name
        );
        if let Some(results) = self.run_dql(&dql) {
            self.b_entities = results
                .rows
                .into_iter()
                .map(|row| {
                    let name = row.get(0).cloned().unwrap_or_default();
                    let type_label = row.get(1).cloned().unwrap_or_default();
                    (name, type_label)
                })
                .collect();
        }
    }

    fn refresh_columns(&mut self) {
        self.b_columns.clear();

        let ns_name = match self.namespaces.get(self.b_selected_ns) {
            Some(n) => n.clone(),
            None => return,
        };
        let entity_name = match self.b_entities.get(self.b_selected_entity) {
            Some((n, _)) => n.clone(),
            None => return,
        };

        // Join entity_attribute + entity + activated_entity + namespace
        let dql = format!(
            r#"sys::entities.entity_attribute(*) as ea, sys::entities.entity(*) as e, ea.entity_id = e.id, sys::ns.activated_entity(*) as ae, ae.entity_id = e.id, sys::ns.namespace(*) as n, ae.namespace_id = n.id, n.fq_name = "{}", e.name = "{}" |> (ea.attribute_name, ea.data_type)"#,
            ns_name, entity_name
        );
        if let Some(results) = self.run_dql(&dql) {
            self.b_columns = results
                .rows
                .into_iter()
                .map(|row| {
                    let name = row.get(0).cloned().unwrap_or_default();
                    let dtype = row.get(1).cloned().unwrap_or_default();
                    (name, dtype)
                })
                .collect();
        }
    }

    fn refresh_main_entities(&mut self) {
        self.a_main_entities.clear();

        let dql = r#"sys::ns.activated_entity(*) as ae, sys::entities.entity(*) as e, ae.entity_id = e.id, sys::ns.namespace(*) as n, ae.namespace_id = n.id, sys::entities.entity_type_enum(*) as ete, e.type = ete.id, n.fq_name = "main" |> (e.name as entity_name, ete.variant as entity_type) |> #(entity_name)"#;
        if let Some(results) = self.run_dql(dql) {
            self.a_main_entities = results
                .rows
                .into_iter()
                .map(|row| {
                    let name = row.get(0).cloned().unwrap_or_default();
                    let type_label = row.get(1).cloned().unwrap_or_default();
                    (name, type_label)
                })
                .collect();
        }
    }

    /// Navigate to the next window in the specified direction
    fn navigate(&mut self, direction: NavigationDirection) {
        let windows = WindowId::all_windows();
        let current_index = windows
            .iter()
            .position(|&w| w == self.focused_window)
            .unwrap_or(0);

        let next_index = match direction {
            NavigationDirection::Left => {
                match self.focused_window {
                    WindowId::B => 0, // B -> A
                    WindowId::Cu3
                    | WindowId::Cu2
                    | WindowId::Cu1
                    | WindowId::C
                    | WindowId::Cd1
                    | WindowId::Cd2
                    | WindowId::Cd3 => 1, // C stack -> B
                    _ => current_index, // A stays A
                }
            }
            NavigationDirection::Right => {
                match self.focused_window {
                    WindowId::A => 1,   // A -> B
                    WindowId::B => 2,   // B -> Cu3 (top of C stack)
                    _ => current_index, // C stack stays in C stack
                }
            }
            NavigationDirection::Up => {
                match self.focused_window {
                    WindowId::A => 0,   // A stays A
                    WindowId::B => 1,   // B stays B
                    WindowId::Cu3 => 2, // Cu3 stays at top
                    WindowId::Cu2 => 2, // Cu2 -> Cu3
                    WindowId::Cu1 => 3, // Cu1 -> Cu2
                    WindowId::C => 4,   // C -> Cu1
                    WindowId::Cd1 => 5, // Cd1 -> C
                    WindowId::Cd2 => 6, // Cd2 -> Cd1
                    WindowId::Cd3 => 7, // Cd3 -> Cd2
                }
            }
            NavigationDirection::Down => {
                match self.focused_window {
                    WindowId::A => 0,   // A stays A
                    WindowId::B => 1,   // B stays B
                    WindowId::Cu3 => 3, // Cu3 -> Cu2
                    WindowId::Cu2 => 4, // Cu2 -> Cu1
                    WindowId::Cu1 => 5, // Cu1 -> C
                    WindowId::C => 6,   // C -> Cd1
                    WindowId::Cd1 => 7, // Cd1 -> Cd2
                    WindowId::Cd2 => 8, // Cd2 -> Cd3
                    WindowId::Cd3 => 8, // Cd3 stays at bottom
                }
            }
        };

        self.focused_window = windows[next_index];
    }

    /// Navigate within the map panes (same logic as window navigation but for map_focused_pane)
    fn navigate_map(&mut self, direction: NavigationDirection) {
        let windows = WindowId::all_windows();
        let current_index = windows
            .iter()
            .position(|&w| w == self.map_focused_pane)
            .unwrap_or(0);

        let next_index = match direction {
            NavigationDirection::Left => {
                match self.map_focused_pane {
                    WindowId::B => 0, // B -> A
                    WindowId::Cu3
                    | WindowId::Cu2
                    | WindowId::Cu1
                    | WindowId::C
                    | WindowId::Cd1
                    | WindowId::Cd2
                    | WindowId::Cd3 => 1, // C stack -> B
                    _ => current_index, // A stays A
                }
            }
            NavigationDirection::Right => {
                match self.map_focused_pane {
                    WindowId::A => 1,   // A -> B
                    WindowId::B => 2,   // B -> Cu3 (top of C stack)
                    _ => current_index, // C stack stays in C stack
                }
            }
            NavigationDirection::Up => {
                match self.map_focused_pane {
                    WindowId::A => 0,   // A stays A
                    WindowId::B => 1,   // B stays B
                    WindowId::Cu3 => 2, // Cu3 stays at top
                    WindowId::Cu2 => 2, // Cu2 -> Cu3
                    WindowId::Cu1 => 3, // Cu1 -> Cu2
                    WindowId::C => 4,   // C -> Cu1
                    WindowId::Cd1 => 5, // Cd1 -> C
                    WindowId::Cd2 => 6, // Cd2 -> Cd1
                    WindowId::Cd3 => 7, // Cd3 -> Cd2
                }
            }
            NavigationDirection::Down => {
                match self.map_focused_pane {
                    WindowId::A => 0,   // A stays A
                    WindowId::B => 1,   // B stays B
                    WindowId::Cu3 => 3, // Cu3 -> Cu2
                    WindowId::Cu2 => 4, // Cu2 -> Cu1
                    WindowId::Cu1 => 5, // Cu1 -> C
                    WindowId::C => 6,   // C -> Cd1
                    WindowId::Cd1 => 7, // Cd1 -> Cd2
                    WindowId::Cd2 => 8, // Cd2 -> Cd3
                    WindowId::Cd3 => 8, // Cd3 stays at bottom
                }
            }
        };

        self.map_focused_pane = windows[next_index];
    }

    /// Jump to the currently selected map pane (make it the active window)
    fn jump_to_map_selection(&mut self) {
        self.focused_window = self.map_focused_pane;
        self.show_map = false; // Close the map
    }
}

#[derive(Debug)]
enum NavigationDirection {
    Left,
    Right,
    Up,
    Down,
}

/// Run the multi-pane TUI
/// Returns the final window position so it can be persisted
pub fn run_multi_pane_tui(
    repl_state: crate::repl::info_panel::SharedReplState,
    handle: Arc<Mutex<Box<dyn delightql_core::api::DqlHandle>>>,
    connection: crate::connection::ConnectionManager,
) -> Result<WindowId> {
    // Setup terminal for TUI
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create TUI state
    let mut tui = MultiPaneTui::new(repl_state, handle, connection);

    // Run the TUI
    let res = run_app(&mut terminal, &mut tui);

    // Restore terminal for rustyline
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen,)?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        eprintln!("Error in multi-pane TUI: {}", err);
    }

    // Return the final window position
    Ok(tui.focused_window)
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    tui: &mut MultiPaneTui,
) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, tui))?;

        // Handle input
        if let Event::Key(key) = event::read()? {
            match (key.code, key.modifiers) {
                // Exit commands
                (KeyCode::Esc, _) | (KeyCode::Char('q'), _) => {
                    return Ok(());
                }
                (KeyCode::Char('t'), KeyModifiers::CONTROL) => {
                    // Ctrl+T also returns to REPL (toggle behavior)
                    return Ok(());
                }

                // Navigation commands when map is not visible (direct window navigation)
                (KeyCode::Char('h'), KeyModifiers::CONTROL) if !tui.show_map => {
                    tui.navigate(NavigationDirection::Left);
                }
                (KeyCode::Char('j'), KeyModifiers::CONTROL) if !tui.show_map => {
                    tui.navigate(NavigationDirection::Down);
                }
                (KeyCode::Char('k'), KeyModifiers::CONTROL) if !tui.show_map => {
                    tui.navigate(NavigationDirection::Up);
                }
                (KeyCode::Char('l'), KeyModifiers::CONTROL) if !tui.show_map => {
                    tui.navigate(NavigationDirection::Right);
                }

                // Navigation commands when map IS visible (navigate within map panes - bare keys)
                (KeyCode::Char('h'), _) if tui.show_map => {
                    tui.navigate_map(NavigationDirection::Left);
                }
                (KeyCode::Char('j'), _) if tui.show_map => {
                    tui.navigate_map(NavigationDirection::Down);
                }
                (KeyCode::Char('k'), _) if tui.show_map => {
                    tui.navigate_map(NavigationDirection::Up);
                }
                (KeyCode::Char('l'), _) if tui.show_map => {
                    tui.navigate_map(NavigationDirection::Right);
                }

                // Enter key when map is visible - jump to selected pane
                (KeyCode::Enter, _) if tui.show_map => {
                    tui.jump_to_map_selection();
                }

                // Toggle window map with Ctrl+Space
                (KeyCode::Char(' '), KeyModifiers::CONTROL) => {
                    tui.show_map = !tui.show_map;
                    // When opening map, sync the map focus to current window
                    if tui.show_map {
                        tui.map_focused_pane = tui.focused_window;
                    }
                }

                // Any other key dismisses map if it's showing
                _ if tui.show_map => {
                    tui.show_map = false;
                }

                // Per-window key handling (when map is NOT showing)
                _ => {
                    handle_window_input(key.code, key.modifiers, tui);
                }
            }
        }
    }
}

/// Handle per-window key input
fn handle_window_input(code: KeyCode, modifiers: KeyModifiers, tui: &mut MultiPaneTui) {
    match tui.focused_window {
        WindowId::B => handle_window_b_input(code, modifiers, tui),
        WindowId::C => handle_window_c_input(code, modifiers, tui),
        _ => {}
    }
}

/// Handle Window B input: namespace/entity browsing
fn handle_window_b_input(code: KeyCode, modifiers: KeyModifiers, tui: &mut MultiPaneTui) {
    let is_up = code == KeyCode::Up
        || (code == KeyCode::Char('p') && modifiers.contains(KeyModifiers::CONTROL));
    let is_down = code == KeyCode::Down
        || (code == KeyCode::Char('n') && modifiers.contains(KeyModifiers::CONTROL));

    match code {
        KeyCode::Tab => {
            tui.b_focus = match tui.b_focus {
                BFocus::Left => BFocus::Right,
                BFocus::Right => BFocus::Left,
            };
        }
        _ if is_up => match tui.b_focus {
            BFocus::Left => {
                if tui.b_selected_ns > 0 {
                    tui.b_selected_ns -= 1;
                    tui.refresh_entities();
                    tui.refresh_columns();
                }
            }
            BFocus::Right => {
                if tui.b_selected_entity > 0 {
                    tui.b_selected_entity -= 1;
                    tui.refresh_columns();
                }
            }
        },
        _ if is_down => match tui.b_focus {
            BFocus::Left => {
                if tui.b_selected_ns + 1 < tui.namespaces.len() {
                    tui.b_selected_ns += 1;
                    tui.refresh_entities();
                    tui.refresh_columns();
                }
            }
            BFocus::Right => {
                if tui.b_selected_entity + 1 < tui.b_entities.len() {
                    tui.b_selected_entity += 1;
                    tui.refresh_columns();
                }
            }
        },
        _ => {}
    }
}

/// Handle Window C input: query history selection and SQL scrolling
fn handle_window_c_input(code: KeyCode, modifiers: KeyModifiers, tui: &mut MultiPaneTui) {
    let is_up = code == KeyCode::Up
        || (code == KeyCode::Char('p') && modifiers.contains(KeyModifiers::CONTROL));
    let is_down = code == KeyCode::Down
        || (code == KeyCode::Char('n') && modifiers.contains(KeyModifiers::CONTROL));

    match code {
        _ if is_up => {
            if tui.c_selected_query > 0 {
                tui.c_selected_query -= 1;
                tui.c_scroll_offset = 0;
            }
        }
        _ if is_down => {
            if tui.c_selected_query + 1 < tui.repl_state.query_history.len() {
                tui.c_selected_query += 1;
                tui.c_scroll_offset = 0;
            }
        }
        KeyCode::PageDown => {
            tui.c_scroll_offset = tui.c_scroll_offset.saturating_add(10);
        }
        KeyCode::PageUp => {
            tui.c_scroll_offset = tui.c_scroll_offset.saturating_sub(10);
        }
        _ => {}
    }
}

fn ui(f: &mut Frame, tui: &MultiPaneTui) {
    let size = f.area();

    if tui.show_map {
        // Show window map overlay
        render_window_map(f, size, tui);
    } else {
        // Normal window display
        // Create layout with status line at bottom and main content area
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(0),    // Main content area (full screen for active window)
                Constraint::Length(1), // Status line
            ])
            .split(size);

        // Render only the currently active window in the main area
        render_full_screen_window(f, chunks[0], tui.focused_window, tui);

        // Render status line showing current window and navigation hints
        render_status_line(f, chunks[1], tui);
    }
}

fn render_full_screen_window(f: &mut Frame, area: Rect, window_id: WindowId, tui: &MultiPaneTui) {
    match window_id {
        WindowId::B => render_window_b(f, area, tui),
        WindowId::C => render_window_c(f, area, tui),
        _ => {
            let content = match window_id {
                WindowId::A => create_window_a_content(tui),
                WindowId::Cu1 => create_window_cu1_content(tui),
                WindowId::Cu2 => create_window_cu2_content(tui),
                WindowId::Cu3 => create_window_cu3_content(tui),
                WindowId::Cd1 => create_window_cd1_content(tui),
                WindowId::Cd2 => create_window_cd2_content(tui),
                WindowId::Cd3 => create_window_cd3_content(tui),
                _ => unreachable!(),
            };

            let border_style = Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD);
            let title_style = Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD);
            let title = format!(" {} [ACTIVE] ", window_id.name());

            let paragraph = Paragraph::new(content).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(Span::styled(title, title_style)),
            );

            f.render_widget(paragraph, area);
        }
    }
}

fn render_status_line(f: &mut Frame, area: Rect, tui: &MultiPaneTui) {
    let extra_hint = match tui.focused_window {
        WindowId::B => " | Tab: toggle focus | Up/Down: select",
        WindowId::C => " | Up/Down: select query | PgUp/PgDn: scroll SQL",
        _ => "",
    };
    let status_content = format!(
        " Window: {} | Navigation: Ctrl+H/J/K/L | Map: Ctrl+Space | Exit: Ctrl+T/ESC/Q{}",
        tui.focused_window.name(),
        extra_hint,
    );

    let status_paragraph =
        Paragraph::new(status_content).style(Style::default().bg(Color::Blue).fg(Color::White));

    f.render_widget(status_paragraph, area);
}

fn render_window_map(f: &mut Frame, area: Rect, tui: &MultiPaneTui) {
    // Create a centered overlay for the map
    let popup_area = centered_rect(80, 80, area);

    // Clear the background
    f.render_widget(Clear, popup_area);

    // Create the main map container
    let map_container = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .title(Span::styled(
            " WINDOW NAVIGATOR ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))
        .style(Style::default().bg(Color::Black));

    f.render_widget(map_container, popup_area);

    // Create inner area for panes (inside the border)
    let inner_area = popup_area.inner(Margin {
        vertical: 1,
        horizontal: 1,
    });

    // Split into main pane area and help text area
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),    // Main pane area
            Constraint::Length(4), // Help text
        ])
        .split(inner_area);

    // Render the actual panes
    render_window_panes(f, main_chunks[0], tui);

    // Render help text
    let help_content = vec![
        Line::from("Navigation: H/J/K/L • Select: Enter • Toggle: Ctrl+Space • Exit: Ctrl+T/ESC/Q"),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                "CYAN",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::from(" = selected pane • "),
            Span::styled(
                "YELLOW",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::from(" = active window"),
        ]),
    ];

    let help_paragraph = Paragraph::new(help_content)
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Center);

    f.render_widget(help_paragraph, main_chunks[1]);
}

fn render_window_panes(f: &mut Frame, area: Rect, tui: &MultiPaneTui) {
    // Create the layout that matches our mental model
    // Layout: A | B | C-stack
    let horizontal_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25), // A column
            Constraint::Percentage(25), // B column
            Constraint::Percentage(50), // C stack column
        ])
        .split(area);

    // Render Window A pane
    render_single_pane(f, horizontal_layout[0], WindowId::A, tui);

    // Render Window B pane
    render_single_pane(f, horizontal_layout[1], WindowId::B, tui);

    // Split the C column into the 7 C-stack windows
    let c_stack_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(14), // Cu3
            Constraint::Percentage(14), // Cu2
            Constraint::Percentage(14), // Cu1
            Constraint::Percentage(16), // C (main - slightly larger)
            Constraint::Percentage(14), // Cd1
            Constraint::Percentage(14), // Cd2
            Constraint::Percentage(14), // Cd3
        ])
        .split(horizontal_layout[2]);

    // Render all C-stack panes
    render_single_pane(f, c_stack_layout[0], WindowId::Cu3, tui);
    render_single_pane(f, c_stack_layout[1], WindowId::Cu2, tui);
    render_single_pane(f, c_stack_layout[2], WindowId::Cu1, tui);
    render_single_pane(f, c_stack_layout[3], WindowId::C, tui);
    render_single_pane(f, c_stack_layout[4], WindowId::Cd1, tui);
    render_single_pane(f, c_stack_layout[5], WindowId::Cd2, tui);
    render_single_pane(f, c_stack_layout[6], WindowId::Cd3, tui);
}

fn render_single_pane(f: &mut Frame, area: Rect, window_id: WindowId, tui: &MultiPaneTui) {
    let current_window = tui.focused_window;
    let selected_pane = tui.map_focused_pane;

    // Determine styling based on selection state
    let (border_style, title_style, title_text) = if window_id == selected_pane {
        // Currently selected pane in map: bold cyan
        (
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            format!(" {} [SELECTED] ", window_id.name()),
        )
    } else if window_id == current_window {
        // Current active window: bold yellow
        (
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            format!(" {} [ACTIVE] ", window_id.name()),
        )
    } else {
        // Other windows: normal white
        (
            Style::default().fg(Color::White),
            Style::default().fg(Color::White),
            format!(" {} ", window_id.name()),
        )
    };

    // Create content for the pane
    let content = if window_id == WindowId::C {
        vec![
            Line::from("MAIN"),
            Line::from(""),
            Line::from("REPL"),
            Line::from("State"),
        ]
    } else {
        vec![
            Line::from(window_id.name()),
            Line::from(""),
            Line::from("Pane"),
        ]
    };

    let paragraph = Paragraph::new(content)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(border_style)
                .title(Span::styled(title_text, title_style)),
        )
        .alignment(Alignment::Center);

    f.render_widget(paragraph, area);
}

/// helper function to create a centered rect using up certain percentage of the available rect `r`
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

// ──────────────────────────────────────────────────────────────
// Window A: Last query + results (top), main entities (bottom)
// ──────────────────────────────────────────────────────────────

fn create_window_a_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    let mut lines = vec![];

    // ── Top half: last successful query + first 10 result rows ──
    lines.push(Line::from(vec![Span::styled(
        " LAST QUERY + RESULTS ",
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )]));
    lines.push(Line::from(""));

    if tui.repl_state.last_input.is_empty() {
        lines.push(Line::from(Span::styled(
            "(no query yet)",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        // Show the DQL input (first 3 lines)
        for (i, line) in tui.repl_state.last_input.lines().take(3).enumerate() {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:3} | ", i + 1),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(line.to_string(), Style::default().fg(Color::White)),
            ]));
        }
        if tui.repl_state.last_input.lines().count() > 3 {
            lines.push(Line::from(Span::styled(
                "    ...",
                Style::default().fg(Color::DarkGray),
            )));
        }

        lines.push(Line::from(""));

        // Show first 10 result rows
        if let Some(ref results) = tui.repl_state.last_results {
            if !results.columns.is_empty() {
                let header_line: Vec<Span> = results
                    .columns
                    .iter()
                    .map(|col| {
                        Span::styled(
                            format!("{:15}", col),
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD),
                        )
                    })
                    .collect();
                lines.push(Line::from(header_line));

                let sep = "-".repeat(15 * results.columns.len().min(6));
                lines.push(Line::from(Span::styled(
                    sep,
                    Style::default().fg(Color::DarkGray),
                )));

                for (i, row) in results.rows.iter().take(10).enumerate() {
                    let style = if i % 2 == 0 {
                        Style::default().fg(Color::White)
                    } else {
                        Style::default().fg(Color::Gray)
                    };
                    let row_line: Vec<Span> = row
                        .iter()
                        .map(|val| Span::styled(format!("{:15}", val), style))
                        .collect();
                    lines.push(Line::from(row_line));
                }

                if results.rows.len() > 10 {
                    lines.push(Line::from(Span::styled(
                        format!("... {} more rows", results.rows.len() - 10),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
            }
        } else {
            lines.push(Line::from(Span::styled(
                "(no results)",
                Style::default().fg(Color::DarkGray),
            )));
        }
    }

    // ── Separator ──
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "────────────────────────────────────────────────",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    // ── Bottom half: entities in primary DB (main namespace) ──
    lines.push(Line::from(vec![Span::styled(
        " ENTITIES (main) ",
        Style::default()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD),
    )]));
    lines.push(Line::from(""));

    if tui.a_main_entities.is_empty() {
        lines.push(Line::from(Span::styled(
            "(no entities in main namespace)",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for (name, type_label) in &tui.a_main_entities {
            lines.push(Line::from(vec![
                Span::styled(format!("  {}", name), Style::default().fg(Color::White)),
                Span::styled(
                    format!(" ({})", type_label),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
    }

    lines
}

// ──────────────────────────────────────────────────────────────
// Window B: Namespace browser (left: namespaces, right: entities/columns)
// ──────────────────────────────────────────────────────────────

fn render_window_b(f: &mut Frame, area: Rect, tui: &MultiPaneTui) {
    let border_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let title_style = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);

    let outer_block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(Span::styled(" B [ACTIVE] ", title_style));

    let inner_area = outer_block.inner(area);
    f.render_widget(outer_block, area);

    // Horizontal split: left 35%, right 65%
    let h_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(inner_area);

    // ── Left pane: namespace list ──
    let ns_highlight_style = if tui.b_focus == BFocus::Left {
        Style::default().bg(Color::Cyan).fg(Color::Black)
    } else {
        Style::default().bg(Color::DarkGray).fg(Color::White)
    };

    let ns_items: Vec<ListItem> = tui
        .namespaces
        .iter()
        .map(|ns| ListItem::new(ns.as_str()))
        .collect();

    let ns_list = List::new(ns_items)
        .block(Block::default().borders(Borders::ALL).title(Span::styled(
            " Namespaces ",
            Style::default().fg(Color::Yellow),
        )))
        .highlight_style(ns_highlight_style)
        .highlight_symbol("> ");

    let mut ns_state = ListState::default();
    if !tui.namespaces.is_empty() {
        ns_state.select(Some(tui.b_selected_ns));
    }
    f.render_stateful_widget(ns_list, h_chunks[0], &mut ns_state);

    // ── Right pane: vertical split (entities top 60%, columns bottom 40%) ──
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(h_chunks[1]);

    // Right-top: entity list
    let entity_highlight_style = if tui.b_focus == BFocus::Right {
        Style::default().bg(Color::Cyan).fg(Color::Black)
    } else {
        Style::default().bg(Color::DarkGray).fg(Color::White)
    };

    let entity_items: Vec<ListItem> = tui
        .b_entities
        .iter()
        .map(|(name, type_label)| ListItem::new(format!("{} ({})", name, type_label)))
        .collect();

    let selected_ns_label = tui
        .namespaces
        .get(tui.b_selected_ns)
        .map(|s| s.as_str())
        .unwrap_or("?");
    let entity_title = format!(" Entities [{}] ", selected_ns_label);

    let entity_list = List::new(entity_items)
        .block(Block::default().borders(Borders::ALL).title(Span::styled(
            entity_title,
            Style::default().fg(Color::Yellow),
        )))
        .highlight_style(entity_highlight_style)
        .highlight_symbol("> ");

    let mut entity_state = ListState::default();
    if !tui.b_entities.is_empty() {
        entity_state.select(Some(tui.b_selected_entity));
    }
    f.render_stateful_widget(entity_list, v_chunks[0], &mut entity_state);

    // Right-bottom: columns (static, no selection)
    let selected_entity_label = tui
        .b_entities
        .get(tui.b_selected_entity)
        .map(|(n, _)| n.as_str())
        .unwrap_or("?");
    let col_title = format!(" Columns [{}] ", selected_entity_label);

    let mut col_lines: Vec<Line> = Vec::new();
    if tui.b_columns.is_empty() {
        col_lines.push(Line::from(Span::styled(
            "(no columns)",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for (name, type_name) in &tui.b_columns {
            let type_display = if type_name.is_empty() {
                String::new()
            } else {
                format!(" : {}", type_name)
            };
            col_lines.push(Line::from(vec![
                Span::styled(format!("  {}", name), Style::default().fg(Color::White)),
                Span::styled(type_display, Style::default().fg(Color::DarkGray)),
            ]));
        }
    }

    let col_paragraph = Paragraph::new(col_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(Span::styled(col_title, Style::default().fg(Color::Yellow))),
    );
    f.render_widget(col_paragraph, v_chunks[1]);
}

// ──────────────────────────────────────────────────────────────
// Window C: Query history (top) + SQL of selected query (bottom)
// ──────────────────────────────────────────────────────────────

fn render_window_c(f: &mut Frame, area: Rect, tui: &MultiPaneTui) {
    let border_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let title_style = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);

    let outer_block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(Span::styled(" C [ACTIVE] ", title_style));

    let inner_area = outer_block.inner(area);
    f.render_widget(outer_block, area);

    // Vertical split: top 40% (history list), bottom 60% (SQL)
    let v_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(inner_area);

    // ── Top: query history list ──
    let history = &tui.repl_state.query_history;

    if history.is_empty() {
        let empty_msg = Paragraph::new(Span::styled(
            "(no queries yet)",
            Style::default().fg(Color::DarkGray),
        ))
        .block(Block::default().borders(Borders::ALL).title(Span::styled(
            " Query History ",
            Style::default().fg(Color::Yellow),
        )));
        f.render_widget(empty_msg, v_chunks[0]);
    } else {
        let items: Vec<ListItem> = history
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let truncated = if entry.dql.len() > 60 {
                    format!("{}...", &entry.dql[..60])
                } else {
                    entry.dql.clone()
                };
                ListItem::new(format!("{:3}  {}", i + 1, truncated))
            })
            .collect();

        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(Span::styled(
                " Query History ",
                Style::default().fg(Color::Yellow),
            )))
            .highlight_style(Style::default().bg(Color::Cyan).fg(Color::Black))
            .highlight_symbol("> ");

        let mut state = ListState::default();
        state.select(Some(tui.c_selected_query));
        f.render_stateful_widget(list, v_chunks[0], &mut state);
    }

    // ── Bottom: SQL of selected query ──
    let sql_text = history
        .get(tui.c_selected_query)
        .map(|e| e.sql.as_str())
        .unwrap_or("(select a query above)");

    let sql_lines: Vec<Line> = sql_text
        .lines()
        .enumerate()
        .skip(tui.c_scroll_offset)
        .map(|(i, line)| {
            Line::from(vec![
                Span::styled(
                    format!("{:4} | ", i + 1),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(line.to_string(), Style::default().fg(Color::Green)),
            ])
        })
        .collect();

    let sql_paragraph =
        Paragraph::new(sql_lines).block(Block::default().borders(Borders::ALL).title(
            Span::styled(" Generated SQL ", Style::default().fg(Color::Yellow)),
        ));
    f.render_widget(sql_paragraph, v_chunks[1]);
}

// ──────────────────────────────────────────────────────────────
// Window Cu1: Process info (command line, CWD, PID)
// ──────────────────────────────────────────────────────────────

fn create_window_cu1_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    let mut lines = vec![
        Line::from(vec![Span::styled(
            " PROCESS INFO ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
    ];

    let cmd_line = if tui.repl_state.cli_args.is_empty() {
        "(unknown)".to_string()
    } else {
        tui.repl_state.cli_args.join(" ")
    };

    let cwd = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "(unknown)".to_string());

    let pid = std::process::id();

    let label_style = Style::default().fg(Color::DarkGray);
    let value_style = Style::default().fg(Color::White);

    lines.push(Line::from(vec![
        Span::styled("Command:  ", label_style),
        Span::styled(cmd_line, value_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("CWD:      ", label_style),
        Span::styled(cwd, value_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("PID:      ", label_style),
        Span::styled(pid.to_string(), value_style),
    ]));

    if let Some(ref db) = tui.repl_state.db_path {
        lines.push(Line::from(vec![
            Span::styled("Database: ", label_style),
            Span::styled(db.clone(), value_style),
        ]));
    } else {
        lines.push(Line::from(vec![
            Span::styled("Database: ", label_style),
            Span::styled(":memory:", value_style),
        ]));
    }

    lines
}

// ──────────────────────────────────────────────────────────────
// Window Cd1: REPL configuration snapshot
// ──────────────────────────────────────────────────────────────

fn create_window_cd1_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    let mut lines = vec![
        Line::from(vec![Span::styled(
            " REPL CONFIGURATION ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
    ];

    let label_style = Style::default().fg(Color::DarkGray);
    let value_style = Style::default().fg(Color::White);
    let on_style = Style::default().fg(Color::Green);
    let off_style = Style::default().fg(Color::Red);

    let fmt_on_off = |b: bool| -> Span<'_> {
        if b {
            Span::styled("on", on_style)
        } else {
            Span::styled("off", off_style)
        }
    };

    lines.push(Line::from(vec![
        Span::styled("Output format:    ", label_style),
        Span::styled(tui.repl_state.config_output_format.clone(), value_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Target stage:     ", label_style),
        Span::styled(tui.repl_state.config_target_stage.clone(), value_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("SQL mode:         ", label_style),
        fmt_on_off(tui.repl_state.config_sql_mode),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Zebra mode:       ", label_style),
        match tui.repl_state.config_zebra_mode {
            None => Span::styled("off", off_style),
            Some(n) => Span::styled(format!("{} colors", n), on_style),
        },
    ]));
    lines.push(Line::from(vec![
        Span::styled("No headers:       ", label_style),
        fmt_on_off(tui.repl_state.config_no_headers),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Multiline:        ", label_style),
        fmt_on_off(tui.repl_state.config_multiline),
    ]));

    lines
}

// ──────────────────────────────────────────────────────────────
// Unchanged windows: Cu2, Cu3, Cd2, Cd3 (kept from original)
// ──────────────────────────────────────────────────────────────

/// Window Cu2: Emit Stream 1
fn create_window_cu2_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    create_emit_window_content(tui, 0, "EMIT STREAM 1")
}

/// Window Cu3: Emit Stream 2
fn create_window_cu3_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    create_emit_window_content(tui, 1, "EMIT STREAM 2")
}

/// Helper to create emit stream window content
fn create_emit_window_content<'a>(
    tui: &MultiPaneTui,
    emit_index: usize,
    title: &'a str,
) -> Vec<Line<'a>> {
    let mut lines = vec![
        Line::from(vec![Span::styled(
            format!(" {} ", title),
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightBlue)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
    ];

    if let Some(emit_result) = tui.repl_state.last_emit_results.get(emit_index) {
        lines.push(Line::from(vec![
            Span::styled("Name: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                emit_result.name.clone(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        lines.push(Line::from(""));

        if !emit_result.columns.is_empty() {
            // Display column headers
            let header_line: Vec<Span> = emit_result
                .columns
                .iter()
                .map(|col| {
                    Span::styled(
                        format!("{:15}", col),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    )
                })
                .collect();
            lines.push(Line::from(header_line));

            // Separator
            let sep = "-".repeat(15 * emit_result.columns.len().min(6));
            lines.push(Line::from(Span::styled(
                sep,
                Style::default().fg(Color::DarkGray),
            )));

            // Display rows (limit to first 30 for TUI)
            for (i, row) in emit_result.rows.iter().take(30).enumerate() {
                let row_style = if i % 2 == 0 {
                    Style::default().fg(Color::White)
                } else {
                    Style::default().fg(Color::Gray)
                };
                let row_line: Vec<Span> = row
                    .iter()
                    .map(|val| Span::styled(format!("{:15}", val), row_style))
                    .collect();
                lines.push(Line::from(row_line));
            }

            if emit_result.rows.len() > 30 {
                lines.push(Line::from(Span::styled(
                    format!("... and {} more rows", emit_result.rows.len() - 30),
                    Style::default().fg(Color::DarkGray),
                )));
            }

            lines.push(Line::from(""));
            lines.push(Line::from(format!(
                "Total: {} rows",
                emit_result.rows.len()
            )));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "(No emit stream at this index)",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from("Use (~~emit:name ...~~) in your query"));
        lines.push(Line::from("to capture filtered row streams."));
    }

    lines
}

/// Window Cd2: Schema/Column Info (placeholder)
fn create_window_cd2_content(_tui: &MultiPaneTui) -> Vec<Line<'_>> {
    vec![
        Line::from(vec![Span::styled(
            " SCHEMA INFO ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Gray)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(Span::styled(
            "(Schema information - coming soon)",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from("This window will show:"),
        Line::from("  - Column types and constraints"),
        Line::from("  - Table relationships"),
        Line::from("  - Index information"),
    ]
}

/// Window Cd3: Debug/Log Info
fn create_window_cd3_content(tui: &MultiPaneTui) -> Vec<Line<'_>> {
    vec![
        Line::from(vec![Span::styled(
            " DEBUG LOG ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(format!("Query count: {}", tui.repl_state.query_count)),
        Line::from(format!(
            "Input length: {} chars",
            tui.repl_state.last_input.len()
        )),
        Line::from(format!(
            "SQL length: {} chars",
            tui.repl_state.last_sql.as_ref().map_or(0, |s| s.len())
        )),
        Line::from(format!(
            "Emit streams: {}",
            tui.repl_state.last_emit_results.len()
        )),
        Line::from(format!(
            "Has results: {}",
            tui.repl_state.last_results.is_some()
        )),
        Line::from(format!(
            "History entries: {}",
            tui.repl_state.query_history.len()
        )),
        Line::from(format!("Namespaces: {}", tui.repl_state.cli_args.len())),
    ]
}
