use anyhow::Result;
use crossterm::{
    self,
    event::{Event, EventStream, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::{StreamExt, TryFutureExt};
use kafka4rust::protocol::Broker;
use kafka4rust::{protocol, Cluster};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{stdout, Write};
use std::panic;
use tracing_futures::Instrument;
use tui::layout::Rect;
use tui::text::{Span, Spans};
use tui::widgets::{Cell, ListItem, ListState};
use tui::{
    self,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, List, Paragraph, Row, Table},
    Frame,
};
use std::time::Duration;

enum Page {
    Brokers,
    Topics,
    /*Groups,
    Acls,
    Configs,
    LogDirss,
    Reassignments,*/
}

#[derive(Debug)]
enum Cmd {
    TopicMeta(protocol::MetadataResponse0),
    Offsets(protocol::ListOffsetsResponse0),
    ConnState(ConnState),
    Err(anyhow::Error)
}

impl Display for Cmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::TopicMeta(_) => f.write_str("TopicMeta"),
            Cmd::Offsets(_) => f.write_str("Offsets"),
            Cmd::ConnState(s) => f.write_fmt(format_args!("ConnState({:?})", s)),
            Cmd::Err(e) => f.write_fmt(format_args!("Error: {:#?}", e))
        }
    }
}

#[derive(Debug)]
enum ConnState {
    Connecting,
    Connected,
    Disconnected,
    Error(anyhow::Error),
}

struct State {
    page: Page,
    // TODO: select by name, to preserve selection in case of added/removed topic
    master_selected: usize,
    topics: Vec<String>,
    partitions: HashMap<String, Vec<PartitionRecord>>,
    brokers: Vec<Broker>,
    conn_state: ConnState,
}

impl Default for State {
    fn default() -> Self {
        State {
            page: Page::Topics,
            master_selected: 0,
            topics: vec![],
            partitions: HashMap::new(),
            brokers: vec![],
            conn_state: ConnState::Disconnected,
        }
    }
}

struct PartitionRecord {
    partition: u32,
    first: u64,
    last: u64,
    // TODO: available from DescribeLogDirs
    //size: u64,
    // TODO
    // time
}

/// Protect raw terminal mode against panic
struct TerminalRawModeGuard {}
impl Drop for TerminalRawModeGuard {
    fn drop(&mut self) {
        // Do not check result because might be called from panic
        let _ = disable_raw_mode();
    }
}

type Terminal = tui::Terminal<CrosstermBackend<std::io::Stdout>>;

/// UI entry point
pub async fn main_ui(bootstrap: &str) -> Result<()> {
    enable_raw_mode()?;
    let _terminal_guard = TerminalRawModeGuard {};

    let mut stdout = stdout();

    // Capture current screen to be restored upon exit
    execute!(stdout, EnterAlternateScreen)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    terminal.hide_cursor()?;
    terminal.clear()?;

    let bootstrap = bootstrap.to_string();
    // kafka client runs in tokio future and communicates with UI main thread via channel
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let res = tokio::spawn(
        async move {
            let res = async {
                tracing::event!(tracing::Level::DEBUG, %bootstrap, "Connecting");
                tx.send(Cmd::ConnState(ConnState::Connecting)).await?;
                let mut cluster = Cluster::with_bootstrap(&bootstrap, Some(Duration::from_secs(20)))?;
                let topics_meta = cluster.fetch_topic_meta(&[]).await?;
                tracing::debug_span!("Connected");
                tx.send(Cmd::ConnState(ConnState::Connected)).await?;
                let topics: Vec<_> = topics_meta
                    .topics
                    .iter()
                    .map(|t| (t.topic.as_str(), t.partition_metadata.len() as u32))
                    .collect();
                let offsets = crate::get_offsets(&cluster, &topics).await.unwrap();
                tracing::debug_span!("Sending topic meta");
                tx.send(Cmd::TopicMeta(topics_meta)).await?;
                tx.send(Cmd::Offsets(offsets)).await?;

                Ok::<_, anyhow::Error>(())
            }.await;

            if let Err(e) = res {
                tracing::error!("Error in kafka loop: {:#?}", e);
                tx.send(Cmd::Err(e)).await;
            }
        }
        .instrument(tracing::debug_span!("kafka client loop"))
    );

    // Start eval loop
    eval_loop(&mut terminal, State::default(), rx).await?;

    // Cleanup
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    disable_raw_mode()?;

    Ok(())
}

fn draw(terminal: &mut Terminal, state: &State) -> Result<()> {
    terminal.draw(|frame| {
        let main_and_status = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(1)].as_ref())
            .split(frame.size());
        let (main_area, status_area) = (main_and_status[0], main_and_status[1]);

        let status_text = match &state.conn_state {
            ConnState::Disconnected => Span::styled("X", Style::default().fg(Color::Red)),
            ConnState::Connecting => Span::styled("-", Style::default().fg(Color::LightYellow)),
            ConnState::Connected => Span::styled("|", Style::default().fg(Color::LightGreen)),
            ConnState::Error(e) => Span::styled(format!("{:#?}", e).replace("\n", "    "), Style::default().fg(Color::Red)),
        };

        // status line
        let status_style = Style::default().bg(Color::LightBlue);
        let status_line = Paragraph::new(Spans(vec![
            Span::from(" 1"),
            Span::styled("Help", status_style),
            Span::from(" 3"),
            Span::styled("Log", status_style),
            Span::from(" 4"),
            Span::styled("Brokers", status_style),
            Span::from(" 8"),
            Span::styled("Delete", status_style),
            Span::from(" 10"),
            Span::styled("Quit", status_style),
            Span::from(" "),
            status_text,
        ]));
        frame.render_widget(status_line, status_area);

        match state.page {
            Page::Topics => draw_topics(frame, main_area, state),
            Page::Brokers => draw_brokers(frame, main_area, state),
        }
    })?;

    Ok(())
}

fn draw_topics<T: tui::backend::Backend>(frame: &mut Frame<T>, area: Rect, state: &State) {
    //
    // -main_frame--------------------------
    // |     master_box        | detail_box|
    // |                       |           |
    // -------------------------------------
    // ......status_frame...................
    //

    let master_and_detail = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(30), Constraint::Length(50)].as_ref())
        .split(area);
    let (master_box, detail_box) = (master_and_detail[0], master_and_detail[1]);

    let topics: Vec<ListItem> = state
        .topics
        .iter()
        .map(|t| ListItem::new(t.as_str()))
        .collect();
    let mut topic_state = ListState::default();
    topic_state.select(Some(state.master_selected));
    let topics = List::new(topics)
        //.select(Some(state.master_selected))
        .highlight_symbol(">")
        .highlight_style(Style::default().fg(Color::Yellow))
        .block(Block::default().title("Topics").borders(Borders::ALL));
    //.render(&mut f, master_box);
    frame.render_stateful_widget(topics, master_box, &mut topic_state);

    let empty = vec![];
    let partitions = state
        .topics
        .get(state.master_selected)
        .and_then(|t| state.partitions.get(t))
        .unwrap_or(&empty);

    let table = Table::new(
        // Row::new(vec!["#", "First", "Last"/*, "Size"*/])
        partitions.iter().map(|row| {
            Row::new(vec![
                format!("{}", row.partition),
                format!("{}", row.first),
                format!("{}", row.last),
            ])
        }),
    )
    .widths(&[
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
    ])
    .block(Block::default().title("Partitions").borders(Borders::ALL))
    // .header_style(Style::default().fg(Color::Yellow))
    .column_spacing(1);
    //.render(&mut f, detail_box);
    frame.render_widget(table, detail_box);
}

fn draw_brokers<B: tui::backend::Backend>(frame: &mut Frame<B>, area: Rect, _state: &State) {
    let table = Table::new(_state.brokers.iter().map(|b| {
        Row::new(vec![
            Cell::from(format!("{}:{}", b.host, b.port)),
            Cell::from(b.node_id.to_string()),
            Cell::from(""),
        ])
    }))
    .header(Row::new(vec!["Host", "Id", "Parts prim/repl"]))
    .block(Block::default().title("Brokers").borders(Borders::ALL));

    frame.render_widget(table, area);
}

async fn eval_loop(
    term: &mut Terminal,
    mut state: State,
    mut kafka_commands: tokio::sync::mpsc::Receiver<Cmd>,
) -> Result<()> {
    // Show initial state
    draw(term, &state)?;
    let mut term_events = EventStream::new();

    // Await for 2 event sources: kafka events and keyboard events
    loop {
        tokio::select! {
            cmd = kafka_commands.recv() => {
                match cmd {
                    Some(ref cmd) => tracing::debug_span!("eval_loop: got kafka command", %cmd),
                    None => tracing::debug_span!("eval_loop: got kafka command 'None'"),
                };

                match cmd {
                    // Changes in the list of topics
                    Some(Cmd::TopicMeta(topics_meta)) => {
                        state.topics = topics_meta.topics.iter().map(|t| t.topic.clone()).collect();
                        state.brokers = topics_meta.brokers;
                        tracing::debug_span!("Got Cmd::TopicMeta", "topics" = state.topics.join(",").as_str());
                    }
                    // Changes in offsets
                    Some(Cmd::Offsets(offsets)) => {
                        state.partitions = offsets.responses.into_iter().map(|r| (
                            r.topic,
                            r.partition_responses.into_iter().map(|p| PartitionRecord {
                                partition: p.partition,
                                first: *p.offsets.get(1).unwrap_or(&0),
                                last: *p.offsets.get(0).unwrap_or(&0),
                            }).collect()
                        )).collect();
                    }
                    Some(Cmd::ConnState(conn_state)) => {
                        state.conn_state = conn_state;
                    }
                    Some(Cmd::Err(e)) => {
                        state.conn_state = ConnState::Error(e)
                    }
                    None => {
                        //break
                        // TODO:
                        let _ = draw(term, &state);
                    }
                }
            }
            key_event = term_events.next() => {
                if let Some(Ok(Event::Key(KeyEvent{code, modifiers: _}))) = key_event {
                    match code {
                        KeyCode::Char('q') | KeyCode::F(10) => break,
                        KeyCode::F(3) => state.page = Page::Brokers,
                        KeyCode::Down => {
                            let len = state.topics.len();
                            if len == 0 { continue }
                            state.master_selected = (state.master_selected + 1).min(len - 1)
                        },
                        KeyCode::Up => state.master_selected = (state.master_selected as isize - 1).max(0) as usize,
                        _ => {}
                    }
                }
            }
        }
        draw(term, &state)?;
    }

    Ok(())
}
