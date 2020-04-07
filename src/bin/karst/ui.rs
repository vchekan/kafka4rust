use std::io::{stdout, Write};
use failure::Error;
use kafka4rust::{protocol, Cluster};
use tui::{
    self,
    widgets::{Widget, Block, Borders, SelectableList, Text, Paragraph, Row, Table},
    layout::{Layout, Constraint, Direction},
    style::{Style, Color},
    backend::{CrosstermBackend},
};
use crossterm::{self, execute, 
    terminal::{enable_raw_mode, disable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    event::{Event, KeyEvent, KeyCode, EventStream}};
use futures::{StreamExt, TryFutureExt};
use std::panic;
use tokio;
use std::collections::HashMap;
use std::iter::FromIterator;
use tracing;
use tracing_futures::Instrument;

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
}

struct State {
    page: Page,
    // TODO: select by name, to preserve selection in case of added/removed topic
    master_selected: usize,
    topics: Vec<String>,
    partitions: HashMap<String,Vec<PartitionRecord>>,
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
        println!("Dropped");
    }
}

type Terminal = tui::Terminal<CrosstermBackend<std::io::Stdout>>;

/// UI entry point
pub async fn main_ui(bootstrap: &str) -> Result<(), Error> {
    enable_raw_mode()?;
    let _terminal_guard = TerminalRawModeGuard {};

    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    terminal.hide_cursor()?;
    terminal.clear()?;

    let bootstrap = bootstrap.to_string();
    let (mut tx, rx) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        tracing::event!(tracing::Level::DEBUG, "Connecting to {}", bootstrap);
        let mut cluster = Cluster::with_bootstrap(&bootstrap)?;
        let topics_meta = cluster.fetch_topic_meta(&vec![]).await?;
        let topics: Vec<_> = topics_meta.topics.iter().map(|t| (t.topic.as_str(), t.partition_metadata.len() as u32)).collect();
        let offsets = crate::get_offsets(&cluster, &topics).await.unwrap();
        tx.send(Cmd::TopicMeta(topics_meta)).await?;
        tx.send(Cmd::Offsets(offsets)).await?;

        Ok::<_,Error>(())
    }.inspect_err(|e| {
        panic!("Error in kafka loop: {}", e);
    })).instrument(tracing::debug_span!("UI eval loop"));

    let state = State {
        page: Page::Topics,
        master_selected: 0,
        topics: vec![],
        partitions: HashMap::new(),
    };
    eval_loop(&mut terminal, state, rx).await?;

    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    // Cleanup
    disable_raw_mode()?;

    Ok(())
}

fn draw(terminal: &mut Terminal, state: &State) -> Result<(), Error> {
    match state.page {
        Page::Topics => draw_topics(terminal, state),
        Page::Brokers => draw_brokers(terminal, state),
    }
}

fn draw_topics(terminal: &mut Terminal, state: &State) -> Result<(), Error> {
    terminal.draw(|mut f| {
        // top/status
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(4),
                Constraint::Length(1),
            ].as_ref())
            .split(f.size());
        let (top, status) = (chunks[0], chunks[1]);
        
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(10),
                Constraint::Max(50)
            ].as_ref())
            .split(top);
        let (master_box, detail_box) = (chunks[0], chunks[1]);

        //let topics: Vec<&str> = state.topics.map(|meta| meta.topics.iter().map(|t| t.topic.as_str()).collect()).unwrap_or(vec![]);
        SelectableList::default()
            .items(&state.topics)
            .select(Some(state.master_selected))
            .highlight_symbol(">")
            .highlight_style(Style::default().fg(Color::Yellow))
            .block(Block::default()
                .title("Topics")
                .borders(Borders::ALL))
            .render(&mut f, master_box);

        let empty = vec![];
        let partitions = state.topics.get(state.master_selected)
            .and_then(|t| state.partitions.get(t))
            .unwrap_or(&empty);

        Table::new(
                ["#", "First", "Last"/*, "Size"*/].iter(),
                partitions.into_iter().map(|row| {
                    Row::StyledData(vec![
                        format!("{}", row.partition),
                        format!("{}", row.first),
                        format!("{}", row.last),
                    ].into_iter(), Style::default())
                })
            )
            .widths(&[Constraint::Length(10), Constraint::Length(10), Constraint::Length(10)])
            .block(Block::default()
            .title("Topics")
            .borders(Borders::ALL))
            .header_style(Style::default().fg(Color::Yellow))
            .column_spacing(1)
            .render(&mut f, detail_box);

        // status line
        Paragraph::new([
            Text::raw(" 1"), Text::styled("Help", Style::default().bg(Color::LightBlue)), 
            Text::raw(" 3"), Text::styled("Brokers", Style::default().bg(Color::LightBlue)), 
            Text::raw(" 8"), Text::styled("Delete", Style::default().bg(Color::LightBlue)),
            Text::raw(" 10"), Text::styled("Quit", Style::default().bg(Color::LightBlue)),
        ].iter()).wrap(true).render(&mut f, status);
        
    })?;
    Ok(())
}

fn draw_brokers(_terminal: &mut Terminal, _state: &State) -> Result<(), Error> {
    unimplemented!()
}

async fn eval_loop(term: &mut Terminal, mut state: State, mut commands: tokio::sync::mpsc::Receiver<Cmd>) -> Result<(),Error> {
    // Show initial state
    draw(term, &state)?;
    let mut term_events = EventStream::new();

    loop {
        tokio::select! {
            cmd = commands.recv() => {
                match cmd {
                    Some(Cmd::TopicMeta(topics)) => {
                        state.topics = topics.topics.iter().map(|t| t.topic.clone()).collect();
                    }
                    Some(Cmd::Offsets(offsets)) => {
                        state.partitions = HashMap::from_iter(offsets.responses.into_iter().map(|r| (
                            r.topic,
                            r.partition_responses.into_iter().map(|p| PartitionRecord {
                                partition: p.partition,
                                first: *p.offsets.get(1).unwrap_or(&0),
                                last: *p.offsets.get(0).unwrap_or(&0),
                            }).collect()
                        )));
                    }
                    None => {
                        //break
                        // TODO:
                        let _ = draw(term, &state);
                    }
                }
            }
            key_event = term_events.next() => {
                match key_event {
                    Some(Ok(Event::Key(KeyEvent{code, modifiers: _}))) => {
                        match code {
                            KeyCode::Char('q') => break,
                            KeyCode::Down => {
                                let len = state.topics.len();
                                if len == 0 { continue }
                                state.master_selected = (state.master_selected + 1).min(len - 1)
                            },
                            KeyCode::Up => state.master_selected = (state.master_selected as isize - 1).max(0) as usize,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
        draw(term, &state)?;
    }

    Ok(())
}