mod ui;

use std::path::PathBuf;
use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use kafka4rust::{Cluster, init_tracer};
use tracing::info_span;
use tracing_attributes::instrument;
use std::time::Duration;


#[tokio::main]
#[instrument(level="debug")]
async fn main() -> Result<()> {
    // let span = tracing::info_span!("main");
    // let _guard = span.enter();

    let cli = Cli::parse();
    /*let level = match cli.value_of("log").unwrap_or("info") {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "error" => LevelFilter::Error,
        "off" => LevelFilter::Off,
        _ => panic!("Unknown log level")
    };*/
    // TODO: logging messes up UI. Think how to redirect into a window in UI?
    // simple_logger::SimpleLogger::new().with_level(level).init().unwrap();
    //simple_logger::init_with_env()?;


    let _tracer = if cli.tracing {
        Some(init_tracer("karst")?)
    } else {
        None
    };

    info_span!("ui-main");

    match cli.command {
        KartCommand::List {subcommand} => {
            let mut cluster = Cluster::new(cli.bootstrap, Some(Duration::from_secs(20)));
            // TODO: check for errors
            let meta = cluster.fetch_topic_meta_no_update(vec![]).await?;
            match subcommand {
                ListCommands::Topics {filter, filter_regex} => {
                    let topics = meta.topics.iter();
                    topics.for_each(|t| {
                        println!("Topic: {}", t.topic);
                        if !t.error_code.is_ok() {
                            println!("Error: {}", t.error_code);
                        }
                        println!("Partitions: {:?}",t.partition_metadata)
                    });
                }
                ListCommands::Brokers => {
                    let brokers = meta
                        .brokers
                        .iter()
                        .map(|b| format!("id:{} addr: {}:{}", b.node_id, b.host, b.port));
                    brokers.for_each(|t| println!("{}", t));
                }
                ListCommands::Offsets {topic} => {
                    let offsets = cluster.list_offsets(vec![topic]).await?;
                    for part in offsets {
                        println!("{:?}", part);
                    }
                }
            }
        }
        KartCommand::Publish {key, topic, single_message, send_timeout_sec, from_file, msg_value} => {
            let brokers = cli.bootstrap;
            // let mut producer = ProducerBuilder::new(&brokers);
            // if let Some(send_timeout) = send_timeout_sec {
            //     producer = producer.send_timeout(Duration::from_secs(send_timeout));
            // }

            todo!()
            // let (mut producer, _acks) =
            //     producer.start().expect("Failed to create publisher");
            // if let Some(key) = key {
            //     let msg = (Some(key.to_string()), val.to_string());
            //     producer.send(msg, topic).await?;
            // } else {
            //     producer.send((Option::<&[u8]>::None,val.to_string()), topic).await?;
            // }
            // producer.close().await?;
        }
        KartCommand::Ui => {
            ui::main_ui(&cli.bootstrap).await?;
        }
    }
    Ok(())
}


#[derive(Parser)]
#[command(name = "karst", version, author, long_about = None)]
#[command(about = "Kafka command line and UI tool")]
#[command(arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: KartCommand,

    /// Bootstrap servers, comma separated, port is optional, for example host1.dc.net,192.168.1.1:9092
    #[arg(short, long, default_value = "localhost:9092")]
    bootstrap: String,

    #[arg(short, long, value_enum, default_value_t = LogLevel::Error)]
    log: LogLevel,

    /// Turn on Opentelemetry tracing
    #[arg(long)]
    tracing: bool,
}

#[derive(Subcommand)]
enum KartCommand {
    /// list items (topics, brokers, partitions)
    List {
        #[command(subcommand)]
        subcommand: ListCommands,
    },

    /// Publish message
    Publish {
        #[arg(short, long)]
        key: Option<String>,

        #[arg(short, long)]
        topic: String,

        /// Interpret input file as a single message as opposite to default a message per line
        #[arg(long)]
        single_message: bool,

        /// Read values from file. By default, one message per line, but can change with --single-message
        #[arg(short, long)]
        from_file: Option<PathBuf>,

        /// Sending a batch timeout in seconds
        #[arg(long)]
        send_timeout_sec: Option<u64>,

        /// Message to be published. Conflicts with --from-file
        #[arg(conflicts_with = "from_file", index = 1)]
        msg_value: Option<String>,
    },
    Ui,
}

#[derive(Subcommand)]
enum ListCommands {
    /// list brokers (from metadata, not from seeds)
    Brokers,
    /// list topics
    Topics {
        /// filter topics which have given substring anywhere in name
        #[arg(short, long)]
        filter: Option<String>,

        /// filter topics which match given regex. See https://docs.rs/regex/1.3.4/regex/#syntax
        #[arg(long, short = 'r')]
        filter_regex: Option<String>,
    },

    /// list offsets
    Offsets {
        #[arg(short, long)]
        topic: String,
    },
}

#[derive(ValueEnum, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LogLevel {
    None,
    Error,
    Warn,
    Info,
    Debug,
    Trace
}
