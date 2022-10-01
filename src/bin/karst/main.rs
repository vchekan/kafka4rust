mod ui;

use anyhow::Result;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use kafka4rust::{protocol, ClusterHandler, ProducerBuilder};
//use opentelemetry::api::trace::provider::Provider;
use opentelemetry::{global, sdk};
use std::process::exit;
use tracing::dispatcher;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use tracing_attributes::instrument;
use std::time::Duration;
use itertools::Itertools;
use kafka4rust::init_tracer;

#[tokio::main]
#[instrument(level="debug")]
async fn main() -> Result<()> {
    let _tracer = init_tracer("karst")?;
    // let span = tracing::info_span!("main");
    // let _guard = span.enter();

    let cli = parse_cli();

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

    match cli.subcommand() {
        ("list", Some(list)) => {
            let brokers = list.value_of("brokers").unwrap();
            let mut cluster = ClusterHandler::with_bootstrap(brokers, Some(Duration::from_secs(20)))?;
            // TODO: check for errors
            let meta = cluster.fetch_topic_meta_owned(&[]).await?;
            match list.subcommand() {
                ("topics", Some(_matches)) => {
                    let topics = meta.topics.iter().map(|t| t.topic.to_string());
                    topics.for_each(|t| println!("{}", t));
                }
                ("brokers", Some(_matches)) => {
                    let brokers = meta
                        .brokers
                        .iter()
                        .map(|b| format!("{}:{}", b.host, b.port));
                    brokers.for_each(|t| println!("{}", t));
                }
                _ => {
                    eprintln!("Subcommand required. Don't know what to list");
                    exit(1);
                }
            }
        }
        ("publish", Some(args)) => {
            let brokers = args.value_of("brokers").unwrap();
            let key = args.value_of("key");
            let topic = args.value_of("topic").unwrap();
            let _single_message = args.value_of("single-message");
            let send_timeout: Option<u64> = args.value_of("send-timeout").map(|t| t.parse().expect("Timeout must be integer in seconds"));
            let val = args
                .value_of("MSG-VALUE")
                .expect("Message value is not provided");
            let mut producer = ProducerBuilder::new(brokers);
            if let Some(send_timeout) = send_timeout {
                producer = producer.send_timeout(Duration::from_secs(send_timeout));
            }

            let (mut producer, _acks) =
                producer.start().expect("Failed to create publisher");
            if let Some(key) = key {
                let msg = (Some(key.to_string()), val.to_string());
                producer.send(msg, topic).await?;
            } else {
                producer.send((Option::<&[u8]>::None,val.to_string()), topic).await?;
            }
            producer.close().await?;
        }
        ("ui", Some(args)) => {
            let brokers = args.value_of("brokers").unwrap();
            ui::main_ui(brokers).await?;
        }
        _ => {}
    }
    Ok(())
}

fn brokers_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("brokers").
        default_value("localhost:9092").
        short("b").
        long("brokers").
        help("Bootstrap servers, comma separated, port is optional, for example host1.dc.net,192.168.1.1:9092").
        takes_value(true)
}

fn parse_cli<'a>() -> ArgMatches<'a> {
    App::new("karst")
        .settings(&[AppSettings::ArgRequiredElseHelp, AppSettings::ColoredHelp])
        .version(env!("CARGO_PKG_VERSION"))
        .about("Kafka command line and UI tool")
        .arg(Arg::with_name("log")
            .short("l")
            .long("log")
            .takes_value(true)
            .possible_values(&["trace", "debug", "info", "error", "off"])
        )
    .subcommand(SubCommand::with_name("ui")
        .about("start terminal UI")
        .setting(AppSettings::ColoredHelp)
        .arg(brokers_arg())
    ).subcommand(
        SubCommand::with_name("list")
        .about("list items (topics, brokers, partitions)")
        .setting(AppSettings::ColoredHelp)
        .arg(brokers_arg())
            .subcommand(SubCommand::with_name("topics").
            about("List topics").
            arg(
                Arg::with_name("filter").
                short("f").
                long("filter").
                help("filter topics which have given substring anywhere in name").
                takes_value(true)
            ).arg(
                Arg::with_name("filter-regex").
                short("r").
                long("filter-regex").
                help("filter topics which match given regex. See https://docs.rs/regex/1.3.4/regex/#syntax").
                takes_value(true)
            )
        ).subcommand(SubCommand::with_name("brokers").
            about("list brokers (from metadata, not from seeds)")
            .setting(AppSettings::ColoredHelp)
        )
    ).subcommand(
        SubCommand::with_name("publish")
            .about("Publish message")
            .setting(AppSettings::ColoredHelp)
            // .arg_from_usage("-k --key=<KEY> 'publish message with given key'")
            .arg(brokers_arg())
            .arg(Arg::with_name("key")
                .takes_value(true)
                .short("k")
                .long("key")
                .help("publish message with given key")
            ).arg(
                Arg::with_name("topic")
                    .short("t")
                    .long("topic")
                    .help("topic")
                    .required(true)
                    .takes_value(true)
            ).arg(
                    Arg::with_name("single-message")
                        .short("s")
                        .long("single-message")
                        .help("Interpret input file as a single message or as a message per line")
            ).arg(Arg::with_name("file")
                .short("f")
                .long("file")
                .help("Read values from file. By default, one message per line, but can change with --single-message")
                .takes_value(true)
            ).arg(Arg::with_name("send-timeout")
                .long("send-timeout")
                .help("Sending a batch timeout in seconds")
                .takes_value(true)
            ).arg(
                Arg::with_name("MSG-VALUE")
                    .conflicts_with("from-file")
                    .index(1)
            )
            // .arg_from_usage("-f --from-file=<FILE> 'file as a value (one message per line by default)'")
            // TODO: timeout
    ).get_matches()
}

pub(crate) async fn get_offsets(
    cluster: &ClusterHandler,
    topics_partition_count: &[(&str, u32)],
) -> Result<protocol::ListOffsetsResponse0> {
    let topics_partition_count = topics_partition_count
        .iter().map(|t| (t.0.to_owned(), t.1))
        .collect_vec();
    Ok(cluster.fetch_offsets(topics_partition_count).await?)
    // let req = protocol::ListOffsetsRequest0 {
    //     replica_id: -1,
    //     topics: topics_partition_count
    //         .iter()
    //         .map(|t| protocol::Topics {
    //             topic: t.0.to_string(),
    //             partitions: (0..t.1)
    //                 .map(|partition| protocol::Partition {
    //                     partition,
    //                     timestamp: -1,
    //                     max_num_offsets: 2,
    //                 })
    //                 .collect(),
    //         })
    //         .collect(),
    // };
    // Ok(cluster.request_any(req).await?)
}
