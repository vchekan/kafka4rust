#![feature(iterator_fold_self)]

mod ui;

use clap::{Arg, App, SubCommand, ArgMatches};
use std::process::exit;
use kafka4rust::{
    Cluster,
    protocol,
    Producer
};
use tracing::{dispatcher};
use opentelemetry::{sdk, global};
use tracing_subscriber::Registry;
use opentelemetry::api::trace::provider::Provider;
use tracing_subscriber::layer::SubscriberExt;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracer()?;
    let span = tracing::info_span!("test");
    let _guard = span.enter();

    let cli = parse_cli();
    let bootstrap = cli.value_of("bootstrap").expect("Bootstrap is required");

    let v = vec![1,2,3];
    let mut vi = v.iter();
    v.iter().fold(vi.next().cloned(), |a, i| a.map(|a| a + i));

    vec![1,2,3].iter().cloned().fold_first(|a, b| a + b);

    match cli.subcommand() {
        ("list", Some(list)) => {
            let mut cluster = Cluster::with_bootstrap(bootstrap)?;
            // TODO: check for errors
            let meta = cluster.fetch_topic_meta(&[]).await?;
            match list.subcommand() {
                ("topics", Some(_matches)) => {
                    let topics = meta.topics.iter().map(|t| t.topic.to_string());
                    topics.for_each(|t| println!("{}", t));
                }
                ("brokers", Some(_matches)) => {
                    let brokers = meta.brokers.iter().map(|b| format!("{}:{}", b.host, b.port));
                    brokers.for_each(|t| println!("{}", t));
                }
                _ => {
                    eprintln!("Subcommand required. Don't know what to list");
                    exit(1);
                }
            }    
        }
        ("publish", Some(args)) => {
            let key = args.value_of("key");
            let topic = args.value_of("topic").unwrap();
            let _single_message = args.value_of("single-message");
            let val = args.value_of("MSG-VALUE").expect("Message value is not provided");
            let (mut producer, _acks) = Producer::new(bootstrap).expect("Failed to create publisher");
            if let Some(key) = key {
                let msg = (key.to_string(), val.to_string());
                producer.send(msg, topic).await?;
            } else {
                producer.send(val.to_string(), topic).await?;
            }
            producer.close().await?;
        }
        ("ui", Some(_)) => {
            ui::main_ui(bootstrap).await?;
        }
        _ => { 
                eprintln!("No command provided");
                exit(1);
        }
    }
    Ok(())
}

fn parse_cli<'a>() -> ArgMatches<'a> {
    App::new("karst").
    version(env!("CARGO_PKG_VERSION")).
    about("Kafka command line and UI tool").
    arg(Arg::with_name("bootstrap").
        default_value("localhost:9092").
        short("b").
        long("bootstrap").
        help("Bootstrap servers, comma separated, port is optional, for example host1.dc.net,192.168.1.1:9092").
        takes_value(true)
    ).subcommand(SubCommand::with_name("ui")
        .about("start terminal UI")
    ).subcommand(
        SubCommand::with_name("list")
        .about("list items (topics, brokers, partitions)").
        subcommand(SubCommand::with_name("topics").
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
        )
    ).subcommand(
        SubCommand::with_name("publish")
            .about("Publish message")
            // .arg_from_usage("-k --key=<KEY> 'publish message with given key'")
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
            ).arg(
                Arg::with_name("MSG-VALUE")
                    .conflicts_with("from-file")
                    .index(1)
            )
            // .arg_from_usage("-f --from-file=<FILE> 'file as a value (one message per line by default)'")
            // TODO: timeout
    ).get_matches()
}

async fn get_offsets(cluster: &Cluster, topics_partition_count: &[(&str, u32)]) -> Result<protocol::ListOffsetsResponse0> {
    let req = protocol::ListOffsetsRequest0 {
        replica_id: -1,
        topics: topics_partition_count.iter().map(|t| protocol::Topics {
            topic: t.0.to_string(),
            partitions: (0 .. t.1).map(|partition| protocol::Partition {
                partition,
                timestamp: -1,
                max_num_offsets: 2
            }).collect()
        }).collect()
    };
    Ok(cluster.request(req).await?)
}

pub fn init_tracer() -> Result<()> {
    let exporter = opentelemetry_jaeger::Exporter::builder()
        .with_process(opentelemetry_jaeger::Process {
            service_name: "karst-tui".to_string(),
            tags: vec![],
        })
        .init()?;
    let provider = sdk::Provider::builder()
        .with_simple_exporter(exporter)
        .with_config(sdk::Config {
            default_sampler: Box::new(sdk::Sampler::Always),
            max_events_per_span: 500,
            ..Default::default()
        })
        .build();
    global::set_provider(provider);

    let tracer = global::trace_provider().get_tracer("component1");
    let otl = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(otl);
    let dispatch = dispatcher::Dispatch::new(subscriber);
    dispatcher::set_global_default(dispatch)?;

    Ok(())
}