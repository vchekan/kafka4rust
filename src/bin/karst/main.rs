mod ui;

use clap::{Arg, App, SubCommand, ArgMatches};
use kafka4rust;
use tokio::main;
use failure::Error;
use simple_logger;
use std::process::exit;
use kafka4rust::{
    Cluster,
    protocol,
};

type Result<T> = std::result::Result<T,Error>;

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: global panic handler
    //simple_logger::init_with_level(log::Level::Debug)?;
    
    let cli = parse_cli();
    let bootstrap = cli.value_of("bootstrap").expect("Bootstrap is required");

    match cli.subcommand() {
        ("list", Some(list)) => {
            let cluster = Cluster::connect_with_bootstrap(bootstrap).await?;
            // TODO: check for errors
            let meta = cluster.fetch_topics(&vec![]).await?;
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
    ).subcommand(SubCommand::with_name("webui")
        .about("start web UI")
    ).subcommand(
        SubCommand::with_name("list")
        .about("list items").
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