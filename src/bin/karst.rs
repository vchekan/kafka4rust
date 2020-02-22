use clap::{Arg, App, SubCommand, ArgMatches};
use kafka4rust;
use tokio::main;
use failure::Error;
use simple_logger;

type Result<T> = std::result::Result<T,Error>;

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: global panic handler
    simple_logger::init_with_level(log::Level::Debug)?;
    let cli = parse_cli();
    if cli.is_present("list-topics") {
        let bootstrap = cli.value_of("bootstrap").expect("Bootstrap is required");
        list_topics(bootstrap).await?;
    }
    Ok(())
}

async fn list_topics(bootstrap: &str) -> Result<()> {
    let cluster = kafka4rust::Cluster::connect_with_bootstrap(bootstrap).await?;
    let topics = cluster.fetch_topics(&vec![]).await?;
    println!("Topic:");
    for topic in topics.topics {
        // TODO: check for errors
        println!("  {}", topic.topic);
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
        SubCommand::with_name("list-topics")
        .about("list topics").
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
    ).get_matches()
}