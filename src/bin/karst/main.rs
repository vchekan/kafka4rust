mod ui;

use std::path::PathBuf;
use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use kafka4rust::{init_console_tracer, init_grpc_opentetemetry_tracer, Cluster};
use opentelemetry::trace::{Tracer, TracerProvider};
use tracing::{debug, error, info, info_span, instrument::{self, WithSubscriber}, Instrument, Span};
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use std::time::Duration;
use kafka4rust::Producer;
use tracing_attributes::instrument;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // let provider = init_grpc_opentetemetry_tracer(); 
    // let tracer = provider.tracer("main");
    // let ot_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    // tracing_subscriber::registry()
    //     .with(EnvFilter::from_default_env())
    //     .with(ot_layer)
    //     .with(tracing_subscriber::fmt::layer()
    //         .with_thread_ids(true)
    //         .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE))
    //     .init();

   

    // let span = info_span!("root");
    // let _guard = span.enter();
    let x = run()/*.with_subscriber(subscriber).in_current_span()*/.await?;

    // tracing::subscriber::with_default(subscriber, || {
    //     let span = info_span!("root");
    //     let _guard = span.enter();
    //     info!("info test");
    //     tokio::spawn(future).with_subscriber(subscriber);
    //     run().await?;
    // });

    // tracing::subscriber::set_global_default(subscriber).unwrap();
    // let span = tracing::info_span!("main");
    // info!("global test");
    // run().instrument(span).await?;

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())

//}.instrument(info_span!("root-main"))
//    })
}

#[instrument(level = "debug")]
async fn run() -> Result<()> {
    debug!("parsing");
    let cli = Cli::parse();
    // match cli.tracing {
    //     TracingType::Off => {}
    //     TracingType::Console => init_console_tracer(),
    //     TracingType::OpentelemetryGrpc => init_grpc_opentetemetry_tracer()
    // }


    
    match cli.command {
        KartCommand::List {subcommand} => {
            let mut cluster = Cluster::new(cli.bootstrap, Some(Duration::from_secs(20)));
            // TODO: check for errors
            let meta = cluster.fetch_topic_meta_no_update(vec![]).await?;
            match subcommand {
                ListCommands::Topics {filter, filter_regex} => {
                    debug!("Listing topics");
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
            let mut producer = Producer::builder(brokers).send_timeout(send_timeout_sec.map(Duration::from_secs)).build();
            if let Some(msg) = msg_value {
                debug!("publishing...");
                producer.publish(msg, topic).await?;
                debug!("closing...");
                producer.close().await.unwrap_or_else(|e| error!("Error while waiting for producer closure: {e}"));
                debug!("closed");
            }
            
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
    debug!("exiting");
    println!("exiting");
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

    /// Turn on tracing
    #[arg(long, value_enum, default_value_t = TracingType::Off)]
    tracing: TracingType,
}

#[derive(Clone, Copy, ValueEnum)]
enum TracingType {
    Off,
    Console,
    OpentelemetryGrpc,
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
