use anyhow::Result;
use opentelemetry::{global, sdk};
use opentelemetry_jaeger;
use std::process;

pub fn docker_down() -> Result<()> {
    assert!(process::Command::new("docker-compose")
        .current_dir("docker")
        .arg("down")
        .status()?
        .success());
    Ok(())
}

pub fn docker_up() -> Result<()> {
    process::Command::new("docker-compose")
        .current_dir("docker")
        .arg("down")
        .status()?;
    Ok(())
}

pub struct DockerGuard {}
impl Drop for DockerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker-compose")
            .current_dir("docker")
            .arg("down")
            .status();
    }
}

pub fn init_tracer() -> thrift::Result<()> {
    let exporter = opentelemetry_jaeger::Exporter::builder()
        .with_agent_endpoint("localhost:6831".parse().unwrap())
        .with_process(opentelemetry_jaeger::Process {
            service_name: "kafka4rust".to_string(),
            tags: vec![
                //Key::new("exporter").string("jaeger"),
                //Key::new("float").f64(312.23),
            ],
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
    Ok(())
}
