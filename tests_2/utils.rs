use anyhow::Result;
use opentelemetry::{global, sdk};
use opentelemetry_jaeger;
use std::process;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

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
