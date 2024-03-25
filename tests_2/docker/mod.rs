use log::{debug, error, info};
use std::process::{Command, Output};

pub struct DockerGuard {}

pub fn up() -> DockerGuard {
    info!("Docker is starting...");
    compose_cmd(&["--log-level", "INFO", "up", "-d"]);
    info!("Docker is started");
    DockerGuard {}
}

pub fn up_no_close() {
    info!("Docker is starting...");
    compose_cmd(&["--log-level", "INFO", "up", "-d"]);
    info!("Docker is started");
}


impl Drop for DockerGuard {
    fn drop(&mut self) {
        info!("Docker is shutting down...");
        compose_cmd(&["down"]);
        info!("Docker is down");
    }
}

fn compose_cmd(cmd: &[&str]) -> Output {
    Command::new("docker-compose")
        .current_dir("docker")
        .args(cmd)
        .spawn()
        .expect("Docker command failed")
        .wait_with_output()
        .unwrap()
}

pub fn hard_kill_kafka(port: i32) {
    // See docker/docker-compose.yml
    let service = match (port) {
        9092 => "broker1",
        9093 => "broker2",
        _ => panic!("Unknown port: {}", port),
    };
    info!("Killing broker '{}'", service);
    let out = compose_cmd(&["kill", service]);
    if !out.status.success() {
        error!(">{:?}", out);
        panic!("Failed to kill broker");
    }
}
