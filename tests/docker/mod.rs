use std::process::{Command, Output};
use log::{debug, info, error};

pub struct DockerGuard {}

pub fn up() -> DockerGuard {
    docker_cmd(&["--log-level", "ERROR", "up", "-d"]);
    DockerGuard{}
}

impl Drop for DockerGuard {
    fn drop(&mut self) {
        docker_cmd(&["down"]);
    }
}

fn docker_cmd(cmd: &[&str]) -> Output {
    Command::new("docker-compose")
        .current_dir("docker")
        .args(cmd)
        .spawn()
        .expect("Docker command failed").wait_with_output().unwrap()
}


pub fn hard_kill_kafka(port: i32) {
    // See docker/docker-compose.yml
    let service = match(port) {
        9092 => "broker1",
        9093 => "broker2",
        _ => panic!("Unknown port: {}", port)
    };
    info!("Killing broker '{}'", service);
    let out = docker_cmd(&["exec", service, "kill", "-9", "6"]);
    if !out.status.success() {
        error!(">{:?}", out);
        panic!("Failed to kill broker");
    }
}