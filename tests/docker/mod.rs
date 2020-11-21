use std::process::{Command, Output};
use log::debug;

pub struct Docker {}
pub struct DockerGuard {}

impl Docker {
    pub fn up() -> DockerGuard {
        Self::docker_cmd(&["--log-level", "ERROR", "up", "-d"]);
        DockerGuard{}
    }


    fn docker_cmd(cmd: &[&str]) -> Output {
        Command::new("docker-compose")
            .current_dir("docker")
            .args(cmd)
            .spawn()
            .expect("Docker command failed").wait_with_output().unwrap()

    }
}

impl Drop for DockerGuard {
    fn drop(&mut self) {
        Docker::docker_cmd(&["down"]);
    }
}