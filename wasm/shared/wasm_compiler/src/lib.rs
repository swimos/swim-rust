use std::env::var_os;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, Result};
use tokio::process::Command;

const TARGET: &str = "wasm32-unknown-unknown";

#[derive(PartialEq)]
pub enum ReleaseMode {
    Debug,
    Release,
}

impl ReleaseMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReleaseMode::Debug => "debug",
            ReleaseMode::Release => "release",
        }
    }
}

pub struct Project {
    dir: PathBuf,
    features: Vec<String>,
    release_mode: ReleaseMode,
}

impl Project {
    pub fn new(dir: PathBuf, features: Vec<String>, release_mode: ReleaseMode) -> Project {
        Project {
            dir,
            features,
            release_mode,
        }
    }
}

pub async fn install_wasm_target() -> Result<()> {
    let mut cargo = Command::new("rustup");
    let command = cargo
        .args(&["target", "add", TARGET])
        .stderr(Stdio::inherit())
        .stdout(Stdio::inherit());

    let status = command.spawn()?.wait().await?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("Failed to install wasm target"))
    }
}

pub async fn compile(project: Project) -> Result<()> {
    let Project {
        dir,
        features,
        release_mode,
    } = project;

    let mut command = match var_os("CARGO") {
        Some(cargo) => Command::new(cargo),
        None => Command::new("cargo"),
    };

    command
        .current_dir(dir)
        .arg("build")
        .args(&["--target", TARGET])
        .arg("--quiet");

    if release_mode == ReleaseMode::Release {
        command.arg("--release");
    }

    let features = if features.is_empty() {
        Vec::new()
    } else {
        vec![
            "--no-default-features".to_owned(),
            "--features".to_owned(),
            features.join(","),
        ]
    };

    command.args(features);

    let status = command.spawn()?.wait().await?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("Failed to build project"))
    }
}
