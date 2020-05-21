// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process::Command;

use git2::{ErrorCode, Repository};

fn main() {
    // Tell Cargo to rerun this script if the server directory has changed
    println!("cargo:rerun-if-changed=../../target/tests/server");

    let repository_url = "https://github.com/SirCipher/swim-test-server/";
    let clone_directory = "../../target/tests/server";
    let clone_result = Repository::clone(repository_url, clone_directory);

    let repo = match clone_result {
        Ok(r) => {
            println!("Cloned repository");
            r
        }
        Err(e) => {
            if let ErrorCode::Exists = e.code() {
                println!("Repository already cloned");
                return;
            } else {
                panic!("Error cloning server repository: {:?}", e);
            }
        }
    };

    let repo_path = repo
        .path()
        .parent()
        .expect("Failed to find server repository path");

    println!("Building server");

    let cmd_output = if cfg!(windows) {
        Command::new("cmd")
            .args(&["/C", "gradlew.bat dockerBuildImage"])
            .current_dir(repo_path)
            .output()
            .expect("failed to build docker image")
    } else if cfg!(unix) {
        Command::new("sh")
            .arg("-c")
            .arg("./gradlew dockerBuildImage")
            .current_dir(repo_path)
            .output()
            .expect("failed to build docker image")
    } else {
        panic!("Unsupported operating system for server tests")
    };

    println!("Build output:");

    for out in String::from_utf8(cmd_output.stdout).iter() {
        println!("{}", out);
    }

    assert!(cmd_output.status.success(), "Failed to build test server");
    println!("Built build server");
}
