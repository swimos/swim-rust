// Copyright 2015-2021 SWIM.AI inc.
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

#![allow(clippy::match_wild_err_arm)]

#[cfg(feature = "enabled")]
mod enabled {
    use std::env;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::str::FromStr;

    use git2::build::RepoBuilder;
    use git2::{Error, ErrorCode, Repository};
    use std::ffi::OsString;

    fn get_tags(repo: &Repository) -> Vec<String> {
        match repo.tag_names(None) {
            Ok(tags) => tags
                .iter()
                .filter_map(|e| match e {
                    Some(t) => Some(t.to_string()),
                    None => None,
                })
                .collect(),
            Err(_) => vec![],
        }
    }

    pub fn init_server() {
        let tag = match env::var_os("TEST_SERVER_VERSION") {
            Some(tag) => tag,
            None => {
                return;
            }
        };

        if env::var("TEST_SERVER_IMAGE_NAME").is_err() {
            panic!("TEST_SERVER_IMAGE_NAME must be set");
        }

        let out_dir = env::var("OUT_DIR").unwrap();

        let mut out_dir = PathBuf::from_str(&out_dir).unwrap();
        out_dir.push("tests");
        out_dir.push("server");

        println!("Building server");

        let repo_path = match clone_repo(&out_dir, tag) {
            Ok(p) => p,
            Err(e) => {
                if let ErrorCode::Exists = e.code() {
                    println!("Repository already cloned");
                    return;
                } else {
                    panic!("Error cloning server repository: {:?}", e);
                }
            }
        };

        let parent = repo_path.parent().expect("");
        build_image(parent);

        // Tell Cargo to rerun this script if the server directory has changed
        println!("cargo:rerun-if-changed={:?}", out_dir);

        // Enable the test server
        println!("cargo:rustc-cfg=test_server");

        // Enable the test server
        println!("Built build server");
    }

    fn clone_repo(out_dir: &PathBuf, tag: OsString) -> Result<PathBuf, Error> {
        let repo_url = match env::var("REPO_URL") {
            Ok(url) => url,
            Err(_) => panic!("REPO_URL environment variable must be set"),
        };

        let clone_result = RepoBuilder::new().clone(&repo_url, out_dir.as_path());

        match clone_result {
            Ok(r) => {
                println!("Cloned repository");

                let tag = match r.refname_to_id(&("refs/tags/".to_owned() + &tag.to_string_lossy()))
                {
                    Ok(oid) => oid,
                    Err(_) => {
                        let tags = get_tags(&r);

                        panic!(
                            "Failed to find tag {:?} in repository. \n Valid tags are:\n {:?}",
                            tag, tags
                        );
                    }
                };

                let path = r.path().to_owned();

                match r.find_tag(tag) {
                    Ok(sha1) => {
                        if let Err(e) = r.checkout_tree(&sha1.clone().into_object(), None) {
                            panic!(
                                "Failed to checkout tag {:?}, at commit {:?}. {:?}",
                                tag, sha1, e
                            );
                        }

                        Ok(path)
                    }
                    Err(_) => {
                        let tags = get_tags(&r);

                        panic!(
                            "Failed to find tag {:?} in repository. \n Valid tags are:\n {:?}",
                            tag, tags
                        );
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    fn build_image(repo_path: &Path) {
        let cmd_output = if cfg!(windows) {
            Command::new("cmd")
                .args(&["/C", "gradlew.bat dockerBuildImage --info"])
                .current_dir(repo_path)
                .output()
                .expect("failed to build docker image")
        } else if cfg!(unix) {
            Command::new("sh")
                .arg("-c")
                .arg("./gradlew dockerBuildImage --info")
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
    }
}

#[cfg(feature = "enabled")]
fn main() {
    enabled::init_server();
}

#[cfg(not(feature = "enabled"))]
fn main() {}
