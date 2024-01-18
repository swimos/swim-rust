// Copyright 2015-2023 Swim Inc.
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

use crate::workspace::CARGO_FILE_NAME;
use anyhow::Result;
use cargo_toml::Manifest;
use std::env::current_dir;
use std::fs;

pub fn add_member(new_member: &str) -> Result<()> {
    let mut path = current_dir()?;
    path.push(CARGO_FILE_NAME);

    let mut manifest = Manifest::from_path(&path)?;
    let workspace = manifest
        .workspace
        .as_mut()
        .expect("Missing workspace in manifest");

    for member in &workspace.members {
        if member == new_member {
            return Ok(());
        }
    }

    workspace.members.push(new_member.to_string());

    let manifest_str = toml::to_string_pretty(&manifest).expect("Failed to serialize manifest");
    fs::write(path, manifest_str)?;

    Ok(())
}
