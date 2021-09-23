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

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::{fs, io};
use tempdir::TempDir;

/// A directory on the file system that is either persistence or transient.
#[derive(Debug)]
pub enum Dir {
    /// A transient directory on the filesystem that is automatically deleted when it is dropped.
    Transient(TempDir),
    /// A persistent directory on the filesystem.
    Persistent(PathBuf),
}

impl Dir {
    /// Attempts to create a directory at the provided path. If the path already exists, then
    /// this will return successfully.
    ///
    /// # Errors
    /// Errors if the directory cannot be created.
    pub fn persistent<I: AsRef<Path>>(path: I) -> io::Result<Dir> {
        match fs::create_dir(&path) {
            Ok(_) => Ok(Dir::Persistent(path.as_ref().to_path_buf())),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                Ok(Dir::Persistent(path.as_ref().to_path_buf()))
            }
            Err(e) => Err(e),
        }
    }

    /// Creates a temporary directory on the filesystem prefixed by `prefix` that will be deleted
    /// when it is dropped.
    ///
    /// # Panics
    /// Panics if the temporary directory cannot be opened.
    pub fn transient(prefix: &str) -> io::Result<Dir> {
        let temp_dir = TempDir::new(prefix)?;
        Ok(Dir::Transient(temp_dir))
    }

    /// Returns path of the directory.
    pub fn path(&self) -> &Path {
        match self {
            Dir::Transient(dir) => dir.path(),
            Dir::Persistent(path) => path.as_path(),
        }
    }
}
