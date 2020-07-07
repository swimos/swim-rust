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

use crate::model::Value;
use base64::write::EncoderWriter;
use base64::Config;
use std::io;
use std::io::Write;

#[derive(Debug, Clone)]
pub struct Blob {
    data: Vec<u8>,
    config: Config,
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Default for Blob {
    fn default() -> Blob {
        Blob {
            data: Vec::new(),
            config: base64::URL_SAFE,
        }
    }
}

impl Blob {
    pub fn new(config: Config) -> Blob {
        Blob {
            data: Vec::new(),
            config,
        }
    }

    pub fn with_capacity(cap: usize, config: Config) -> Blob {
        Blob {
            data: Vec::with_capacity(cap),
            config,
        }
    }

    pub fn from_vec(data: Vec<u8>, config: Config) -> Blob {
        Blob { data, config }
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn reserve(&mut self, n: usize) {
        self.data.reserve(n);
    }

    pub fn encode<W: Write>(&self, mut writer: W) -> io::Result<()> {
        EncoderWriter::new(&mut writer, self.config).write_all(&self.data)
    }

    pub fn decode<T>(encoded: T, config: Config) -> Result<Blob, base64::DecodeError>
    where
        T: AsRef<[u8]>,
    {
        base64::decode_config(encoded.as_ref(), config).map(|b| Blob::from_vec(b, config))
    }

    pub fn append<T>(&mut self, encoded: T, config: Config) -> Result<(), base64::DecodeError>
    where
        T: AsRef<[u8]>,
    {
        base64::decode_config_buf(encoded.as_ref(), config, &mut self.data)
    }
}
