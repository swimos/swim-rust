// Copyright 2015-2021 Swim Inc.
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

use std::{sync::Arc, time::Duration};

use futures::future::BoxFuture;
use swim_model::path::AbsolutePath;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::error::DownlinkTaskError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownlinkKind {
    Value,
    Event,
    Map,
}

pub struct DownlinkConfig {
    _flush_timeout: Duration,
}

pub trait Downlink {
    fn kind(&self) -> DownlinkKind;

    fn run(
        &self,
        path: AbsolutePath,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>>;
}

impl<T: Downlink> Downlink for Box<T> {
    fn kind(&self) -> DownlinkKind {
        (**self).kind()
    }

    fn run(
        &self,
        path: AbsolutePath,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (**self).run(path, config, input, output)
    }
}

impl<T: Downlink + Send + Sync> Downlink for Arc<T> {
    fn kind(&self) -> DownlinkKind {
        (**self).kind()
    }

    fn run(
        &self,
        path: AbsolutePath,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (**self).run(path, config, input, output)
    }
}
