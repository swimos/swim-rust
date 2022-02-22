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

use crate::model::lifecycle::{EventDownlinkLifecycle, ValueDownlinkLifecycle};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_api::downlink::{Downlink, DownlinkConfig, DownlinkKind};
use swim_api::error::DownlinkTaskError;

use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::path::Path;

use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::{EventDownlinkModel, ValueDownlinkModel};

mod event;
#[cfg(test)]
mod tests;
mod value;

/// Wrapper for downlink models to allow them to serve as a [`Downlink`].
pub struct DownlinkTask<Model>(Model);

impl<Model> DownlinkTask<Model> {
    pub fn new(model: Model) -> Self {
        DownlinkTask(model)
    }
}

impl<T, LC> Downlink for DownlinkTask<ValueDownlinkModel<T, LC>>
where
    T: Form + Send + Sync + 'static,
    <T as RecognizerReadable>::Rec: Send,
    LC: ValueDownlinkLifecycle<T> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn run(
        self,
        path: Path,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        value::value_dowinlink_task(model, path, config, input, output).boxed()
    }
}

impl<T, LC> Downlink for DownlinkTask<EventDownlinkModel<T, LC>>
where
    T: Form + Send + Sync + 'static,
    <T as RecognizerReadable>::Rec: Send,
    LC: EventDownlinkLifecycle<T> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Event
    }

    fn run(
        self,
        path: Path,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        event::event_dowinlink_task(model, path, config, input, output).boxed()
    }
}
