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

use crate::model::lifecycle::{
    EventDownlinkLifecycle, MapDownlinkLifecycle, ValueDownlinkLifecycle,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::hash::Hash;
use swimos_api::{address::Address, agent::DownlinkKind, error::DownlinkTaskError};
use swimos_client_api::{Downlink, DownlinkConfig};

use swimos_form::read::RecognizerReadable;
use swimos_form::Form;
use swimos_model::Text;

use swimos_utilities::io::byte_channel::{ByteReader, ByteWriter};

use tracing::{info_span, Instrument};

use crate::model::MapDownlinkModel;
use crate::{EventDownlinkModel, ValueDownlinkModel};

mod event;
mod map;
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
    T: Form + Send + Sync + Clone + 'static,
    <T as RecognizerReadable>::Rec: Send,
    LC: ValueDownlinkLifecycle<T> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        value::value_downlink_task(model, path, config, input, output)
            .instrument(info_span!("Downlink task.", kind = ?DownlinkKind::Value))
            .boxed()
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
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
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        event::event_downlink_task(model, path, config, input, output)
            .instrument(info_span!("Downlink task.", kind = ?DownlinkKind::Event))
            .boxed()
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
    }
}

pub trait MapKey:
    Clone + Form + Send + Sync + Eq + Hash + Ord + RecognizerReadable + 'static
{
}
impl<T> MapKey for T where
    T: Clone + Form + Send + Sync + Eq + Hash + Ord + RecognizerReadable + 'static
{
}

pub trait MapValue: Clone + Form + Send + Sync + Eq + 'static {}
impl<T> MapValue for T where T: Clone + Form + Send + Sync + Eq + 'static {}

impl<K, V, LC> Downlink for DownlinkTask<MapDownlinkModel<K, V, LC>>
where
    K: MapKey,
    K::Rec: Send,
    K::BodyRec: Send,
    V: MapValue,
    V::Rec: Send,
    V::BodyRec: Send,
    LC: MapDownlinkLifecycle<K, V> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Map
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        map::map_downlink_task(model, path, config, input, output)
            .instrument(info_span!("Downlink task.", kind = ?DownlinkKind::Map))
            .boxed()
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
    }
}
