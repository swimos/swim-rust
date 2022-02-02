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

use crate::compat::{ResponseMessage, ResponseMessageEncoder};
use futures::sink;
use futures::SinkExt;
use std::marker::PhantomData;
use swim_form::structural::write::StructuralWritable;
use swim_utilities::future::item_sink::ItemSink;
use tokio::io::AsyncWrite;
use tokio_util::codec::FramedWrite;

pub struct RouteSender<T, W> {
    inner: FramedWrite<W, ResponseMessageEncoder>,
    _type: PhantomData<fn(T)>,
}

impl<T, W: AsyncWrite> RouteSender<T, W> {
    pub fn new(writer: W) -> Self {
        RouteSender {
            inner: FramedWrite::new(writer, ResponseMessageEncoder),
            _type: PhantomData,
        }
    }
}

impl<'a, T, U, W> ItemSink<'a, ResponseMessage<T, U>> for RouteSender<T, W>
where
    T: StructuralWritable + Send + 'static,
    U: AsRef<[u8]> + Send + 'a,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = std::io::Error;
    type SendFuture = sink::Send<'a, FramedWrite<W, ResponseMessageEncoder>, ResponseMessage<T, U>>;

    fn send_item(&'a mut self, value: ResponseMessage<T, U>) -> Self::SendFuture {
        self.inner.send(value)
    }
}
