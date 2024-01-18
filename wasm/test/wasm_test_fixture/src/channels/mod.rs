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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::{ready, Stream};
use futures_util::stream::SelectAll;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};

use swim_api::error::FrameIoError;
use swim_api::protocol::agent::{
    LaneRequest, LaneRequestEncoder, LaneResponse, MapLaneResponseDecoder, ValueLaneResponseDecoder,
};
use swim_api::protocol::map::{MapMessage, RawMapOperationMut};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::bilock::{bilock, BiLock};

#[cfg(test)]
mod tests;

#[derive(Debug)]
enum LaneDecoder {
    Value(FramedRead<ByteReader, ValueLaneResponseDecoder>),
    Map(FramedRead<ByteReader, MapLaneResponseDecoder>),
}

impl LaneDecoder {
    fn value(rx: ByteReader) -> LaneDecoder {
        LaneDecoder::Value(FramedRead::new(rx, ValueLaneResponseDecoder::default()))
    }

    fn map(rx: ByteReader) -> LaneDecoder {
        LaneDecoder::Map(FramedRead::new(rx, MapLaneResponseDecoder::default()))
    }
}

impl Stream for LaneDecoder {
    type Item = Result<LaneResponseDiscriminant, FrameIoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().get_mut() {
            LaneDecoder::Value(dec) => {
                let item = ready!(Pin::new(dec).poll_next(cx));
                Poll::Ready(item.map(|resp| resp.map(LaneResponseDiscriminant::Value)))
            }
            LaneDecoder::Map(dec) => {
                let item = ready!(Pin::new(dec).poll_next(cx));
                Poll::Ready(item.map(|resp| resp.map(LaneResponseDiscriminant::Map)))
            }
        }
    }
}

#[derive(Debug)]
struct TaggedLaneStream {
    lane_uri: String,
    decoder: LaneDecoder,
}

impl Stream for TaggedLaneStream {
    type Item = Result<(String, LaneResponseDiscriminant), FrameIoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(Pin::new(&mut self.decoder).poll_next(cx));
        Poll::Ready(item.map(|resp| resp.map(|disc| (self.lane_uri.clone(), disc))))
    }
}

#[derive(Debug, Default)]
struct ChannelsInner {
    readers: SelectAll<TaggedLaneStream>,
    writers: HashMap<String, ByteWriter>,
}

pub fn channels() -> (ChannelsSender, ChannelsReceiver) {
    let (tx, rx) = bilock(ChannelsInner::default());
    let sender = ChannelsSender { inner: tx };
    let receiver = ChannelsReceiver { inner: rx };

    (sender, receiver)
}

#[derive(Debug)]
pub struct ChannelsSender {
    inner: BiLock<ChannelsInner>,
}

impl ChannelsSender {
    pub async fn push_value_channel(&self, tx: ByteWriter, rx: ByteReader, lane_uri: String) {
        let guard = &mut *self.inner.lock().await;
        guard.readers.push(TaggedLaneStream {
            lane_uri: lane_uri.clone(),
            decoder: LaneDecoder::value(rx),
        });
        guard.writers.insert(lane_uri, tx);
    }

    pub async fn push_map_channel(&self, tx: ByteWriter, rx: ByteReader, lane_uri: String) {
        let guard = &mut *self.inner.lock().await;
        guard.readers.push(TaggedLaneStream {
            lane_uri: lane_uri.clone(),
            decoder: LaneDecoder::map(rx),
        });
        guard.writers.insert(lane_uri, tx);
    }

    pub async fn send<R: Into<LaneRequestDiscriminant>>(&self, lane_uri: String, request: R) {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        let writer = inner
            .writers
            .get_mut(&lane_uri)
            .expect(&format!("Missing channel for lane: {lane_uri}"));
        let mut framed = FramedWrite::new(writer, LaneRequestDiscriminantEncoder);

        framed.send(request.into()).await.expect("Channel closed");
    }

    pub async fn send_bytes(&self, lane_uri: String, buf: &[u8]) {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        let writer = inner.writers.get_mut(&lane_uri).expect("Missing channel");

        writer.write_all(buf).await.expect("Channel closed");
    }

    pub async fn drop_all(&self) {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        inner.writers.clear();
        inner.readers.clear();
    }
}

#[derive(Debug)]
pub struct ChannelsReceiver {
    inner: BiLock<ChannelsInner>,
}

impl ChannelsReceiver {
    pub async fn clear(&self) {
        let mut guard = self.inner.lock().await;
        guard.writers.clear();
        guard.readers.clear();
    }

    pub async fn with_writer<O, F, Fut>(&self, uri: &str, func: F) -> Option<O>
    where
        F: FnOnce(ByteWriter) -> Fut,
        Fut: Future<Output = (ByteWriter, O)>,
    {
        let mut guard = self.inner.lock().await;
        match guard.writers.remove(uri) {
            Some(writer) => {
                let (writer, result) = func(writer).await;
                guard.writers.insert(uri.to_string(), writer);
                Some(result)
            }
            None => None,
        }
    }

    pub async fn expect_value_event(&mut self, expected_uri: &str, expected_event: &str) {
        match self.next().await {
            Some((actual_uri, actual_response)) => {
                assert_eq!(actual_uri, expected_uri);
                assert_eq!(
                    actual_response,
                    LaneResponseDiscriminant::Value(LaneResponse::StandardEvent(BytesMut::from(
                        expected_event
                    ),))
                );
            }
            None => {
                panic!("No message")
            }
        }
    }
}

impl Stream for ChannelsReceiver {
    type Item = (String, LaneResponseDiscriminant);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut guard = ready!(self.inner.poll_lock(cx));
            let inner = &mut *guard;

            match ready!(Pin::new(&mut inner.readers).poll_next(cx)) {
                Some(Ok(item)) => return Poll::Ready(Some(item)),
                Some(Err(e)) => {
                    panic!("Read error: {:?}", e);
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum LaneResponseDiscriminant {
    Value(LaneResponse<BytesMut>),
    Map(LaneResponse<RawMapOperationMut>),
}

impl LaneResponseDiscriminant {
    pub fn expect_value(self) -> LaneResponse<BytesMut> {
        match self {
            LaneResponseDiscriminant::Value(r) => r,
            LaneResponseDiscriminant::Map(_) => {
                panic!("Expected a value message")
            }
        }
    }
}

impl From<LaneResponse<BytesMut>> for LaneResponseDiscriminant {
    fn from(value: LaneResponse<BytesMut>) -> Self {
        LaneResponseDiscriminant::Value(value)
    }
}

impl From<LaneResponse<RawMapOperationMut>> for LaneResponseDiscriminant {
    fn from(value: LaneResponse<RawMapOperationMut>) -> Self {
        LaneResponseDiscriminant::Map(value)
    }
}

pub struct LaneRequestDiscriminantEncoder;

impl Encoder<LaneRequestDiscriminant> for LaneRequestDiscriminantEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: LaneRequestDiscriminant,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            LaneRequestDiscriminant::Value(r) => LaneRequestEncoder::value().encode(r, dst),
            LaneRequestDiscriminant::Map(r) => LaneRequestEncoder::map().encode(r, dst),
        }
    }
}

#[derive(Debug)]
pub enum LaneRequestDiscriminant {
    Value(LaneRequest<BytesMut>),
    Map(LaneRequest<MapMessage<BytesMut, BytesMut>>),
}

impl From<LaneRequest<BytesMut>> for LaneRequestDiscriminant {
    fn from(value: LaneRequest<BytesMut>) -> Self {
        LaneRequestDiscriminant::Value(value)
    }
}

impl From<LaneRequest<MapMessage<BytesMut, BytesMut>>> for LaneRequestDiscriminant {
    fn from(value: LaneRequest<MapMessage<BytesMut, BytesMut>>) -> Self {
        LaneRequestDiscriminant::Map(value)
    }
}
