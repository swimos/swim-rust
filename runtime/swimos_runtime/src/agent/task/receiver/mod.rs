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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{ready, Stream, StreamExt};
use swimos_agent_protocol::{
    encoding::lane::{RawMapLaneResponseDecoder, RawValueLaneResponseDecoder},
    encoding::store::{RawMapStoreResponseDecoder, RawValueStoreResponseDecoder},
    LaneResponse, MapLaneResponse, MapOperation, StoreResponse,
};
use swimos_api::agent::UplinkKind;
use swimos_utilities::byte_channel::ByteReader;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use super::remotes::UplinkResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueOrSupply {
    Value,
    Supply,
}

impl ValueOrSupply {
    fn uplink_kind(&self) -> UplinkKind {
        match self {
            ValueOrSupply::Value => UplinkKind::Value,
            ValueOrSupply::Supply => UplinkKind::Supply,
        }
    }
}

/// Error type indicating that a lane has failed (specifying its ID).
#[derive(Debug)]
pub enum Failed {
    Lane(u64),
    Store(u64),
}

impl<I> ItemResponse<I> {
    pub fn is_lane(&self) -> bool {
        matches!(
            self,
            ItemResponse {
                body: ResponseData::Lane(..),
                ..
            }
        )
    }
}

/// Content received from a lane over a byte channel.
#[derive(Debug)]
pub struct LaneData {
    pub target: Option<Uuid>,
    pub response: UplinkResponse,
}

impl LaneData {
    pub fn new(target: Option<Uuid>, response: UplinkResponse) -> Self {
        LaneData { target, response }
    }
}

/// Content received from a store over a byte channel.
#[derive(Debug)]
pub enum StoreData {
    Value(Bytes),
    Map(MapOperation<BytesMut, BytesMut>),
}

/// Content received from an item (lane or store) over a byte channel.
#[derive(Debug)]
pub enum ResponseData {
    Lane(LaneData),
    Store(StoreData),
}

/// A response message received from an item of the agent with attached identifiers.
#[derive(Debug)]
pub struct ItemResponse<I> {
    /// The internal identifier used to distinguish the items of the agent.
    pub item_id: u64,
    /// The stable identifier used by the store to identify the item (absent for transient items).
    pub store_id: Option<I>,
    /// The content.
    pub body: ResponseData,
}

impl<I> ItemResponse<I> {
    pub fn value_lane(
        item_id: u64,
        store_id: Option<I>,
        target: Option<Uuid>,
        body: Bytes,
    ) -> Self {
        ItemResponse {
            item_id,
            store_id,
            body: ResponseData::Lane(LaneData::new(target, UplinkResponse::Value(body))),
        }
    }

    pub fn supply_lane(
        item_id: u64,
        store_id: Option<I>,
        target: Option<Uuid>,
        body: Bytes,
    ) -> Self {
        ItemResponse {
            item_id,
            store_id,
            body: ResponseData::Lane(LaneData::new(target, UplinkResponse::Supply(body))),
        }
    }

    pub fn map_lane(
        item_id: u64,
        store_id: Option<I>,
        target: Option<Uuid>,
        body: MapOperation<BytesMut, BytesMut>,
    ) -> Self {
        ItemResponse {
            item_id,
            store_id,
            body: ResponseData::Lane(LaneData::new(target, UplinkResponse::Map(body))),
        }
    }

    pub fn lane_synced(item_id: u64, target: Uuid, kind: UplinkKind) -> Self {
        ItemResponse {
            item_id,
            store_id: None,
            body: ResponseData::Lane(LaneData::new(Some(target), UplinkResponse::Synced(kind))),
        }
    }

    pub fn value_store(item_id: u64, store_id: I, body: Bytes) -> Self {
        ItemResponse {
            item_id,
            store_id: Some(store_id),
            body: ResponseData::Store(StoreData::Value(body)),
        }
    }

    pub fn map_store(item_id: u64, store_id: I, body: MapOperation<BytesMut, BytesMut>) -> Self {
        ItemResponse {
            item_id,
            store_id: Some(store_id),
            body: ResponseData::Store(StoreData::Map(body)),
        }
    }
}

impl<I> ItemResponse<I> {
    pub fn into_uplink_response(self) -> Option<(u64, LaneData)> {
        let ItemResponse { item_id, body, .. } = self;
        if let ResponseData::Lane(resp) = body {
            Some((item_id, resp))
        } else {
            None
        }
    }
}

/// Enumeration over receivers for each type of lane or store. This is to allow the agent runtime
/// to wait on responses from all if its items with a single [`futures::stream::FuturesUnordered`].
#[derive(Debug)]
pub enum ResponseReceiver<I> {
    ValueLikeLane {
        item_id: u64,
        store_id: Option<I>,
        uplink: ValueOrSupply,
        reader: FramedRead<ByteReader, RawValueLaneResponseDecoder>,
    },
    MapLane {
        item_id: u64,
        store_id: Option<I>,
        reader: FramedRead<ByteReader, RawMapLaneResponseDecoder>,
    },
    ValueStore {
        item_id: u64,
        store_id: I,
        reader: FramedRead<ByteReader, RawValueStoreResponseDecoder>,
    },
    MapStore {
        item_id: u64,
        store_id: I,
        reader: FramedRead<ByteReader, RawMapStoreResponseDecoder>,
    },
}

impl<I> ResponseReceiver<I> {
    pub fn value_like_lane(item_id: u64, store_id: Option<I>, rx: ByteReader) -> Self {
        ResponseReceiver::ValueLikeLane {
            item_id,
            store_id,
            uplink: ValueOrSupply::Value,
            reader: FramedRead::new(rx, Default::default()),
        }
    }

    pub fn supply_lane(item_id: u64, store_id: Option<I>, rx: ByteReader) -> Self {
        ResponseReceiver::ValueLikeLane {
            item_id,
            store_id,
            uplink: ValueOrSupply::Supply,
            reader: FramedRead::new(rx, Default::default()),
        }
    }

    pub fn map_lane(item_id: u64, store_id: Option<I>, rx: ByteReader) -> Self {
        ResponseReceiver::MapLane {
            item_id,
            store_id,
            reader: FramedRead::new(rx, Default::default()),
        }
    }

    pub fn value_store(item_id: u64, store_id: I, rx: ByteReader) -> Self {
        ResponseReceiver::ValueStore {
            item_id,
            store_id,
            reader: FramedRead::new(rx, Default::default()),
        }
    }

    pub fn map_store(item_id: u64, store_id: I, rx: ByteReader) -> Self {
        ResponseReceiver::MapStore {
            item_id,
            store_id,
            reader: FramedRead::new(rx, Default::default()),
        }
    }
}

impl<I: Copy + Unpin> Stream for ResponseReceiver<I> {
    type Item = Result<ItemResponse<I>, Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            ResponseReceiver::ValueLikeLane {
                item_id,
                store_id,
                uplink,
                reader,
            } => {
                let next = loop {
                    let maybe_result = ready!(reader.poll_next_unpin(cx));

                    match maybe_result {
                        Some(Ok(r)) => {
                            if let Some(resp) =
                                value_or_supply_raw_response(*item_id, r, *uplink, *store_id)
                            {
                                break Some(Ok(resp));
                            }
                        }
                        Some(Err(_)) => break Some(Err(Failed::Lane(*item_id))),
                        _ => break None,
                    };
                };
                Poll::Ready(next)
            }
            ResponseReceiver::MapLane {
                item_id,
                store_id,
                reader,
            } => {
                let next = loop {
                    let maybe_result = ready!(reader.poll_next_unpin(cx));
                    match maybe_result {
                        Some(Ok(r)) => {
                            if let Some(resp) = map_raw_response(*item_id, r, *store_id) {
                                break Some(Ok(resp));
                            }
                        }
                        Some(Err(_)) => break Some(Err(Failed::Lane(*item_id))),
                        _ => break None,
                    };
                };
                Poll::Ready(next)
            }
            ResponseReceiver::ValueStore {
                item_id,
                store_id,
                reader,
            } => match ready!(reader.poll_next_unpin(cx)) {
                Some(Ok(StoreResponse { message })) => Poll::Ready(Some(Ok(
                    ItemResponse::value_store(*item_id, *store_id, message.freeze()),
                ))),
                Some(Err(_)) => Poll::Ready(Some(Err(Failed::Store(*item_id)))),
                _ => Poll::Ready(None),
            },
            ResponseReceiver::MapStore {
                item_id,
                store_id,
                reader,
            } => match ready!(reader.poll_next_unpin(cx)) {
                Some(Ok(StoreResponse { message })) => Poll::Ready(Some(Ok(
                    ItemResponse::map_store(*item_id, *store_id, message),
                ))),
                Some(Err(_)) => Poll::Ready(Some(Err(Failed::Store(*item_id)))),
                _ => Poll::Ready(None),
            },
        }
    }
}

fn value_or_supply_raw_response<I>(
    item_id: u64,
    resp: LaneResponse<BytesMut>,
    uplink: ValueOrSupply,
    store_id: Option<I>,
) -> Option<ItemResponse<I>> {
    match resp {
        LaneResponse::StandardEvent(body) => match uplink {
            ValueOrSupply::Value => Some(ItemResponse::value_lane(
                item_id,
                store_id,
                None,
                body.freeze(),
            )),
            ValueOrSupply::Supply => Some(ItemResponse::supply_lane(
                item_id,
                store_id,
                None,
                body.freeze(),
            )),
        },
        LaneResponse::Initialized => None,
        LaneResponse::SyncEvent(id, body) => match uplink {
            ValueOrSupply::Value => Some(ItemResponse::value_lane(
                item_id,
                store_id,
                Some(id),
                body.freeze(),
            )),
            ValueOrSupply::Supply => Some(ItemResponse::supply_lane(
                item_id,
                store_id,
                Some(id),
                body.freeze(),
            )),
        },
        LaneResponse::Synced(id) => {
            Some(ItemResponse::lane_synced(item_id, id, uplink.uplink_kind()))
        }
    }
}

fn map_raw_response<I>(
    item_id: u64,
    resp: MapLaneResponse<BytesMut, BytesMut>,
    store_id: Option<I>,
) -> Option<ItemResponse<I>> {
    match resp {
        LaneResponse::StandardEvent(body) => {
            Some(ItemResponse::map_lane(item_id, store_id, None, body))
        }
        LaneResponse::Initialized => None,
        LaneResponse::SyncEvent(id, body) => {
            Some(ItemResponse::map_lane(item_id, store_id, Some(id), body))
        }
        LaneResponse::Synced(id) => Some(ItemResponse::lane_synced(item_id, id, UplinkKind::Map)),
    }
}
