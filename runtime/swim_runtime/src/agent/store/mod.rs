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

use bytes::BytesMut;
use futures::{
    future::{ready, BoxFuture},
    FutureExt, SinkExt,
};
use swim_api::{
    error::StoreError,
    protocol::{
        agent::{LaneRequest, LaneRequestEncoder},
        map::{MapMessage, MapMessageEncoder, MapOperation, RawMapOperationEncoder},
        WithLengthBytesCodec,
    },
    store::{NodePersistence, NodePersistenceBase, RangeConsumer},
};
use swim_utilities::io::byte_channel::ByteWriter;
use thiserror::Error;
use tokio_util::codec::FramedWrite;

#[derive(Debug, Error)]
pub enum StoreInitError {
    #[error("An error occurred reading the state from the store.")]
    Store(#[from] StoreError),
    #[error("An error occurred sendig the state to the agent implementation.")]
    Channel(#[from] std::io::Error),
}

pub trait AgentPersistence {
    type LaneId: Copy;

    fn lane_id(&self, name: &str) -> Result<Self::LaneId, StoreError>;

    fn init_value_lane<'a>(
        &'a self,
        _lane_id: Self::LaneId,
        _channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>>;

    fn init_map_lane<'a>(
        &'a self,
        _lane_id: Self::LaneId,
        _channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>>;

    fn put_value(&self, lane_id: Self::LaneId, bytes: &[u8]) -> Result<(), StoreError>;

    fn put_map<B: AsRef<[u8]>>(
        &self,
        lane_id: Self::LaneId,
        op: MapOperation<B, B>,
    ) -> Result<(), StoreError>;
}

#[derive(Debug, Clone, Copy)]
pub struct StoreDisabled;

impl AgentPersistence for StoreDisabled {
    type LaneId = ();

    fn lane_id(&self, _name: &str) -> Result<Self::LaneId, StoreError> {
        Ok(())
    }

    fn init_value_lane<'a>(
        &'a self,
        _lane_id: Self::LaneId,
        _channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>> {
        ready(Ok(())).boxed()
    }

    fn init_map_lane<'a>(
        &'a self,
        _lane_id: Self::LaneId,
        _channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>> {
        ready(Ok(())).boxed()
    }

    fn put_value(&self, _lane_id: Self::LaneId, _bytes: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn put_map<B: AsRef<[u8]>>(
        &self,
        _lane_id: Self::LaneId,
        _op: MapOperation<B, B>,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

pub struct StorePersistence<S>(pub S);

type ValueLaneEncoder = LaneRequestEncoder<WithLengthBytesCodec>;
type MapLaneEncoder = LaneRequestEncoder<MapMessageEncoder<RawMapOperationEncoder>>;

impl<S> AgentPersistence for StorePersistence<S>
where
    S: NodePersistence + Send + Sync + 'static,
{
    type LaneId = <S as NodePersistenceBase>::LaneId;

    fn lane_id(&self, name: &str) -> Result<Self::LaneId, StoreError> {
        let StorePersistence(store) = self;
        store.id_for(name)
    }

    fn put_value(&self, lane_id: Self::LaneId, bytes: &[u8]) -> Result<(), StoreError> {
        let StorePersistence(store) = self;
        store.put_value(lane_id, bytes)
    }

    fn put_map<B: AsRef<[u8]>>(
        &self,
        lane_id: Self::LaneId,
        op: MapOperation<B, B>,
    ) -> Result<(), StoreError> {
        let StorePersistence(store) = self;
        match op {
            MapOperation::Update { key, value } => {
                store.update_map(lane_id, key.as_ref(), value.as_ref())
            }
            MapOperation::Remove { key } => store.remove_map(lane_id, key.as_ref()),
            MapOperation::Clear => store.clear(lane_id),
        }
    }

    fn init_value_lane<'a>(
        &'a self,
        lane_id: Self::LaneId,
        channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>> {
        async move {
            let StorePersistence(store) = self;
            let mut writer = FramedWrite::new(channel, ValueLaneEncoder::default());
            let mut buffer = BytesMut::new();
            if store.get_value(lane_id, &mut buffer)?.is_some() {
                writer.send(LaneRequest::Command(buffer)).await?;
            }
            writer.send(LaneRequest::<&[u8]>::InitComplete).await?;
            Ok(())
        }
        .boxed()
    }

    fn init_map_lane<'a>(
        &'a self,
        lane_id: Self::LaneId,
        channel: &'a mut ByteWriter,
    ) -> BoxFuture<'a, Result<(), StoreInitError>> {
        async move {
            let StorePersistence(store) = self;
            let mut writer = FramedWrite::new(channel, MapLaneEncoder::default());
            let mut it = store.read_map(lane_id)?;
            while let Some((key, value)) = it.consume_next()? {
                writer
                    .send(LaneRequest::Command(MapMessage::Update { key, value }))
                    .await?;
            }
            writer
                .send(LaneRequest::<MapMessage<&[u8], &[u8]>>::InitComplete)
                .await?;
            Ok(())
        }
        .boxed()
    }
}
