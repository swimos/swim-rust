// Copyright 2015-2024 Swim Inc.
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

#[cfg(test)]
mod tests;

use crate::{
    forest::{UriForest, UriPart},
    task::AgentMeta,
};
use futures::future::{BoxFuture, Either};
use futures::stream::select;
use futures::{FutureExt, SinkExt, StreamExt, TryFutureExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use swimos_agent_protocol::encoding::lane::{MapLaneResponseEncoder, RawValueLaneRequestDecoder};
use swimos_agent_protocol::{LaneRequest, LaneResponse, MapOperation};
use swimos_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult, WarpLaneKind};
use swimos_api::error::{AgentTaskError, FrameIoError};
use swimos_form::read::{ReadError, ReadEvent, Recognizer, RecognizerReadable};
use swimos_form::write::{StructuralWritable, StructuralWriter};
use swimos_form::Form;
use swimos_model::Text;
use swimos_utilities::trigger;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    routing::RouteUri,
};
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct MetaMeshAgent {
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
}

impl MetaMeshAgent {
    pub fn new(agents: Arc<RwLock<UriForest<AgentMeta>>>) -> MetaMeshAgent {
        MetaMeshAgent { agents }
    }
}

impl Agent for MetaMeshAgent {
    fn run(
        &self,
        _route: RouteUri,
        _route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let MetaMeshAgent { agents } = self;
        run_init(agents.clone(), config, context).boxed()
    }
}

const NODES_LANE: &str = "nodes";
const NODES_COUNT_LANE: &str = "nodes#/";

async fn run_init(
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
    config: AgentConfig,
    context: Box<dyn AgentContext + Send>,
) -> AgentInitResult {
    let mut lane_config = config.default_lane_config.unwrap_or_default();
    lane_config.transient = true;
    let nodes_io = context
        .add_lane(NODES_LANE, WarpLaneKind::DemandMap, lane_config)
        .await?;
    let nodes_count_io = context
        .add_lane(NODES_COUNT_LANE, WarpLaneKind::DemandMap, lane_config)
        .await?;
    Ok(Box::pin(async move {
        let (_shutdown_tx, shutdown_rx) = trigger::trigger();
        run_task(shutdown_rx, agents, context, nodes_io, nodes_count_io)
            .map_err(|error| AgentTaskError::BadFrame {
                lane: Text::from("nodes"),
                error,
            })
            .await
    }))
}

#[derive(Form, Debug, PartialEq, Ord, PartialOrd, Eq)]

pub struct NodeInfoList {
    #[form(name = "nodeUri")]
    node_uri: String,
    created: i64,
    agents: Vec<String>,
}

#[derive(Form, Debug, PartialEq, Ord, PartialOrd, Eq)]

pub struct NodeInfoCount {
    #[form(name = "nodeUri")]
    node_uri: String,
    created: i64,
    #[form(name = "childCount")]
    child_count: usize,
}

#[derive(Debug, PartialEq, Ord, PartialOrd, Eq)]
pub enum NodeInfo {
    List(NodeInfoList),
    Count(NodeInfoCount),
}

impl StructuralWritable for NodeInfo {
    fn num_attributes(&self) -> usize {
        match self {
            NodeInfo::List(item) => item.num_attributes(),
            NodeInfo::Count(item) => item.num_attributes(),
        }
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            NodeInfo::List(item) => item.write_with(writer),
            NodeInfo::Count(item) => item.write_with(writer),
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            NodeInfo::List(item) => item.write_into(writer),
            NodeInfo::Count(item) => item.write_into(writer),
        }
    }
}

pub struct NodeInfoRec<L, R> {
    list: L,
    count: R,
}

impl<L, R> Recognizer for NodeInfoRec<L, R>
where
    L: Recognizer<Target = NodeInfoList>,
    R: Recognizer<Target = NodeInfoCount>,
{
    type Target = NodeInfo;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let NodeInfoRec { list, count } = self;
        match list.feed_event(input.clone()) {
            Some(Ok(item)) => Some(Ok(NodeInfo::List(item))),
            Some(Err(_)) => match count.feed_event(input)? {
                Ok(item) => Some(Ok(NodeInfo::Count(item))),
                Err(e) => Some(Err(e)),
            },
            None => match count.feed_event(input)? {
                Ok(item) => Some(Ok(NodeInfo::Count(item))),
                _ => None,
            },
        }
    }

    fn reset(&mut self) {
        self.list.reset();
        self.count.reset();
    }
}

impl RecognizerReadable for NodeInfo {
    type Rec = NodeInfoRec<
        <NodeInfoList as RecognizerReadable>::Rec,
        <NodeInfoCount as RecognizerReadable>::Rec,
    >;
    type AttrRec = NodeInfoRec<
        <NodeInfoList as RecognizerReadable>::AttrRec,
        <NodeInfoCount as RecognizerReadable>::AttrRec,
    >;
    type BodyRec = NodeInfoRec<
        <NodeInfoList as RecognizerReadable>::BodyRec,
        <NodeInfoCount as RecognizerReadable>::BodyRec,
    >;

    fn make_recognizer() -> Self::Rec {
        NodeInfoRec {
            list: NodeInfoList::make_recognizer(),
            count: NodeInfoCount::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        NodeInfoRec {
            list: NodeInfoList::make_attr_recognizer(),
            count: NodeInfoCount::make_attr_recognizer(),
        }
    }

    fn make_body_recognizer() -> Self::BodyRec {
        NodeInfoRec {
            list: NodeInfoList::make_body_recognizer(),
            count: NodeInfoCount::make_body_recognizer(),
        }
    }
}

type Io = (ByteWriter, ByteReader);

async fn run_task(
    shutdown_rx: trigger::Receiver,
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
    context: Box<dyn AgentContext + Send>,
    nodes_io: Io,
    nodes_count_io: Io,
) -> Result<(), FrameIoError> {
    let (nodes_tx, nodes_rx) = nodes_io;
    let (nodes_count_tx, nodes_count_rx) = nodes_count_io;

    let nodes_input = FramedRead::new(nodes_rx, RawValueLaneRequestDecoder::default());
    let mut nodes_output = FramedWrite::new(nodes_tx, MapLaneResponseEncoder::default());

    let nodes_count_input = FramedRead::new(nodes_count_rx, RawValueLaneRequestDecoder::default());
    let mut nodes_count_output =
        FramedWrite::new(nodes_count_tx, MapLaneResponseEncoder::default());

    let mut request_stream = select(
        nodes_input.map(Either::Left),
        nodes_count_input.map(Either::Right),
    )
    .take_until(shutdown_rx);

    while let Some(request) = request_stream.next().await {
        match request {
            Either::Left(request) => {
                if let LaneRequest::Sync(id) = request? {
                    // Done in two passes in to reduce the time that we hold the lock
                    let parts = {
                        let guard = agents.read();
                        let forest = &*guard;

                        forest
                            .uri_iter()
                            .map(|(node_uri, meta)| NodeInfoList {
                                node_uri,
                                created: meta.created.millis(),
                                agents: vec![meta.name.to_string()],
                            })
                            .collect::<Vec<_>>()
                    };

                    for info in parts {
                        let key = info.node_uri.clone();
                        let op = MapOperation::Update {
                            key: key.as_str(),
                            value: &info,
                        };
                        nodes_output.send(LaneResponse::SyncEvent(id, op)).await?;
                    }

                    let synced: LaneResponse<MapOperation<&str, &NodeInfoList>> =
                        LaneResponse::Synced(id);
                    nodes_output.send(synced).await?;
                }
            }
            Either::Right(request) => {
                if let LaneRequest::Sync(id) = request? {
                    // Done in two passes in to reduce the time that we hold the lock
                    let parts = {
                        let guard = agents.read();
                        let forest = &*guard;
                        forest
                            .part_iter()
                            .map(|part| match part {
                                UriPart::Leaf { path, data } => {
                                    let info = NodeInfoList {
                                        node_uri: path.clone(),
                                        created: data.created.millis(),
                                        agents: vec![data.name.clone().into()],
                                    };
                                    (path, NodeInfo::List(info))
                                }
                                UriPart::Junction { path, descendants } => {
                                    let info = NodeInfoCount {
                                        node_uri: path.clone(),
                                        created: 0,
                                        child_count: descendants,
                                    };
                                    (path, NodeInfo::Count(info))
                                }
                            })
                            .collect::<Vec<_>>()
                    };

                    for (path, info) in parts {
                        nodes_count_output
                            .send(LaneResponse::SyncEvent(
                                id,
                                MapOperation::Update {
                                    key: path.as_str(),
                                    value: &info,
                                },
                            ))
                            .await?
                    }

                    let synced: LaneResponse<MapOperation<&str, &()>> = LaneResponse::Synced(id);
                    nodes_count_output.send(synced).await?;
                }
            }
        }
    }

    // deferred drop so the agent doesn't terminate early.
    let _context = context;

    Ok(())
}
