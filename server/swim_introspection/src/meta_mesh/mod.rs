use crate::task::AgentMeta;
use futures::future::{BoxFuture, Either};
use futures::stream::select;
use futures::{FutureExt, SinkExt, StreamExt, TryFutureExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use swim_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult};
use swim_api::error::{AgentTaskError, FrameIoError};
use swim_api::meta::lane::LaneKind;
use swim_api::protocol::agent::{
    LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder,
};
use swim_api::protocol::map::{MapOperation, MapOperationEncoder};
use swim_api::protocol::WithLengthBytesCodec;
use swim_form::Form;
use swim_model::time::Timestamp;
use swim_model::Text;
use swim_runtime::downlink::Io;
use swim_utilities::routing::route_uri::RouteUri;
use swim_utilities::uri_forest::{UriForest, UriPart};
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
        .add_lane(NODES_LANE, LaneKind::DemandMap, lane_config)
        .await?;
    let nodes_count_io = context
        .add_lane(NODES_COUNT_LANE, LaneKind::DemandMap, lane_config)
        .await?;
    Ok(run_task(agents, context, nodes_io, nodes_count_io)
        .map_err(|error| AgentTaskError::BadFrame {
            lane: Text::from("nodes"),
            error,
        })
        .boxed())
}

#[derive(Form)]
#[form_root(::swim_form)]
struct NodeInfo {
    #[form(name = "node_uri")]
    node_uri: String,
    created: Timestamp,
    agents: Vec<String>,
}

#[derive(Form)]
#[form_root(::swim_form)]
struct NodeInfoCount {
    #[form(name = "node_uri")]
    node_uri: String,
    created: usize,
    #[form(name = "childCount")]
    child_count: usize,
}

enum NodeInfoKind {
    List(NodeInfo),
    Count(NodeInfoCount),
}

async fn run_task(
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
    context: Box<dyn AgentContext + Send>,
    nodes_io: Io,
    nodes_count_io: Io,
) -> Result<(), FrameIoError> {
    let (nodes_tx, nodes_rx) = nodes_io;
    let (nodes_count_tx, nodes_count_rx) = nodes_count_io;

    let nodes_input = FramedRead::new(
        nodes_rx,
        LaneRequestDecoder::new(WithLengthBytesCodec::default()),
    );
    let mut nodes_output = FramedWrite::new(
        nodes_tx,
        LaneResponseEncoder::new(MapOperationEncoder::default()),
    );

    let nodes_count_input = FramedRead::new(
        nodes_count_rx,
        LaneRequestDecoder::new(WithLengthBytesCodec::default()),
    );
    let mut nodes_count_output = FramedWrite::new(
        nodes_count_tx,
        LaneResponseEncoder::new(MapOperationEncoder::default()),
    );

    let mut request_stream = select(
        nodes_input.map(Either::Left),
        nodes_count_input.map(Either::Right),
    );

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
                            .map(|(node_uri, meta)| NodeInfo {
                                node_uri,
                                created: meta.created,
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

                    let synced: LaneResponse<MapOperation<&str, &NodeInfo>> =
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
                                    let info = NodeInfo {
                                        node_uri: path.clone(),
                                        created: data.created.clone(),
                                        agents: vec![data.name.clone().into()],
                                    };
                                    (path, NodeInfoKind::List(info))
                                }
                                UriPart::Junction { path, descendants } => {
                                    let info = NodeInfoCount {
                                        node_uri: path.clone(),
                                        created: 0,
                                        child_count: descendants,
                                    };
                                    (path, NodeInfoKind::Count(info))
                                }
                            })
                            .collect::<Vec<_>>()
                    };

                    for (path, info) in parts {
                        match info {
                            NodeInfoKind::List(info) => {
                                let op = MapOperation::Update {
                                    key: path.as_str(),
                                    value: &info,
                                };
                                nodes_output.send(LaneResponse::SyncEvent(id, op)).await?;

                                let synced: LaneResponse<MapOperation<&str, &NodeInfo>> =
                                    LaneResponse::Synced(id);
                                nodes_output.send(synced).await?;
                            }
                            NodeInfoKind::Count(info) => {
                                let op = MapOperation::Update {
                                    key: path.as_str(),
                                    value: &info,
                                };
                                nodes_count_output
                                    .send(LaneResponse::SyncEvent(id, op))
                                    .await?;

                                let synced: LaneResponse<MapOperation<&str, &NodeInfoCount>> =
                                    LaneResponse::Synced(id);
                                nodes_count_output.send(synced).await?;
                            }
                        }
                    }
                }
            }
        }
    }

    // deferred drop so the agent doesn't terminate early.
    let _context = context;

    Ok(())
}
