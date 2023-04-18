use crate::task::AgentMeta;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use swim_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult};
use swim_api::error::AgentTaskError;
use swim_api::meta::lane::LaneKind;
use swim_api::protocol::agent::{LaneRequestDecoder, LaneResponseEncoder};
use swim_api::protocol::map::MapOperationEncoder;
use swim_api::protocol::WithLengthBytesCodec;
use swim_runtime::downlink::Io;
use swim_utilities::routing::route_uri::RouteUri;
use swim_utilities::uri_forest::UriForest;
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

async fn run_init(
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
    config: AgentConfig,
    context: Box<dyn AgentContext + Send>,
) -> AgentInitResult {
    let mut lane_config = config.default_lane_config.unwrap_or_default();
    lane_config.transient = true;
    let mesh_io = context
        .add_lane(NODES_LANE, LaneKind::DemandMap, lane_config)
        .await?;
    Ok(run_task(agents, context, mesh_io).boxed())
}

async fn run_task(
    agents: Arc<RwLock<UriForest<AgentMeta>>>,
    context: Box<dyn AgentContext + Send>,
    mesh_io: Io,
) -> Result<(), AgentTaskError> {
    // deferred drop so the agent doesn't terminate early.
    let _context = context;

    let (tx, rx) = mesh_io;

    let mut input = FramedRead::new(rx, LaneRequestDecoder::new(WithLengthBytesCodec::default()))
        .take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, LaneResponseEncoder::new(MapOperationEncoder::default()));
}
