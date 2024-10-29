use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use bytes::BytesMut;
use futures::{stream::FuturesUnordered, StreamExt};
use swimos_agent::{
    agent_model::{
        downlink::BoxDownlinkChannelFactory, AgentSpec, ItemDescriptor, ItemFlags, WarpLaneKind,
    },
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, EventHandler, HandlerFuture, LaneSpawnOnDone,
        LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
    AgentMetadata,
};
use swimos_api::{
    address::Address,
    agent::AgentConfig,
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

pub struct LaneRequest<A> {
    name: String,
    is_map: bool,
    on_done: LaneSpawnOnDone<A>,
}

impl<A> std::fmt::Debug for LaneRequest<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneRequest")
            .field("name", &self.name)
            .field("is_map", &self.is_map)
            .field("on_done", &"...")
            .finish()
    }
}

#[derive(Debug)]
pub struct TestSpawner<A> {
    allow_downlinks: bool,
    downlinks: RefCell<Vec<DownlinkRequest<A>>>,
    suspended: FuturesUnordered<HandlerFuture<A>>,
    lane_requests: RefCell<Vec<LaneRequest<A>>>,
}

impl<A> Default for TestSpawner<A> {
    fn default() -> Self {
        TestSpawner {
            allow_downlinks: false,
            downlinks: RefCell::new(vec![]),
            suspended: Default::default(),
            lane_requests: RefCell::new(vec![]),
        }
    }
}

impl<A> TestSpawner<A> {
    pub fn with_downlinks() -> TestSpawner<A> {
        TestSpawner {
            allow_downlinks: true,
            ..Default::default()
        }
    }

    pub fn take_downlinks(&self) -> Vec<DownlinkRequest<A>> {
        let mut guard = self.downlinks.borrow_mut();
        std::mem::take(&mut *guard)
    }
}

impl<A> Spawner<A> for TestSpawner<A> {
    fn spawn_suspend(&self, fut: HandlerFuture<A>) {
        self.suspended.push(fut);
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer");
    }
}

pub struct DownlinkRequest<A> {
    pub path: Address<String>,
    _make_channel: BoxDownlinkChannelFactory<A>,
    _on_done: DownlinkSpawnOnDone<A>,
}

impl<A> std::fmt::Debug for DownlinkRequest<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkRequest")
            .field("path", &self.path)
            .field("make_channel", &"...")
            .field("_on_done", &"...")
            .finish()
    }
}

impl<A> LinkSpawner<A> for TestSpawner<A> {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<A>,
        _on_done: DownlinkSpawnOnDone<A>,
    ) {
        if self.allow_downlinks {
            self.downlinks.borrow_mut().push(DownlinkRequest {
                path: path.into(),
                _make_channel,
                _on_done,
            })
        } else {
            panic!("Opening downlinks not supported.");
        }
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl<A> LaneSpawner<A> for TestSpawner<A> {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<A>,
    ) -> Result<(), DynamicRegistrationError> {
        let is_map = match kind {
            WarpLaneKind::Map => true,
            WarpLaneKind::Value => false,
            _ => panic!("Unexpected lane kind: {}", kind),
        };
        self.lane_requests.borrow_mut().push(LaneRequest {
            name: name.to_string(),
            is_map,
            on_done,
        });
        Ok(())
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

pub async fn run_handler_with_futures_dl<A, H: EventHandler<A>>(
    agent: &A,
    handler: H,
) -> (HashSet<u64>, Vec<DownlinkRequest<A>>)
where
    A: AgentSpec,
{
    let mut spawner = TestSpawner::with_downlinks();
    let mut command_buffer = BytesMut::new();
    let modified =
        run_handler_with_futures_inner(agent, handler, &mut spawner, &mut command_buffer).await;
    (modified, spawner.take_downlinks())
}

pub async fn run_handler_with_futures<A, H: EventHandler<A>>(agent: &A, handler: H) -> HashSet<u64>
where
    A: AgentSpec,
{
    run_handler_with_futures_inner(
        agent,
        handler,
        &mut TestSpawner::default(),
        &mut BytesMut::new(),
    )
    .await
}

pub async fn run_handler_with_futures_and_cmds<A, H: EventHandler<A>>(
    agent: &A,
    handler: H,
    command_buffer: &mut BytesMut,
) -> HashSet<u64>
where
    A: AgentSpec,
{
    run_handler_with_futures_inner(agent, handler, &mut TestSpawner::default(), command_buffer)
        .await
}

async fn run_handler_with_futures_inner<A, H: EventHandler<A>>(
    agent: &A,
    handler: H,
    spawner: &mut TestSpawner<A>,
    command_buffer: &mut BytesMut,
) -> HashSet<u64>
where
    A: AgentSpec,
{
    let mut modified = run_handler_with_commands(agent, spawner, handler, command_buffer);
    let mut handlers = vec![];
    let reg = move |req: LaneRequest<A>| {
        let LaneRequest {
            name,
            is_map,
            on_done,
        } = req;
        let kind = if is_map {
            WarpLaneKind::Map
        } else {
            WarpLaneKind::Value
        };
        let descriptor = ItemDescriptor::WarpLane {
            kind,
            flags: ItemFlags::TRANSIENT,
        };
        let result = agent.register_dynamic_item(&name, descriptor);
        on_done(result.map_err(Into::into))
    };
    for request in
        std::mem::take::<Vec<LaneRequest<A>>>(spawner.lane_requests.borrow_mut().as_mut())
    {
        handlers.push(reg(request));
    }

    while !(handlers.is_empty() && spawner.suspended.is_empty()) {
        let m = if let Some(h) = handlers.pop() {
            run_handler_with_commands(agent, spawner, h, command_buffer)
        } else {
            let h = spawner.suspended.next().await.expect("No handler.");
            run_handler_with_commands(agent, spawner, h, command_buffer)
        };
        modified.extend(m);
        for request in
            std::mem::take::<Vec<LaneRequest<A>>>(spawner.lane_requests.borrow_mut().as_mut())
        {
            handlers.push(reg(request));
        }
    }
    modified
}

pub fn run_handler<A, H: EventHandler<A>>(
    agent: &A,
    spawner: &TestSpawner<A>,
    handler: H,
) -> HashSet<u64>
where
    A: AgentSpec,
{
    let mut command_buffer = BytesMut::new();
    run_handler_with_commands(agent, spawner, handler, &mut command_buffer)
}

pub fn run_handler_with_commands<A, H: EventHandler<A>>(
    agent: &A,
    spawner: &TestSpawner<A>,
    mut handler: H,
    command_buffer: &mut BytesMut,
) -> HashSet<u64>
where
    A: AgentSpec,
{
    let route_params = HashMap::new();
    let uri = make_uri();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        command_buffer,
    );

    let mut modified = HashSet::new();

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                if let Some(m) = modified_item {
                    modified.insert(m.id());
                }
            }
            StepResult::Fail(err) => panic!("Handler Failed: {}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(m) = modified_item {
                    modified.insert(m.id());
                }
                break modified;
            }
        }
    }
}
