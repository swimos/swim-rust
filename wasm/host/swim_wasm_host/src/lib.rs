use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::BytesMut;
use futures::StreamExt;
use futures::{pin_mut, Stream};
use futures_util::future::BoxFuture;
use futures_util::stream::SelectAll;
use futures_util::{FutureExt, TryFutureExt};
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::Decoder;
use tokio_util::time::delay_queue::Key;
use tracing::{debug, trace};
use uuid::Uuid;

use interval_stream::{IntervalStream, ItemStatus, ScheduleDef, StreamItem};
use swim_api::agent::Agent;
use swim_api::agent::{AgentConfig, AgentContext, AgentInitResult};
use swim_api::error::{AgentInitError, AgentRuntimeError, AgentTaskError, FrameIoError};
use swim_api::protocol::agent::LaneRequest;
use swim_model::Text;
use swim_utilities::io::byte_channel::ByteWriter;
use swim_utilities::routing::route_uri::RouteUri;
use wasm_ir::requests::{
    CancelTaskRequest, CommandEvent, GuestLaneResponses, GuestRuntimeEvent, HostRequest,
    IdentifiedLaneResponse, IdentifiedLaneResponseDecoder, LaneSyncRequest, OpenLaneRequest,
    ScheduleTaskRequest,
};
use wasm_ir::AgentSpec;

use crate::codec::{DiscriminatedLaneRequest, LaneReader};
use crate::guest::{InitialisedWasmAgent, WasmGuestAgent};
use crate::lanes::{open_lane, OpenLaneError};
use crate::runtime::wasm::SharedMemory;
use crate::runtime::{WasmGuestRuntime, WasmGuestRuntimeFactory};

mod codec;
mod error;
mod guest;
mod lanes;
pub mod runtime;

pub mod wasm {
    pub use wasmtime::*;
}

pub enum WasmAgentInitError {}

pub struct WasmAgentState {
    channel: mpsc::Sender<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    // guest_notify: Arc<Notify>,
    shared_memory: SharedMemory,
}

impl WasmAgentState {
    pub fn new(
        channel: mpsc::Sender<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
        /*guest_notify: Arc<Notify>, */ shared_memory: SharedMemory,
    ) -> WasmAgentState {
        WasmAgentState {
            // store,
            // guest_notify,
            channel,
            shared_memory,
        }
    }
}

pub struct WasmAgentModel<R> {
    module_watch: watch::Receiver<R>,
}

impl<R> WasmAgentModel<R> {
    pub fn new(module_watch: watch::Receiver<R>) -> WasmAgentModel<R> {
        WasmAgentModel { module_watch }
    }
}

impl<R> Agent for WasmAgentModel<R>
where
    R: WasmGuestRuntimeFactory,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let WasmAgentModel { module_watch } = self;
        let task_watch = module_watch.clone();
        let inner = task_watch.borrow().clone();

        let (tx, rx) = mpsc::channel(8);
        let instance = inner.new_instance(tx);

        Box::pin(async move {
            let runtime = instance.await.unwrap();
            let InitialisedWasmAgent { spec, agent } = guest::initialise_agent(runtime).await?;

            initialize_agent(spec, route, route_params, config, context)
                .await
                .map(|task| task.run(agent, task_watch, rx).boxed())
        })
    }
}

pub struct GuestEnvironment {
    _route: RouteUri,
    _route_params: HashMap<String, String>,
    config: AgentConfig,
    lane_identifiers: HashMap<u64, Text>,
    lane_readers: SelectAll<LaneReader>,
    lane_writers: HashMap<u64, ByteWriter>,
}

async fn initialize_agent(
    spec: AgentSpec,
    route: RouteUri,
    route_params: HashMap<String, String>,
    config: AgentConfig,
    agent_context: Box<dyn AgentContext + Send>,
) -> Result<GuestAgentTask, AgentInitError> {
    let AgentSpec { lane_specs, .. } = spec;
    let mut guest_environment = GuestEnvironment {
        _route: route,
        _route_params: route_params,
        config,
        lane_identifiers: HashMap::new(),
        lane_readers: SelectAll::new(),
        lane_writers: HashMap::new(),
    };

    for (uri, spec) in lane_specs.into_iter() {
        match open_lane(spec, uri.as_str(), &agent_context, &mut guest_environment).await {
            Ok(()) => continue,
            Err(OpenLaneError::AgentRuntime(error)) => {
                return match error {
                    AgentRuntimeError::Stopping => Err(AgentInitError::FailedToStart),
                    AgentRuntimeError::Terminated => Err(AgentInitError::FailedToStart),
                };
            }
            Err(OpenLaneError::FrameIo(error)) => {
                return Err(AgentInitError::LaneInitializationFailure(error));
            }
        }
    }

    debug!("Agent initialised");

    Ok(GuestAgentTask {
        agent_context,
        guest_environment,
    })
}

#[derive(Clone, Debug)]
struct TaskDef {
    id: Uuid,
}

/// Scheduler for delaying tasks to be run in a guest runtime. Task ID's are pushed into this
/// scheduler with an associated schedule at which the IDs will be yielded at.
#[derive(Default)]
struct TaskScheduler {
    /// The queue of delayed tasks.
    tasks: IntervalStream<TaskDef>,
    /// Map of task IDs and their associated keys in the `IntervalStream's` `DelayQueue`. This is
    /// required as task keys may change each time the task is reinserted back in to the `IntervalStream`
    /// after it has been run.   
    keys: HashMap<Uuid, Key>,
}

impl TaskScheduler {
    /// Returns true if there are no tasks scheduled.
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Schedules a new task to be run using the provided schedule.
    ///
    /// # Arguments:
    /// - `id`: the ID of the task registered in the guest runtime.
    /// - `schedule`: the schedule to yield the tasks at.
    fn push_task(&mut self, id: Uuid, schedule: ScheduleDef) {
        let TaskScheduler { tasks, keys } = self;
        let key = tasks.push(schedule, TaskDef { id });
        keys.insert(id, key);
    }

    /// Cancels the task associated with `id`.
    fn cancel_task(&mut self, id: Uuid) {
        let TaskScheduler { tasks, keys } = self;
        match keys.get(&id) {
            Some(key) => {
                tasks.remove(key);
            }
            None => {
                panic!("Missing key for task ID: {}", id)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TaskState {
    id: Uuid,
    complete: bool,
}

impl Stream for TaskScheduler {
    type Item = TaskState;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let status = ready!(Pin::new(&mut self.tasks).poll_next(cx));
        match status {
            Some(item) => {
                let StreamItem { item, status } = item;
                let id = item.id;
                match status {
                    ItemStatus::Complete => {
                        self.keys.remove(&id);
                        Poll::Ready(Some(TaskState { id, complete: true }))
                    }
                    ItemStatus::WillYield { key } => {
                        self.keys.insert(id, key);
                        Poll::Ready(Some(TaskState {
                            id,
                            complete: false,
                        }))
                    }
                }
            }
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug)]
enum RuntimeEvent {
    Redeploy,
    Envelope {
        id: u64,
        request: DiscriminatedLaneRequest,
    },
    EnvelopeError {
        id: u64,
        error: FrameIoError,
    },
    ScheduledEvent {
        event: TaskState,
    },
}

struct GuestAgentTask {
    guest_environment: GuestEnvironment,
    agent_context: Box<dyn AgentContext + Send>,
}

impl GuestAgentTask {
    async fn run<R, F>(
        self,
        mut agent: WasmGuestAgent<R>,
        mut task_changes: watch::Receiver<F>,
        mut guest_channel: mpsc::Receiver<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    ) -> Result<(), AgentTaskError>
    where
        F: WasmGuestRuntimeFactory<AgentRuntime = R>,
        R: WasmGuestRuntime,
    {
        let GuestAgentTask {
            mut guest_environment,
            agent_context,
        } = self;
        let mut task_scheduler = TaskScheduler::default();

        loop {
            let event: Option<RuntimeEvent> = if task_scheduler.is_empty() {
                select! {
                    message = guest_environment.lane_readers.next() => message.map(|(id, request)| {
                        match request {
                            Ok(request) => RuntimeEvent::Envelope { id, request },
                            Err(error) => RuntimeEvent::EnvelopeError { id, error }
                        }
                    }),
                    runtime = task_changes.changed() => match runtime {
                        Ok(()) => Some(RuntimeEvent::Redeploy),
                        Err(_) => {
                            // runtime is stopping
                            break Ok(())
                        }
                    },
                }
            } else {
                select! {
                    message = guest_environment.lane_readers.next() => message.map(|(id, request)| {
                        match request {
                            Ok(request) => RuntimeEvent::Envelope { id, request },
                            Err(error) => RuntimeEvent::EnvelopeError { id, error }
                        }
                    }),
                    task = task_scheduler.next() => task.map(|event| RuntimeEvent::ScheduledEvent { event }),
                    runtime = task_changes.changed() => match runtime {
                        Ok(()) => Some(RuntimeEvent::Redeploy),
                        Err(_) => {
                            // runtime is stopping
                            break Ok(())
                        }
                    }
                }
            };
            match event {
                Some(RuntimeEvent::Redeploy) => {
                    // let factory = task_changes.borrow().clone();
                    // let model = factory.new_instance().await;
                    // todo: prune the database with lanes that have been removed from the agent
                    unimplemented!()
                }
                Some(RuntimeEvent::Envelope {
                    id,
                    request: DiscriminatedLaneRequest { map_like, request },
                }) => {
                    let event = match request {
                        LaneRequest::Command(data) => {
                            HostRequest::Command(CommandEvent { id, data, map_like })
                        }
                        LaneRequest::InitComplete => continue,
                        LaneRequest::Sync(remote) => {
                            HostRequest::Sync(LaneSyncRequest { id, remote })
                        }
                    };
                    if let ControlFlow::Break(Err(e)) = suspend(
                        &mut guest_channel,
                        &mut guest_environment,
                        &agent_context,
                        &mut task_scheduler,
                        agent
                            .dispatch(event)
                            .map_err(|e| AgentTaskError::UserCodeError(Box::new(e))),
                    )
                    .await
                    {
                        break Err(e);
                    }
                }
                Some(RuntimeEvent::EnvelopeError { .. }) => {
                    unimplemented!()
                }
                Some(RuntimeEvent::ScheduledEvent { .. }) => {
                    unimplemented!()
                }
                None => break Ok(()),
            }
        }
    }
}

enum SuspendedRuntimeEvent {
    Request {
        request: (GuestRuntimeEvent, oneshot::Sender<BytesMut>),
    },
    SuspendComplete {
        result: Result<(), AgentTaskError>,
    },
}

impl Debug for SuspendedRuntimeEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SuspendedRuntimeEvent::Request { request } => {
                write!(f, "SuspendedRuntimeEvent::Request({request:?})")
            }
            SuspendedRuntimeEvent::SuspendComplete { result: Ok(_) } => {
                write!(f, "SuspendedRuntimeEvent::SuspendComplete(Ok(_))")
            }
            SuspendedRuntimeEvent::SuspendComplete { result: Err(e) } => {
                write!(f, "SuspendedRuntimeEvent::SuspendComplete(Err({e:?}))")
            }
        }
    }
}

async fn suspend<'l, F>(
    guest_channel: &mut mpsc::Receiver<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    guest_environment: &'l mut GuestEnvironment,
    agent_context: &Box<dyn AgentContext + Send>,
    task_scheduler: &mut TaskScheduler,
    suspended_task: F,
) -> ControlFlow<Result<(), AgentTaskError>>
where
    F: Future<Output = Result<(), AgentTaskError>>,
{
    pin_mut!(suspended_task);

    let result = loop {
        let event: SuspendedRuntimeEvent = select! {
            biased;
            request = guest_channel.recv() => {
                match request {
                    Some(request) => {
                        SuspendedRuntimeEvent::Request {
                            request
                        }
                    },
                    None => {
                        // it's not possible for the sender to be dropped as we own it
                        unreachable!()
                    }
                }
            },
            suspend_result = (&mut suspended_task) => SuspendedRuntimeEvent::SuspendComplete{result:suspend_result},
        };

        debug!(event = ?event, "Suspended runtime event received");

        match event {
            SuspendedRuntimeEvent::Request {
                request: (event, promise),
            } => {
                trace!(request = ?event, "Agent runtime received a request");
                match event {
                    GuestRuntimeEvent::OpenLane(OpenLaneRequest { uri, spec }) => {
                        match open_lane(spec, uri.as_str(), agent_context, guest_environment).await
                        {
                            Ok(()) => {
                                promise
                                    .send(BytesMut::default())
                                    .expect("Wasm call dropped");
                            }
                            Err(OpenLaneError::AgentRuntime(e)) => {
                                promise
                                    .send(BytesMut::default())
                                    .expect("Wasm call dropped");
                                break ControlFlow::Break(Err(AgentTaskError::UserCodeError(
                                    Box::new(e),
                                )));
                            }
                            Err(OpenLaneError::FrameIo(error)) => {
                                promise
                                    .send(BytesMut::default())
                                    .expect("Wasm call dropped");
                                break ControlFlow::Break(Err(AgentTaskError::BadFrame {
                                    lane: uri,
                                    error,
                                }));
                            }
                        }
                    }
                    GuestRuntimeEvent::ScheduleTask(ScheduleTaskRequest { id, schedule }) => {
                        trace!(id = ?id, schedule = ?schedule, "Scheduling task");
                        task_scheduler.push_task(id, schedule);
                        promise
                            .send(BytesMut::default())
                            .expect("Wasm call dropped");
                    }
                    GuestRuntimeEvent::CancelTask(CancelTaskRequest { id }) => {
                        task_scheduler.cancel_task(id);
                        promise
                            .send(BytesMut::default())
                            .expect("Wasm call dropped");
                    }
                    GuestRuntimeEvent::DispatchLaneResponses(GuestLaneResponses {
                        mut responses,
                    }) => {
                        let mut decoder = IdentifiedLaneResponseDecoder;
                        loop {
                            match decoder.decode(&mut responses) {
                                Ok(Some(IdentifiedLaneResponse { id, response })) => {
                                    let writer = guest_environment
                                        .lane_writers
                                        .get_mut(&id)
                                        .expect("Missing lane writer");
                                    if writer.write_all(response.as_ref()).await.is_err() {
                                        // if the writer has closed then it indicates that the
                                        // runtime has shutdown and it is safe to sink the error.
                                        return ControlFlow::Break(Ok(()));
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    unreachable!()
                                }
                            }
                        }
                        promise
                            .send(BytesMut::default())
                            .expect("Wasm call dropped");
                    }
                }
            }
            SuspendedRuntimeEvent::SuspendComplete { result: Ok(()) } => {
                break ControlFlow::Continue(());
            }
            SuspendedRuntimeEvent::SuspendComplete { result: Err(e) } => {
                break ControlFlow::Break(Err(e));
            }
        }
    };

    debug!(result = ?result, "Agent runtime suspend complete");
    result
}
