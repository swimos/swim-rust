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

#[cfg(test)]
mod tests;

pub mod config;

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::agent::{Eff, LaneIo};
use crate::meta::log::config::{FlushStrategy, LogConfig};
use crate::meta::{IdentifiedAgentIo, MetaNodeAddressed};
use either::Either;
use futures::select_biased;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroUsize;
use swim_common::form::{Form, Tag};
use swim_common::model::time::Timestamp;
use swim_common::model::Value;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, span, Level};
use tracing_futures::{Instrument, Instrumented};

use swim_runtime::time::interval::interval;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

pub const TRACE_URI: &str = "traceLog";
pub const DEBUG_URI: &str = "debugLog";
pub const INFO_URI: &str = "infoLog";
pub const WARN_URI: &str = "warnLog";
pub const ERROR_URI: &str = "errorLog";
pub const FAIL_URI: &str = "failLog";

const LOG_TASK: &str = "Node logger";
const LOG_FAIL: &str = "Failed to send log message";

/// A corresponding level associated with a `LogEntry`.
#[derive(Copy, Clone, Debug, Tag, Eq, PartialEq, PartialOrd, Hash)]
pub enum LogLevel {
    /// Fine-grained informational events.
    Trace,
    /// Information that is useful in debugging an application.
    Debug,
    /// Information that denotes the progress of an application.
    Info,
    /// Potentially harmful events to the application.
    Warn,
    /// Log entries that have originated from an error in the application.
    Error,
    /// Events that may lead to the application to exit.
    Fail,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "Trace"),
            LogLevel::Debug => write!(f, "Debug"),
            LogLevel::Info => write!(f, "Info"),
            LogLevel::Warn => write!(f, "Warn"),
            LogLevel::Error => write!(f, "Error"),
            LogLevel::Fail => write!(f, "Fail"),
        }
    }
}

impl LogLevel {
    pub fn enumerated() -> &'static [LogLevel] {
        &[
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
            LogLevel::Fail,
        ]
    }

    /// Returns the lane URI associated with this level;
    pub(crate) fn uri_ref(&self) -> &'static str {
        match self {
            LogLevel::Trace => TRACE_URI,
            LogLevel::Debug => DEBUG_URI,
            LogLevel::Info => INFO_URI,
            LogLevel::Warn => WARN_URI,
            LogLevel::Error => ERROR_URI,
            LogLevel::Fail => FAIL_URI,
        }
    }
}

#[derive(PartialOrd, PartialEq, Debug, Clone)]
pub struct InvalidUri(pub String);

/// Try to parse a `LogLevel` from a URI str.
impl TryFrom<&str> for LogLevel {
    type Error = InvalidUri;

    fn try_from(uri: &str) -> Result<Self, <LogLevel as TryFrom<&str>>::Error> {
        for level in LogLevel::enumerated() {
            if uri == level.uri_ref() {
                return Ok(*level);
            }
        }

        Err(InvalidUri(format!("Unknown log level URI: {}", uri)))
    }
}

/// A log entry that may be supplied to a log lane.
#[derive(Clone, Debug, Form)]
pub struct LogEntry {
    /// Timestamp of when this entry was created.
    time: Timestamp,
    /// The body of the entry.
    message: Value,
    /// The coarseness of this entry.
    #[form(tag)]
    level: LogLevel,
    /// The node URI that produced this entry.
    node: RelativeUri,
    /// The lane URI that produced this entry.
    lane: String,
}

impl LogEntry {
    pub fn make<F>(
        message: F,
        level: LogLevel,
        node: RelativeUri,
        lane: impl Into<String>,
    ) -> LogEntry
    where
        F: Form,
    {
        LogEntry {
            time: Timestamp::now(),
            message: message.into_value(),
            level,
            node,
            lane: lane.into(),
        }
    }
}

/// A handle to all of the log lanes for a node. I.e, for the agent itself and not its uplinks or
/// lanes.
///
/// Internally, this is backed by a buffer which will either send the entries as they're provided,
/// or wait until the buffer is full before sending any entries.
#[derive(Clone)]
pub struct NodeLogger {
    /// Channel to the internal send task.
    sender: mpsc::Sender<LogEntry>,
    /// The node URI that this logger is attched to.
    node_uri: RelativeUri,
}

impl Debug for NodeLogger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeLogger")
            .field("node_uri", &self.node_uri)
            .finish()
    }
}

struct LogLanes {
    /// Lane for fine-grained informational events.
    trace_lane: SupplyLane<LogEntry>,
    /// Lane for information that is useful in debugging an application.
    debug_lane: SupplyLane<LogEntry>,
    /// Lane for information that denotes the progress of an application.
    info_lane: SupplyLane<LogEntry>,
    /// Lane for potentially harmful events to the application.
    warn_lane: SupplyLane<LogEntry>,
    /// Lane for log entries that have originated from an error in the application.
    error_lane: SupplyLane<LogEntry>,
    /// Lane for events that may lead to the application to exit.
    fail_lane: SupplyLane<LogEntry>,
}

impl LogLanes {
    /// Send `entry` to the corresponding supply lane for its log level. This will only error if the
    /// channel to the lane is closed.
    async fn send(&self, entry: LogEntry) -> Result<(), SendError<LogEntry>> {
        let LogLanes {
            trace_lane,
            debug_lane,
            info_lane,
            warn_lane,
            error_lane,
            fail_lane,
        } = self;

        match &entry.level {
            LogLevel::Trace => trace_lane.send(entry).await,
            LogLevel::Debug => debug_lane.send(entry).await,
            LogLevel::Info => info_lane.send(entry).await,
            LogLevel::Warn => warn_lane.send(entry).await,
            LogLevel::Error => error_lane.send(entry).await,
            LogLevel::Fail => fail_lane.send(entry).await,
        }
    }
}

impl NodeLogger {
    fn new(
        buffer_size: NonZeroUsize,
        node_uri: RelativeUri,
        yield_after: NonZeroUsize,
        max_pending: NonZeroUsize,
        stop_rx: trigger::Receiver,
        log_lanes: LogLanes,
        flush_strategy: FlushStrategy,
        flush_interval: Duration,
    ) -> (NodeLogger, impl Future<Output = ()>) {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = LogTask {
            rx,
            flush_interval,
            buffer: flush_strategy.into(),
            log_lanes,
        };

        let node_logger = NodeLogger {
            sender: tx,
            node_uri,
        };

        (node_logger, task.run(yield_after, stop_rx, max_pending))
    }

    /// Log `entry` at `level` and send it to the corresponding lane for `level`.
    ///
    /// See `LogHandler` for message delivery guarantees.
    pub async fn log<F: Form>(
        &self,
        entry: F,
        lane_uri: String,
        level: LogLevel,
    ) -> Result<(), SendError<LogEntry>> {
        let NodeLogger { sender, node_uri } = self;
        let entry = LogEntry::make(entry, level, node_uri.clone(), lane_uri);

        sender.send(entry).await
    }
}

/// An internal task for a `NodeLogger` which forwards all messages from its receive stream to the
/// corresponding supply lane for the log level.
struct LogTask {
    /// The channel to listen to log entries from.
    rx: mpsc::Receiver<LogEntry>,
    /// The interval at which to flush any pending messages from the log buffer.
    flush_interval: Duration,
    /// An internal buffer that may batch messages before sending them to the corresponding supply
    /// lane.
    buffer: LogBuffer,

    log_lanes: LogLanes,
}

/// An optional internal buffer for log entries.
enum LogBuffer {
    /// No entries are buffered and any entries pushed into the buffer will be returned immediately.
    None,
    /// A buffer with a fixed capacity. Once this limit has been reached all buffered entries will
    /// be returned.
    Capped(Vec<LogEntry>),
}

impl LogBuffer {
    /// Push an entry into the buffer.
    ///
    /// Returns any entries once the limit has been reached.
    fn push(&mut self, entry: LogEntry) -> Option<Vec<LogEntry>> {
        match self {
            LogBuffer::None => Some(vec![entry]),
            LogBuffer::Capped(buffer) => {
                if buffer.len() + 1 == buffer.capacity() {
                    let mut drained = buffer.drain(0..).collect::<Vec<_>>();
                    drained.push(entry);
                    Some(drained)
                } else {
                    buffer.push(entry);
                    None
                }
            }
        }
    }

    /// Drain the buffer.
    fn drain(&mut self) -> Vec<LogEntry> {
        match self {
            LogBuffer::None => Vec::new(),
            LogBuffer::Capped(buffer) => buffer.drain(0..).collect(),
        }
    }
}

impl From<FlushStrategy> for LogBuffer {
    fn from(strategy: FlushStrategy) -> Self {
        match strategy {
            FlushStrategy::Immediate => LogBuffer::None,
            FlushStrategy::Buffer(capacity) => {
                LogBuffer::Capped(Vec::with_capacity(capacity.get()))
            }
        }
    }
}

impl LogTask {
    async fn run(
        self,
        yield_after: NonZeroUsize,
        stop_rx: trigger::Receiver,
        max_pending: NonZeroUsize,
    ) {
        let LogTask {
            rx,
            flush_interval,
            mut buffer,
            log_lanes,
        } = self;

        let mut stream = ReceiverStream::new(rx).take_until(stop_rx);
        let mut pending = FuturesUnordered::new();
        let mut timer = interval(flush_interval).fuse();

        let capacity = max_pending.get();
        let yield_mod = yield_after.get();
        let mut iteration_count: usize = 0;

        loop {
            let event: Option<Either<Result<(), SendError<LogEntry>>, LogEntry>> = select_biased! {
                _ = timer.next() => {
                    for entry in buffer.drain() {
                        while pending.len() >= capacity {
                            if let Some(result) = pending.next().await {
                                LogTask::on_result(result)
                            }
                        }

                        let fut = log_lanes.send(entry);
                        pending.push(fut);
                    }

                    continue;
                },
                entry = stream.next() => entry.map(Either::Right),
                result = pending.next() => {
                    match result {
                        Some(inner) => Some(Either::Left(inner)),
                        None => continue,
                    }
                },
            };

            match event {
                None => break,
                Some(event) => match event {
                    Either::Left(result) => LogTask::on_result(result),
                    Either::Right(entry) => {
                        if let Some(entries) = buffer.push(entry) {
                            for entry in entries {
                                while pending.len() >= capacity {
                                    if let Some(result) = pending.next().await {
                                        LogTask::on_result(result)
                                    }
                                }

                                let fut = log_lanes.send(entry);
                                pending.push(fut);
                            }
                        }
                    }
                },
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    fn on_result(result: Result<(), SendError<LogEntry>>) {
        if result.is_err() {
            event!(Level::WARN, LOG_FAIL);
        }
    }
}

#[cfg(test)]
pub(crate) fn make_node_logger(node_uri: RelativeUri) -> NodeLogger {
    NodeLogger {
        sender: mpsc::channel(1).0,
        node_uri,
    }
}

/// Opens log lanes for `node_uri` using the provided configuration.
pub fn open_log_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    config: LogConfig,
    stop_rx: trigger::Receiver,
    yield_after: NonZeroUsize,
    task_manager: &FuturesUnordered<Instrumented<Eff>>,
) -> (
    NodeLogger,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let LogConfig {
        flush_interval,
        channel_buffer_size,
        lane_buffer,
        flush_strategy,
        max_pending_messages,
    } = config;

    let lane_count = LogLevel::enumerated().len();
    let mut lane_tasks = Vec::with_capacity(lane_count);
    let mut lane_ios = HashMap::with_capacity(lane_count);

    let mut make_log_lane = |level: LogLevel| {
        let (lane, task, io) = make_supply_lane(level.uri_ref(), true, lane_buffer);

        lane_tasks.push(task.boxed());
        lane_ios.insert(
            LaneIdentifier::meta(MetaNodeAddressed::NodeLog(level)),
            io.expect("Public lane didn't return any lane IO").boxed(),
        );

        lane
    };

    let log_lanes = LogLanes {
        trace_lane: make_log_lane(LogLevel::Trace),
        debug_lane: make_log_lane(LogLevel::Debug),
        info_lane: make_log_lane(LogLevel::Info),
        warn_lane: make_log_lane(LogLevel::Warn),
        error_lane: make_log_lane(LogLevel::Error),
        fail_lane: make_log_lane(LogLevel::Fail),
    };

    let (node_logger, task) = NodeLogger::new(
        channel_buffer_size,
        node_uri.clone(),
        yield_after,
        max_pending_messages,
        stop_rx,
        log_lanes,
        flush_strategy,
        flush_interval,
    );

    let task = futures::FutureExt::boxed(task).instrument(span!(Level::DEBUG, LOG_TASK, ?node_uri));

    task_manager.push(task);

    (node_logger, lane_tasks, lane_ios)
}
