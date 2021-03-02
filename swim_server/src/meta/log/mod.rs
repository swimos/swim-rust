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
use crate::agent::LaneIo;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::meta::log::config::{FlushStrategy, LogConfig};
use crate::meta::{IdentifiedAgentIo, MetaNodeAddressed};
use crossbeam_queue::SegQueue;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use swim_common::form::{Form, Tag};
use swim_common::model::time::Timestamp;
use swim_common::model::Value;
use tracing::{event, Level};
use utilities::uri::RelativeUri;

pub const TRACE_URI: &str = "traceLog";
pub const DEBUG_URI: &str = "debugLog";
pub const INFO_URI: &str = "infoLog";
pub const WARN_URI: &str = "warnLog";
pub const ERROR_URI: &str = "errorLog";
pub const FAIL_URI: &str = "failLog";

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
/// Log lanes make no guarantees as to whether the messages that are sent to them will actually be
/// delivered. This is so that backpressure is not applied to the supplier of a log entry. If
/// throughput to a log lane was high enough and the lane was asynchronous or blocking, this would
/// create backpressure to the supplier.
///
/// Internally, this is backed by a buffer which will either send the entries as they're provided,
/// or wait until the buffer is full before sending any entries.
#[derive(Clone)]
pub struct NodeLogger {
    /// Internal buffer for entries.
    buffer: Arc<LogBuffer>,
    /// The agent's URI.
    node_uri: RelativeUri,
    /// Lane for fine-grained informational events.
    trace_lane: Arc<SupplyLane<LogEntry>>,
    /// Lane for information that is useful in debugging an application.
    debug_lane: Arc<SupplyLane<LogEntry>>,
    /// Lane for information that denotes the progress of an application.
    info_lane: Arc<SupplyLane<LogEntry>>,
    /// Lane for potentially harmful events to the application.
    warn_lane: Arc<SupplyLane<LogEntry>>,
    /// Lane for log entries that have originated from an error in the application.
    error_lane: Arc<SupplyLane<LogEntry>>,
    /// Lane for events that may lead to the application to exit.
    fail_lane: Arc<SupplyLane<LogEntry>>,
}

impl Debug for NodeLogger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeLogger")
            .field("node_uri", &self.node_uri)
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl NodeLogger {
    fn new(
        buffer: Arc<LogBuffer>,
        node_uri: RelativeUri,
        trace_lane: Arc<SupplyLane<LogEntry>>,
        debug_lane: Arc<SupplyLane<LogEntry>>,
        info_lane: Arc<SupplyLane<LogEntry>>,
        warn_lane: Arc<SupplyLane<LogEntry>>,
        error_lane: Arc<SupplyLane<LogEntry>>,
        fail_lane: Arc<SupplyLane<LogEntry>>,
    ) -> Self {
        NodeLogger {
            buffer,
            node_uri,
            trace_lane,
            debug_lane,
            info_lane,
            warn_lane,
            error_lane,
            fail_lane,
        }
    }

    /// Log `entry` at `level` and send it to the corresponding lane for `level`.
    ///
    /// See `LogHandler` for message delivery guarantees.
    pub fn log<F: Form>(&self, entry: F, level: LogLevel) {
        let NodeLogger { node_uri, .. } = self;

        let entry = LogEntry::make(entry, level, node_uri.clone(), level.uri_ref());
        self.log_entry(entry);
    }

    /// Log `entry` to its corresponding log lane.
    pub fn log_entry(&self, entry: LogEntry) {
        self.buffer.push(entry);
        self.flush();
    }

    /// Flushes any available log entries in the buffer.
    fn flush(&self) {
        if let Some(entries) = self.buffer.next() {
            for entry in entries {
                let result = match &entry.level {
                    LogLevel::Trace => self.trace_lane.try_send(entry),
                    LogLevel::Debug => self.debug_lane.try_send(entry),
                    LogLevel::Info => self.info_lane.try_send(entry),
                    LogLevel::Warn => self.warn_lane.try_send(entry),
                    LogLevel::Error => self.error_lane.try_send(entry),
                    LogLevel::Fail => self.fail_lane.try_send(entry),
                };

                if result.is_err() {
                    let uri = &self.node_uri;
                    event!(Level::WARN, %uri, LOG_FAIL);
                }
            }
        }
    }
}

/// A configurable buffer for log entries. Entries will be available once the buffer's size is equal
/// to or greater than `cap`.  
#[derive(Debug)]
struct LogBuffer {
    buffer: SegQueue<LogEntry>,
    cap: usize,
}

impl LogBuffer {
    /// Create a new buffer that will produce values according to `strategy`.
    fn new(strategy: FlushStrategy) -> Self {
        let cap = match strategy {
            FlushStrategy::Immediate => 1,
            FlushStrategy::Buffer(n) => n.get(),
        };

        LogBuffer {
            buffer: SegQueue::new(),
            cap,
        }
    }

    /// Push a value into the buffer.
    fn push(&self, entry: LogEntry) {
        self.buffer.push(entry);
    }

    /// Takes any entries that are available in the buffer. If the buffer's capacity is greater than
    /// or equal to the configured capacity then the buffer is drained.
    fn next(&self) -> Option<Vec<LogEntry>> {
        let LogBuffer { buffer, cap } = self;

        if buffer.len() >= *cap {
            let len = buffer.len();

            let end = if *cap == 1 || len >= *cap {
                len
            } else {
                return None;
            };

            let mut drained = 0;
            let mut results = Vec::new();

            while drained < end {
                match buffer.pop() {
                    Some(entry) => {
                        drained += 1;
                        results.push(entry);
                    }
                    None => break,
                }
            }

            if results.is_empty() {
                None
            } else {
                Some(results)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
pub(crate) fn make_node_logger(node_uri: RelativeUri) -> NodeLogger {
    use tokio::sync::mpsc;

    NodeLogger {
        buffer: Arc::new(LogBuffer::new(FlushStrategy::Immediate)),
        node_uri,
        trace_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
        debug_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
        info_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
        warn_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
        error_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
        fail_lane: Arc::new(SupplyLane::new(Box::new(mpsc::channel(1).0))),
    }
}

/// Opens log lanes for `node_uri` using the provided configuration.
pub fn open_log_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    config: LogConfig,
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
        send_strategy,
        flush_strategy,
    } = config;

    let lane_count = LogLevel::enumerated().len();
    let mut lane_tasks = Vec::with_capacity(lane_count);
    let mut lane_ios = HashMap::with_capacity(lane_count);

    let mut make_log_lane = |level: LogLevel| {
        let (lane, task, io) = make_supply_lane(level.uri_ref(), true, send_strategy);

        lane_tasks.push(task.boxed());
        lane_ios.insert(
            LaneIdentifier::meta(MetaNodeAddressed::NodeLog(level)),
            io.expect("Public lane didn't return any lane IO").boxed(),
        );

        Arc::new(lane)
    };

    let node_logger = NodeLogger {
        buffer: Arc::new(LogBuffer::new(flush_strategy)),
        node_uri,
        trace_lane: make_log_lane(LogLevel::Trace),
        debug_lane: make_log_lane(LogLevel::Debug),
        info_lane: make_log_lane(LogLevel::Info),
        warn_lane: make_log_lane(LogLevel::Warn),
        error_lane: make_log_lane(LogLevel::Error),
        fail_lane: make_log_lane(LogLevel::Fail),
    };

    (node_logger, lane_tasks, lane_ios)
}
