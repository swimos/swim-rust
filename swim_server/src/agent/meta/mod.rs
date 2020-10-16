// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::agent::{LaneIo, LaneTasks};
use pin_utils::core_reexport::fmt::Formatter;
use std::collections::HashMap;
use std::fmt::Debug;
use swim_common::form::{Form, FormErr};
use swim_common::model::time::Timestamp;
use swim_common::model::Value;
use utilities::uri::RelativeUri;

pub const META_EDGE: &str = "swim:meta:edge";
pub const META_MESH: &str = "swim:meta:mesh";
pub const META_PART: &str = "swim:meta:part";
pub const META_HOST: &str = "swim:meta:host";
pub const META_NODE: &str = "swim:meta:node";
pub const META_LANE: &str = "swim:meta:lane";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaKind {
    Edge,
    Mesh,
    Part,
    Host,
    Node,
    Lane,
}

#[derive(Copy, Clone, Debug)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fail,
}

// todo: manual form implementation. Log level is the tag
#[derive(Clone, Debug)]
pub struct LogEntry {
    time: Timestamp,
    message: Value,
    level: LogLevel,
}

impl Form for LogEntry {
    fn as_value(&self) -> Value {
        unimplemented!()
    }

    fn try_from_value(_value: &Value) -> Result<Self, FormErr> {
        unimplemented!()
    }
}

impl LogEntry {
    fn make<F>(message: F, level: LogLevel) -> LogEntry
    where
        F: Form,
    {
        LogEntry {
            time: Timestamp::now(),
            message: message.into_value(),
            level,
        }
    }

    pub fn trace<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Trace)
    }

    pub fn debug<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Debug)
    }

    pub fn info<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Info)
    }

    pub fn warn<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Warn)
    }

    pub fn error<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Error)
    }

    pub fn fail<F>(message: F) -> LogEntry
    where
        F: Form,
    {
        LogEntry::make(message, LogLevel::Fail)
    }
}

#[derive(Clone)]
pub struct LogHandler {
    uri: RelativeUri,
    trace_lane: SupplyLane<LogEntry>,
    debug_lane: SupplyLane<LogEntry>,
    info_lane: SupplyLane<LogEntry>,
    warn_lane: SupplyLane<LogEntry>,
    error_lane: SupplyLane<LogEntry>,
    fail_lane: SupplyLane<LogEntry>,
}

impl Debug for LogHandler {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

#[cfg(test)]
pub(crate) fn make_log_handler(uri: RelativeUri) -> LogHandler {
    use tokio::sync::mpsc;

    LogHandler {
        uri,
        trace_lane: SupplyLane::new(mpsc::channel(1).0),
        debug_lane: SupplyLane::new(mpsc::channel(1).0),
        info_lane: SupplyLane::new(mpsc::channel(1).0),
        warn_lane: SupplyLane::new(mpsc::channel(1).0),
        error_lane: SupplyLane::new(mpsc::channel(1).0),
        fail_lane: SupplyLane::new(mpsc::channel(1).0),
    }
}

impl LogHandler {
    pub fn log<E: Form>(&self, entry: E, level: LogLevel) {
        let entry = LogEntry::make(entry, level);

        match level {
            LogLevel::Trace => {
                let mut supplier = self.trace_lane.supplier();
                let _ = supplier.try_send(entry);
            }
            LogLevel::Debug => {
                let mut supplier = self.debug_lane.supplier();
                let _ = supplier.try_send(entry);
            }
            LogLevel::Info => {
                let mut supplier = self.info_lane.supplier();
                let _ = supplier.try_send(entry);
            }
            LogLevel::Warn => {
                let mut supplier = self.warn_lane.supplier();
                let _ = supplier.try_send(entry);
            }
            LogLevel::Error => {
                let mut supplier = self.error_lane.supplier();
                let _ = supplier.try_send(entry);
            }
            LogLevel::Fail => {
                let mut supplier = self.fail_lane.supplier();
                let _ = supplier.try_send(entry);
            }
        }
    }
}

const TRACE_URI: &str = "traceLog";
const DEBUG_URI: &str = "debugLog";
const INFO_URI: &str = "infoLog";
const WARN_URI: &str = "warnLog";
const ERROR_URI: &str = "errorLog";
const FAIL_URI: &str = "failLog";

pub fn open_log_lanes<Config, Agent, Context>(
    uri: RelativeUri,
    exec_conf: &AgentExecutionConfig,
) -> (
    LogHandler,
    DynamicLaneTasks<Agent, Context>,
    HashMap<String, Option<impl LaneIo<Context>>>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let mut lane_tasks = Vec::with_capacity(5);
    let mut lane_ios = HashMap::with_capacity(6);

    let mut make_lane = |uri| {
        let (lane, task, io) =
            make_supply_lane::<Agent, Context, LogEntry>(uri, true, exec_conf.lane_buffer);
        lane_tasks.push(task.boxed());
        lane_ios.insert(uri.to_string(), io);

        lane
    };

    let handler = LogHandler {
        uri,
        trace_lane: make_lane(TRACE_URI),
        debug_lane: make_lane(DEBUG_URI),
        info_lane: make_lane(INFO_URI),
        warn_lane: make_lane(WARN_URI),
        error_lane: make_lane(ERROR_URI),
        fail_lane: make_lane(FAIL_URI),
    };

    (handler, lane_tasks, lane_ios)
}
