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

#[cfg(test)]
mod tests;

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::meta::IdentifiedAgentIo;
use crate::agent::LaneIo;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::routing::LaneIdentifier;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use swim_common::form::{Form, Tag};
use swim_common::model::time::Timestamp;
use swim_common::model::Value;
use utilities::uri::RelativeUri;

pub const TRACE_URI: &str = "traceLog";
pub const DEBUG_URI: &str = "debugLog";
pub const INFO_URI: &str = "infoLog";
pub const WARN_URI: &str = "warnLog";
pub const ERROR_URI: &str = "errorLog";
pub const FAIL_URI: &str = "failLog";

#[derive(Copy, Clone, Debug, Tag)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fail,
}

#[derive(Clone, Debug, Form)]
pub struct LogEntry {
    time: Timestamp,
    message: Value,
    #[form(tag)]
    level: LogLevel,
    lane: RelativeUri,
}

impl LogEntry {
    pub fn make<F>(message: F, level: LogLevel, lane: RelativeUri) -> LogEntry
    where
        F: Form,
    {
        LogEntry {
            time: Timestamp::now(),
            message: message.into_value(),
            level,
            lane,
        }
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogHandler")
            .field("uri", &self.uri)
            .finish()
    }
}

#[cfg(test)]
pub(crate) fn make_log_handler(uri: RelativeUri) -> LogHandler {
    use tokio::sync::mpsc;

    LogHandler {
        uri,
        trace_lane: SupplyLane::new(mpsc::channel(5).0),
        debug_lane: SupplyLane::new(mpsc::channel(5).0),
        info_lane: SupplyLane::new(mpsc::channel(5).0),
        warn_lane: SupplyLane::new(mpsc::channel(5).0),
        error_lane: SupplyLane::new(mpsc::channel(5).0),
        fail_lane: SupplyLane::new(mpsc::channel(5).0),
    }
}

impl LogHandler {
    pub fn log<E: Form>(&self, entry: E, level: LogLevel) {
        let entry = LogEntry::make(entry, level, self.uri.clone());

        let sender = match level {
            LogLevel::Trace => self.trace_lane.supplier(),
            LogLevel::Debug => self.debug_lane.supplier(),
            LogLevel::Info => self.info_lane.supplier(),
            LogLevel::Warn => self.warn_lane.supplier(),
            LogLevel::Error => self.error_lane.supplier(),
            LogLevel::Fail => self.fail_lane.supplier(),
        };

        let _ = sender.try_send(entry);
    }
}

pub fn open_log_lanes<Config, Agent, Context>(
    uri: RelativeUri,
    exec_conf: &AgentExecutionConfig,
) -> (
    LogHandler,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let mut lane_tasks = Vec::with_capacity(6);
    let mut lane_ios = HashMap::with_capacity(6);
    let mut make_log_lane = |lane_uri: String| {
        let (lane, task, io) = make_supply_lane(lane_uri.clone(), true, exec_conf.lane_buffer);
        lane_tasks.push(task.boxed());
        lane_ios.insert(
            LaneIdentifier::meta(lane_uri),
            io.expect("Public lane didn't return any lane IO").boxed(),
        );

        lane
    };

    let log_handler = LogHandler {
        uri,
        trace_lane: make_log_lane(TRACE_URI.to_string()),
        debug_lane: make_log_lane(DEBUG_URI.to_string()),
        info_lane: make_log_lane(INFO_URI.to_string()),
        warn_lane: make_log_lane(WARN_URI.to_string()),
        error_lane: make_log_lane(ERROR_URI.to_string()),
        fail_lane: make_log_lane(FAIL_URI.to_string()),
    };

    (log_handler, lane_tasks, lane_ios)
}
