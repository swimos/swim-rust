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
use crate::agent::meta::MetaKind;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::agent::{LaneIo, LaneTasks};
use crate::routing::LaneIdentifier;
use pin_utils::core_reexport::fmt::Formatter;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use swim_common::form::{Form, FormErr};
use swim_common::model::text::Text;
use swim_common::model::time::Timestamp;
use swim_common::model::{Attr, Item, Value};
use utilities::uri::RelativeUri;

pub const TRACE_URI: &str = "traceLog";
pub const DEBUG_URI: &str = "debugLog";
pub const INFO_URI: &str = "infoLog";
pub const WARN_URI: &str = "warnLog";
pub const ERROR_URI: &str = "errorLog";
pub const FAIL_URI: &str = "failLog";

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fail,
}

impl ToString for LogLevel {
    fn to_string(&self) -> String {
        let s = match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
            LogLevel::Fail => "fail",
        };

        s.to_string()
    }
}

impl PartialEq<LogLevel> for Text {
    fn eq(&self, other: &LogLevel) -> bool {
        match (other, self.as_str().to_lowercase().as_str()) {
            (LogLevel::Trace, "trace") => true,
            (LogLevel::Debug, "debug") => true,
            (LogLevel::Info, "info") => true,
            (LogLevel::Warn, "warn") => true,
            (LogLevel::Error, "error") => true,
            (LogLevel::Fail, "fail") => true,
            _ => false,
        }
    }
}

// todo: manual form implementation. Log level is the tag
#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    time: Timestamp,
    message: Value,
    level: LogLevel,
    node: RelativeUri,
    lane: RelativeUri,
}

impl Form for LogEntry {
    fn as_value(&self) -> Value {
        Value::Record(
            vec![Attr::of((
                self.level.to_string(),
                Value::from_vec(vec![
                    Item::of(("time", self.time.as_value())),
                    Item::of(("node", self.node.to_string())),
                    Item::of(("lane", self.lane.to_string())),
                ]),
            ))],
            vec![Item::ValueItem(self.message.as_value())],
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) => {
                let mut attrs_iter = attrs.iter();

                let (level, header_body) = match attrs_iter.next() {
                    Some(Attr { name, value }) if *name == LogLevel::Trace => {
                        (LogLevel::Trace, value)
                    }
                    Some(Attr { name, value }) if *name == LogLevel::Debug => {
                        (LogLevel::Debug, value)
                    }
                    Some(Attr { name, value }) if *name == LogLevel::Info => {
                        (LogLevel::Info, value)
                    }
                    Some(Attr { name, value }) if *name == LogLevel::Warn => {
                        (LogLevel::Warn, value)
                    }
                    Some(Attr { name, value }) if *name == LogLevel::Error => {
                        (LogLevel::Error, value)
                    }
                    Some(Attr { name, value }) if *name == LogLevel::Fail => {
                        (LogLevel::Fail, value)
                    }
                    _ => return Err(FormErr::Malformatted),
                };

                match header_body {
                    Value::Record(_attrs, header_body_items) => {
                        let mut time_opt = None;
                        let mut lane_opt = None;
                        let mut node_opt = None;

                        let mut items_iter = header_body_items.iter();

                        while let Some(value) = items_iter.next() {
                            match value {
                                Item::Slot(Value::Text(key), value) if key == "time" => {
                                    time_opt = Some(Timestamp::try_from_value(value)?);
                                }
                                Item::Slot(Value::Text(key), Value::Text(value))
                                    if key == "lane" =>
                                {
                                    let uri = RelativeUri::from_str(value.as_str())
                                        .map_err(|_| FormErr::Malformatted)?;
                                    lane_opt = Some(uri);
                                }
                                Item::Slot(Value::Text(key), Value::Text(value))
                                    if key == "node" =>
                                {
                                    let uri = RelativeUri::from_str(value.as_str())
                                        .map_err(|_| FormErr::Malformatted)?;
                                    node_opt = Some(uri);
                                }
                                _ => return Err(FormErr::Malformatted),
                            }
                        }

                        let mut items_iter = items.iter();
                        let message_opt;

                        match items_iter.next() {
                            Some(Item::ValueItem(value)) => {
                                message_opt = Some(value.clone());
                            }
                            _ => return Err(FormErr::Malformatted),
                        }

                        Ok(LogEntry {
                            time: time_opt
                                .ok_or(FormErr::Message("Missing field 'time'".to_string()))?,
                            message: message_opt
                                .ok_or(FormErr::Message("Missing log entry body'".to_string()))?,
                            level,
                            node: node_opt
                                .ok_or(FormErr::Message("Missing field 'node'".to_string()))?,
                            lane: lane_opt
                                .ok_or(FormErr::Message("Missing field 'lane'".to_string()))?,
                        })
                    }
                    _ => return Err(FormErr::Malformatted),
                }
            }
            v => Err(FormErr::incorrect_type("Value::Record", v)),
        }
    }
}

impl LogEntry {
    #[cfg(test)]
    fn with_timestamp<F>(
        time: Timestamp,
        message: F,
        level: LogLevel,
        lane: RelativeUri,
        node: RelativeUri,
    ) -> LogEntry
    where
        F: Form,
    {
        LogEntry {
            time,
            message: message.into_value(),
            level,
            lane,
            node,
        }
    }

    fn make<F>(message: F, level: LogLevel, lane: RelativeUri, node: RelativeUri) -> LogEntry
    where
        F: Form,
    {
        LogEntry {
            time: Timestamp::now(),
            message: message.into_value(),
            level,
            lane,
            node,
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
    pub fn log<E: Form>(&self, entry: E, level: LogLevel, node: RelativeUri) {
        let entry = LogEntry::make(entry, level, self.uri.clone(), node);

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

pub fn open_log_lanes<Config, Agent, Context>(
    uri: RelativeUri,
    exec_conf: &AgentExecutionConfig,
    meta_kind: MetaKind,
) -> (
    LogHandler,
    DynamicLaneTasks<Agent, Context>,
    HashMap<LaneIdentifier, Option<impl LaneIo<Context>>>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let mut lane_tasks = Vec::with_capacity(6);
    let mut lane_ios = HashMap::with_capacity(6);
    let mut make_lane = |uri| {
        let (lane, task, io) =
            make_supply_lane::<Agent, Context, LogEntry>(uri, true, exec_conf.lane_buffer);
        lane_tasks.push(task.boxed());
        lane_ios.insert(LaneIdentifier::Meta(meta_kind, uri.to_string()), io);

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
