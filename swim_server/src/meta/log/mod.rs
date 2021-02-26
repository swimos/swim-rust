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

use std::convert::TryFrom;
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

#[derive(Copy, Clone, Debug, Tag, Eq, PartialEq, Hash)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fail,
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

    pub fn uri_ref(&self) -> &'static str {
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
pub struct InvalidLogUri(pub String);

/// Try and parse a `LogLevel` from a URI str.
impl TryFrom<&str> for LogLevel {
    type Error = InvalidLogUri;

    fn try_from(uri: &str) -> Result<Self, <LogLevel as TryFrom<&str>>::Error> {
        for level in LogLevel::enumerated() {
            if uri == level.uri_ref() {
                return Ok(*level);
            }
        }

        Err(InvalidLogUri(format!("Unknown log level URI: {}", uri)))
    }
}

#[derive(Clone, Debug, Form)]
pub struct LogEntry {
    time: Timestamp,
    message: Value,
    #[form(tag)]
    level: LogLevel,
    lane: RelativeUri,
}
