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

use swim_common::form::Form;
use swim_common::model::time::Timestamp;
use utilities::uri::RelativeUri;

pub const META_EDGE: &str = "swim:meta:edge";
pub const META_MESH: &str = "swim:meta:mesh";
pub const META_PART: &str = "swim:meta:part";
pub const META_HOST: &str = "swim:meta:host";
pub const META_NODE: &str = "swim:meta:node";
pub const META_LANE: &str = "swim:meta:lane";

#[derive(Copy, Clone, Debug)]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fail,
}

// todo: manual form implementation. Log level is the tag
#[derive(Copy, Clone)]
pub struct LogEntry<F>
where
    F: Form,
{
    time: Timestamp,
    message: F,
    level: LogLevel,
}

impl<F> LogEntry<F>
where
    F: Form,
{
    fn make(message: F, level: LogLevel) -> LogEntry<F> {
        LogEntry {
            time: Timestamp::now(),
            message,
            level,
        }
    }

    pub fn trace(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Trace)
    }

    pub fn debug(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Debug)
    }

    pub fn info(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Info)
    }

    pub fn warn(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Warn)
    }

    pub fn error(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Error)
    }

    pub fn fail(message: F) -> LogEntry<F> {
        LogEntry::make(message, LogLevel::Fail)
    }
}

#[derive(Clone, Debug)]
pub struct LogHandler {
    uri: RelativeUri,
}

impl LogHandler {
    pub fn new(uri: RelativeUri) -> LogHandler {
        LogHandler { uri }
    }

    pub fn log<E: Form>(&self, _entry: E) {
        unimplemented!()
    }
}
