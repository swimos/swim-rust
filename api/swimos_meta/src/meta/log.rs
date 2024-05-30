// Copyright 2015-2023 Swim Inc.
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

use std::hash::Hash;
use swimos_form::Form;
use swimos_form::Tag;
use swimos_model::time::Timestamp;
use swimos_model::{Text, Value};

/// The significance level associated with a [`LogEntry`].
#[derive(Tag, Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
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

/// A single log entry.
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
    node: Text,
    /// The lane URI that produced this entry.
    lane: Text,
}
