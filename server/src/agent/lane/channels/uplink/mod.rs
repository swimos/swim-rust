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

use std::fmt::{Display, Formatter};
use std::error::Error;

pub mod value;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkAction {
    Link,
    Sync,
    Unlink,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkState {
    Opened,
    Linked,
    Synced,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UplinkMessage<Ev> {
    Linked,
    Synced,
    Unlinked,
    Event(Ev),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkError {
    SenderDropped,
    LaneStoppedReporting,
}

impl Display for UplinkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UplinkError::SenderDropped => write!(f, "Uplink send channel was dropped."),
            UplinkError::LaneStoppedReporting => write!(f, "The lane stopped reporting its state."),
        }
    }
}

impl Error for UplinkError {}

