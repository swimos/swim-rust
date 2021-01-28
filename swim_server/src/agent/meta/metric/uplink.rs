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

use crate::agent::meta::metric::Sender;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

pub enum UplinkEvent {
    Command,
    Event,
}

#[derive(Default, Form, Clone, PartialEq, Hash)]
pub struct UplinkProfile {
    event_delta: i32,
    event_rate: i32,
    event_count: i64,
    command_delta: i32,
    command_rate: i32,
    command_count: i64,
}

pub struct UplinkObserver {
    sender: Sender<UplinkEvent>,
}

impl UplinkObserver {
    pub fn new(sender: Sender<UplinkEvent>) -> UplinkObserver {
        UplinkObserver { sender }
    }

    pub fn on_event(&self) {
        self.sender.try_send(UplinkEvent::Event);
    }

    pub fn on_command(&self) {
        self.sender.try_send(UplinkEvent::Command);
    }

    pub fn did_open(&self) {
        self.sender.try_send()
    }
}
