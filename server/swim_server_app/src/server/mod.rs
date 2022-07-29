// Copyright 2015-2021 Swim Inc.
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

use futures::future::BoxFuture;
use swim_utilities::trigger;

mod builder;
mod runtime;

pub use builder::ServerBuilder;

pub struct ServerHandle {
    stop_trigger: Option<trigger::Sender>,
}

impl ServerHandle {
    fn new(tx: trigger::Sender) -> Self {
        ServerHandle {
            stop_trigger: Some(tx),
        }
    }
}

impl ServerHandle {
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_trigger.take() {
            tx.trigger();
        }
    }
}

pub trait Server {
    fn run(self) -> (BoxFuture<'static, Result<(), std::io::Error>>, ServerHandle);
}
