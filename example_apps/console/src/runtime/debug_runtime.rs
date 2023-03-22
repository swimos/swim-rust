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

use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt};
use parking_lot::RwLock;
use swim_utilities::trigger;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    model::{RuntimeCommand, UIUpdate},
    shared_state::SharedState,
    ui::ViewUpdater,
    RuntimeFactory,
};

#[derive(Debug, Default)]
pub struct DebugFactory;

impl RuntimeFactory for DebugFactory {
    fn run(
        &self,
        shared_state: Arc<RwLock<SharedState>>,
        mut commands: UnboundedReceiver<RuntimeCommand>,
        mut updater: Box<dyn ViewUpdater + Send + 'static>,
        mut stop: trigger::Receiver,
    ) -> BoxFuture<'static, ()> {
        async move {
            loop {
                let command = tokio::select! {
                    _ = &mut stop => {
                        break;
                    }
                    maybe_cmd = commands.recv() => {
                        if let Some(cmd) = maybe_cmd {
                            cmd
                        } else {
                            break;
                        }
                    }
                };
                let string = format!("{:?}", command);
                if let RuntimeCommand::Link { endpoint, response } = command {
                    let id = shared_state.write().insert(endpoint);
                    response.send(Ok(id));
                }
                updater
                    .update(UIUpdate::LogMessage(string))
                    .expect("UI failed.");
            }
        }
        .boxed()
    }
}
