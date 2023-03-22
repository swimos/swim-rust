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

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{
    stream::{unfold, SelectAll},
    FutureExt, Stream, StreamExt,
};
use parking_lot::RwLock;
use ratchet::ErrorKind;
use swim_recon::printer::print_recon;
use swim_utilities::trigger;
use tokio::{sync::mpsc as tmpsc, task::block_in_place};

use crate::{
    model::{DisplayResponse, Endpoint, Host, RuntimeCommand, UIUpdate},
    shared_state::SharedState,
    ui::ViewUpdater,
    RuntimeFactory,
};

#[derive(Debug, Default)]
pub struct DummyRuntimeFactory;

impl RuntimeFactory for DummyRuntimeFactory {
    fn run(
        &self,
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        updater: Box<dyn ViewUpdater + Send + 'static>,
        stop: trigger::Receiver,
    ) -> futures::future::BoxFuture<'static, ()> {
        let runtime = DummyRuntime::new(shared_state, commands, updater, stop);
        runtime.run().boxed()
    }
}

pub struct DummyRuntime {
    shared_state: Arc<RwLock<SharedState>>,
    commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
    output: Box<dyn ViewUpdater + Send + 'static>,
    stop: trigger::Receiver,
}

const REMOTE: &str = "localhost";
const PORT: u16 = 8080;

impl DummyRuntime {
    fn new(
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        output: Box<dyn ViewUpdater + Send + 'static>,
        stop: trigger::Receiver,
    ) -> Self {
        DummyRuntime {
            shared_state,
            commands,
            output,
            stop,
        }
    }

    async fn run(self) {
        let DummyRuntime {
            shared_state,
            mut commands,
            mut output,
            mut stop,
        } = self;
        let mut links = SelectAll::new();
        let mut unlinkers = HashMap::new();

        loop {
            let command = tokio::select! {
                _ = &mut stop => break,
                maybe_cmd = commands.recv() => {
                    if let Some(command) = maybe_cmd {
                        command
                    } else {
                        break;
                    }
                },
                event = links.next(), if !links.is_empty() => {
                    if let Some(ev) = event {
                        let out_ref = &mut output;
                        block_in_place(move || out_ref.update(UIUpdate::LinkDisplay(ev))).unwrap();
                    }
                    continue;
                }
            };

            match command {
                RuntimeCommand::Link { endpoint, response } => {
                    let Endpoint {
                        remote: Host { host_name, port },
                        ..
                    } = endpoint.clone();
                    if host_name == REMOTE && port == PORT {
                        let id = shared_state.write().insert(endpoint);
                        let (done_tx, done_rx) = trigger::trigger();
                        if unlinkers.is_empty() {
                            let out_ref = &mut output;
                            block_in_place(move || {
                                out_ref.update(UIUpdate::LogMessage(
                                    "Opening connection to localhost:8080.".to_string(),
                                ))
                            })
                            .unwrap();
                        }
                        links.push(fake_link(id, done_rx).boxed());
                        unlinkers.insert(id, done_tx);

                        response.send(Ok(id));
                    } else {
                        response.send(Err(ratchet::Error::new(ErrorKind::IO)))
                    }
                }
                RuntimeCommand::Sync(id) => {
                    let out_ref = &mut output;
                    block_in_place(move || {
                        out_ref.update(UIUpdate::LogMessage(format!("Sync for {}.", id)))
                    })
                    .unwrap();
                }
                RuntimeCommand::Command(id, body) => {
                    let out_ref = &mut output;
                    let msg = format!("Command '{}' sent to {}.", print_recon(&body), id);
                    block_in_place(move || out_ref.update(UIUpdate::LogMessage(msg))).unwrap();
                }
                RuntimeCommand::AdHocCommand(target, body) => {
                    let out_ref = &mut output;
                    let msg = format!(
                        "Ad-hoc command '{}' sent to {:?}.",
                        print_recon(&body),
                        target
                    );
                    block_in_place(move || out_ref.update(UIUpdate::LogMessage(msg))).unwrap();
                }
                RuntimeCommand::Unlink(id) => {
                    if let Some(tx) = unlinkers.remove(&id) {
                        tx.trigger();
                        if unlinkers.is_empty() {
                            let out_ref = &mut output;
                            block_in_place(move || {
                                out_ref.update(UIUpdate::LogMessage(
                                    "Closing connection to localhost:8080.".to_string(),
                                ))
                            })
                            .unwrap();
                        }
                    }
                    shared_state.write().remove(id);
                }
            }
        }
    }
}

const INTERVAL: Duration = Duration::from_secs(2);

fn fake_link(id: usize, done: trigger::Receiver) -> impl Stream<Item = DisplayResponse> {
    let mut n = 0;
    unfold((true, Some(done)), move |(first, stop)| {
        let i = n;
        n += 1;
        async move {
            if first {
                Some((DisplayResponse::linked(id), (false, stop)))
            } else {
                if let Some(mut stop) = stop {
                    match tokio::time::timeout(INTERVAL, &mut stop).await {
                        Ok(_) => Some((DisplayResponse::unlinked(id), (false, None))),
                        Err(_) => Some((
                            DisplayResponse::event(id, format!("{}", i)),
                            (false, Some(stop)),
                        )),
                    }
                } else {
                    None
                }
            }
        }
    })
}
