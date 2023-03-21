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

use std::{sync::Arc, time::Duration, collections::HashMap};

use crossbeam_channel as cmpsc;
use futures::{Stream, StreamExt, stream::{unfold, SelectAll}};
use parking_lot::RwLock;
use ratchet::ErrorKind;
use swim_recon::printer::print_recon;
use swim_utilities::trigger;
use tokio::{sync::mpsc as tmpsc, task::block_in_place};

use crate::{shared_state::SharedState, model::{RuntimeCommand, DisplayResponse, Host, Endpoint}};

pub struct DummyRuntime {
    shared_state: Arc<RwLock<SharedState>>,
    commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
    output: cmpsc::Sender<DisplayResponse>,
    errors: cmpsc::Sender<String>,
    stop: trigger::Receiver,
}

const REMOTE: &str = "localhost";
const PORT: u16 = 8080;

impl DummyRuntime {
    pub fn new(
        shared_state: Arc<RwLock<SharedState>>,
        commands: tmpsc::UnboundedReceiver<RuntimeCommand>,
        output: cmpsc::Sender<DisplayResponse>,
        errors: cmpsc::Sender<String>,
        stop: trigger::Receiver,
    ) -> Self {
        DummyRuntime {
            shared_state,
            commands,
            output,
            errors,
            stop,
        }
    }

    pub async fn run(self) {
        let DummyRuntime { shared_state, mut commands, output, errors, mut stop } = self;
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
                        let out_ref = &output;
                        block_in_place(move || out_ref.send(ev)).unwrap();
                    }
                    continue;
                }
            };

            match command {
                RuntimeCommand::Link { endpoint, response } => {
                    let Endpoint { remote: Host { host_name, port }, .. } = endpoint.clone();
                    if host_name == REMOTE && port == PORT {
                        let id = shared_state.write().insert(endpoint);
                        let (done_tx, done_rx) = trigger::trigger();
                        links.push(fake_link(id, done_rx).boxed());
                        unlinkers.insert(id, done_tx);
                        response.send(Ok(id));
                    } else {
                        response.send(Err(ratchet::Error::new(ErrorKind::IO)))
                    }
                },
                RuntimeCommand::Sync(id) => {
                    let err_ref = &errors;
                    block_in_place(move || err_ref.send(format!("Sync for {}.", id))).unwrap();
                },
                RuntimeCommand::Command(id, body) => {
                    let err_ref = &errors;
                    let msg = format!("Command '{}' sent to {}.", print_recon(&body), id);
                    block_in_place(move || err_ref.send(msg)).unwrap();
                },
                RuntimeCommand::AdHocCommand(target, body) => {
                    let err_ref = &errors;
                    let msg = format!("Ad-hoc command '{}' sent to {:?}.", print_recon(&body), target);
                    block_in_place(move || err_ref.send(msg)).unwrap();
                },
                RuntimeCommand::Unlink(id) => {
                    if let Some(tx) = unlinkers.remove(&id) {
                        tx.trigger();
                    }
                    shared_state.write().remove(id);
                },
            }
        }
    }
}

const INTERVAL: Duration = Duration::from_secs(2);

fn fake_link(id: usize, 
    done: trigger::Receiver) -> impl Stream<Item = DisplayResponse> {

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
                        Err(_) => Some((DisplayResponse::event(id, format!("{}", i)), (false, Some(stop)))),
                    }
                } else {
                    None
                }
            }
            
        }
    })
}
