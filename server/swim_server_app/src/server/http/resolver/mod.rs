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

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use parking_lot::RwLock;
use swim_api::agent::HttpLaneRequest;
use swim_model::Text;
use swim_remote::{AgentResolutionError, FindNode, NodeConnectionRequest};
use swim_utilities::trigger;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

enum ResolverEntry {
    Pending(trigger::Receiver),
    Channel(mpsc::Sender<HttpLaneRequest>),
}

#[derive(Clone)]
pub struct Resolver {
    source: Uuid,
    resolved: Arc<RwLock<HashMap<Text, ResolverEntry>>>,
    find: mpsc::Sender<FindNode>,
}

enum Action {
    Lookup,
    Wait(trigger::Receiver),
    Send(mpsc::Sender<HttpLaneRequest>),
    Resolve,
}

impl Resolver {
    pub fn new(source: Uuid, find: mpsc::Sender<FindNode>) -> Self {
        Resolver {
            source,
            resolved: Default::default(),
            find,
        }
    }

    pub async fn send<'a>(
        &'a self,
        mut request: HttpLaneRequest,
    ) -> Result<(), AgentResolutionError> {
        let Resolver {
            source,
            resolved,
            find,
        } = self;

        let node = Text::new(
            percent_encoding::percent_decode_str(request.request.uri.path())
                .decode_utf8_lossy()
                .as_ref(),
        );
        let mut action = Action::Lookup;

        loop {
            match action {
                Action::Lookup => {
                    action = match resolved.read().get(&node) {
                        Some(ResolverEntry::Channel(tx)) => match tx.try_send(request) {
                            Ok(_) => break Ok(()),
                            Err(mpsc::error::TrySendError::Closed(req)) => {
                                request = req;
                                Action::Resolve
                            }
                            Err(mpsc::error::TrySendError::Full(req)) => {
                                request = req;
                                Action::Send(tx.clone())
                            }
                        },
                        Some(ResolverEntry::Pending(rx)) => Action::Wait(rx.clone()),
                        None => Action::Resolve,
                    };
                }
                Action::Wait(rx) => {
                    let _ = rx.await;
                    action = Action::Lookup;
                }
                Action::Send(tx) => match tx.send(request).await {
                    Ok(_) => break Ok(()),
                    Err(mpsc::error::SendError(req)) => {
                        request = req;
                        action = Action::Resolve;
                    }
                },
                Action::Resolve => {
                    let wait_tx = match resolved.write().entry(node.clone()) {
                        Entry::Occupied(entry) => {
                            match entry.get() {
                                ResolverEntry::Pending(rx) => {
                                    action = Action::Wait(rx.clone());
                                }
                                ResolverEntry::Channel(tx) => match tx.try_send(request) {
                                    Ok(_) => break Ok(()),
                                    Err(mpsc::error::TrySendError::Closed(req)) => {
                                        request = req;
                                        action = Action::Resolve;
                                    }
                                    Err(mpsc::error::TrySendError::Full(req)) => {
                                        request = req;
                                        action = Action::Send(tx.clone());
                                    }
                                },
                            }
                            continue;
                        }
                        Entry::Vacant(entry) => {
                            let (wait_tx, wait_rx) = trigger::trigger();
                            entry.insert(ResolverEntry::Pending(wait_rx));
                            wait_tx
                        }
                    };
                    let (find_tx, find_rx) = oneshot::channel();
                    if find
                        .send(FindNode {
                            source: *source,
                            node: Text::new(node.as_ref()),
                            lane: None,
                            request: NodeConnectionRequest::Http { promise: find_tx },
                        })
                        .await
                        .is_err()
                    {
                        return Err(AgentResolutionError::PlaneStopping);
                    }
                    let sender = if let Ok(result) = find_rx.await {
                        result?
                    } else {
                        return Err(AgentResolutionError::PlaneStopping);
                    };
                    match sender.send(request).await {
                        Ok(_) => {
                            resolved
                                .write()
                                .insert(Text::new(node.as_ref()), ResolverEntry::Channel(sender));
                            wait_tx.trigger();
                            return Ok(());
                        }
                        Err(mpsc::error::SendError(req)) => {
                            request = req;
                            action = Action::Resolve;
                        }
                    }
                }
            }
        }
    }
}
