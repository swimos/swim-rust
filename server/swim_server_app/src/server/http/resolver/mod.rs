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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use parking_lot::RwLock;
use swim_api::agent::HttpLaneRequest;
use swim_model::Text;
use swim_remote::{AgentResolutionError, FindNode, NodeConnectionRequest};
use swim_utilities::{time::AtomicInstant, trigger};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

#[derive(Debug)]
enum ResolverEntry {
    Pending(trigger::Receiver),
    Channel {
        sender: mpsc::Sender<HttpLaneRequest>,
        last_used: AtomicInstant,
    },
}

impl ResolverEntry {
    pub fn into_last_used(self) -> Option<std::time::Instant> {
        match self {
            ResolverEntry::Pending(_) => None,
            ResolverEntry::Channel { last_used, .. } => Some(last_used.into_inner()),
        }
    }
}

#[derive(Debug)]
struct Inner {
    resolved_map: HashMap<Text, ResolverEntry>,
    oldest: AtomicInstant,
    oldest_defined: AtomicBool,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            resolved_map: Default::default(),
            oldest: AtomicInstant::new(std::time::Instant::now()),
            oldest_defined: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Resolver {
    resolved: Arc<RwLock<Inner>>,
    find: mpsc::Sender<FindNode>,
    inactive_timeout: Duration,
}

enum Action {
    Lookup,
    Wait(trigger::Receiver),
    Send(mpsc::Sender<HttpLaneRequest>),
    Resolve,
}

impl Resolver {
    pub fn new(find: mpsc::Sender<FindNode>, inactive_timeout: Duration) -> Self {
        Resolver {
            resolved: Default::default(),
            find,
            inactive_timeout,
        }
    }

    pub fn check_access_times(&self) -> Option<std::time::Instant> {
        let Resolver {
            resolved,
            inactive_timeout,
            ..
        } = self;
        let guard = resolved.read();
        let now = Instant::now().into_std();
        let oldest_ts = if !guard.resolved_map.is_empty() {
            if !guard.oldest_defined.load(Ordering::Relaxed)
                || now - guard.oldest.load(Ordering::Relaxed) > *inactive_timeout
            {
                drop(guard);
                let mut guard = resolved.write();
                let Inner {
                    resolved_map,
                    oldest,
                    oldest_defined,
                } = &mut *guard;
                let mut new_oldest = None;
                resolved_map.retain(|_, v| match v {
                    ResolverEntry::Channel { last_used, .. } => {
                        let t = last_used.load(Ordering::Relaxed);
                        let keep = now - last_used.load(Ordering::Relaxed) < *inactive_timeout;
                        if keep {
                            if let Some(prev) = new_oldest {
                                new_oldest = Some(t.min(prev));
                            } else {
                                new_oldest = Some(t);
                            }
                        }
                        keep
                    }
                    _ => true,
                });
                if let Some(t) = new_oldest {
                    oldest.store(t, Ordering::Relaxed);
                    oldest_defined.store(true, Ordering::Relaxed);
                } else {
                    oldest_defined.store(false, Ordering::Relaxed);
                }
                new_oldest
            } else {
                Some(guard.oldest.load(Ordering::Relaxed))
            }
        } else {
            None
        };
        oldest_ts.map(|t| t + *inactive_timeout)
    }

    pub async fn send(&self, mut request: HttpLaneRequest) -> Result<(), AgentResolutionError> {
        let Resolver { resolved, find, .. } = self;
        let node = Text::new(
            percent_encoding::percent_decode_str(request.request.uri.path())
                .decode_utf8_lossy()
                .as_ref(),
        );
        let mut action = Action::Lookup;

        let result = loop {
            match action {
                Action::Lookup => {
                    action = match resolved.read().resolved_map.get(&node) {
                        Some(ResolverEntry::Channel { sender, last_used }) => {
                            match sender.try_send(request) {
                                Ok(_) => {
                                    last_used.store(Instant::now().into_std(), Ordering::Relaxed);
                                    break Ok(());
                                }
                                Err(mpsc::error::TrySendError::Closed(req)) => {
                                    request = req;
                                    Action::Resolve
                                }
                                Err(mpsc::error::TrySendError::Full(req)) => {
                                    last_used.store(Instant::now().into_std(), Ordering::Relaxed);
                                    request = req;
                                    Action::Send(sender.clone())
                                }
                            }
                        }
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
                    let wait_tx = {
                        let mut guard = resolved.write();
                        let Inner {
                            resolved_map,
                            oldest,
                            oldest_defined,
                        } = &mut *guard;
                        match resolved_map.entry(node.clone()) {
                            Entry::Occupied(mut entry) => {
                                match entry.get_mut() {
                                    ResolverEntry::Pending(rx) => {
                                        action = Action::Wait(rx.clone());
                                    }
                                    ResolverEntry::Channel { sender, last_used } => {
                                        match sender.try_send(request) {
                                            Ok(_) => {
                                                last_used.store(
                                                    Instant::now().into_std(),
                                                    Ordering::Relaxed,
                                                );
                                                break Ok(());
                                            }
                                            Err(mpsc::error::TrySendError::Closed(req)) => {
                                                request = req;
                                                if let Some(last_used) =
                                                    entry.remove().into_last_used()
                                                {
                                                    if *oldest_defined.get_mut()
                                                        && oldest.get() == last_used
                                                    {
                                                        *oldest_defined.get_mut() = false;
                                                    }
                                                }
                                                action = Action::Resolve;
                                            }
                                            Err(mpsc::error::TrySendError::Full(req)) => {
                                                last_used.store(
                                                    Instant::now().into_std(),
                                                    Ordering::Relaxed,
                                                );
                                                request = req;
                                                action = Action::Send(sender.clone());
                                            }
                                        }
                                    }
                                }
                                continue;
                            }
                            Entry::Vacant(entry) => {
                                let (wait_tx, wait_rx) = trigger::trigger();
                                entry.insert(ResolverEntry::Pending(wait_rx));
                                wait_tx
                            }
                        }
                    };
                    let (find_tx, find_rx) = oneshot::channel();
                    if find
                        .send(FindNode {
                            node: Text::new(node.as_ref()),
                            lane: None,
                            request: NodeConnectionRequest::Http { promise: find_tx },
                        })
                        .await
                        .is_err()
                    {
                        break Err(AgentResolutionError::PlaneStopping);
                    }
                    let sender = if let Ok(result) = find_rx.await {
                        result?
                    } else {
                        break Err(AgentResolutionError::PlaneStopping);
                    };
                    match sender.send(request).await {
                        Ok(_) => {
                            let mut guard = resolved.write();
                            let Inner {
                                resolved_map,
                                oldest,
                                oldest_defined,
                            } = &mut *guard;
                            let now = Instant::now().into_std();
                            if resolved_map.is_empty() {
                                *oldest_defined.get_mut() = true;
                                oldest.set(now);
                            }
                            resolved_map.insert(
                                Text::new(node.as_ref()),
                                ResolverEntry::Channel {
                                    sender,
                                    last_used: AtomicInstant::new(now),
                                },
                            );
                            wait_tx.trigger();
                            break Ok(());
                        }
                        Err(mpsc::error::SendError(req)) => {
                            request = req;
                            action = Action::Resolve;
                        }
                    }
                }
            }
        };
        result
    }
}
