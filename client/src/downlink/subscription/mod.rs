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

use crate::configuration::downlink::{Config, MuxMode};
use crate::downlink::any::{AnyDownlink, AnyReceiver};
use crate::downlink::model::map::{MapAction, ViewWithEvent};
use crate::downlink::model::value;
use crate::downlink::model::value::SharedValue;
use crate::downlink::Command;
use crate::router::Router;
use crate::sink::item::ItemSender;
use common::model::Value;
use common::request::Request;
use common::topic::Topic;
use common::warp::path::AbsolutePath;
use futures::Stream;
use futures_util::future::TryFutureExt;
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub mod envelopes;

pub type ValueDownlink = AnyDownlink<value::Action, SharedValue>;
pub type MapDownlink = AnyDownlink<MapAction, ViewWithEvent>;

pub type ValueReceiver = AnyReceiver<SharedValue>;
pub type MapReceiver = AnyReceiver<ViewWithEvent>;

pub struct Downlinks {
    sender: mpsc::Sender<DownlinkRequest>,
    _task: JoinHandle<()>,
}

impl Downlinks {
    pub async fn new<C, R>(config: Arc<C>, router: R) -> Downlinks
    where
        C: Config + 'static,
        R: Router + 'static,
    {
        let client_params = config.client_params();
        let task = DownlinkTask::new(config, router);
        let (tx, rx) = mpsc::channel(client_params.dl_req_buffer_size.get());
        let task_handle = tokio::task::spawn(task.run(rx));
        Downlinks {
            sender: tx,
            _task: task_handle,
        }
    }

    pub async fn subscribe_value(
        &mut self,
        init: Value,
        path: AbsolutePath,
    ) -> Result<(ValueDownlink, ValueReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkRequest::Value(init, path, Request::new(tx)))
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }

    pub async fn subscribe_map(
        &mut self,
        path: AbsolutePath,
    ) -> Result<(MapDownlink, MapReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkRequest::Map(path, Request::new(tx)))
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }
}

#[derive(Clone, Debug)]
pub enum DownlinkKind {
    Value,
    Map,
}

impl Display for DownlinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DownlinkKind::Value => write!(f, "Value"),
            DownlinkKind::Map => write!(f, "Map"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SubscriptionError {
    BadKind {
        expected: DownlinkKind,
        actual: DownlinkKind,
    },
    DownlinkTaskStopped,
}

impl From<mpsc::error::SendError<DownlinkRequest>> for SubscriptionError {
    fn from(_: SendError<DownlinkRequest>) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl From<oneshot::error::RecvError> for SubscriptionError {
    fn from(_: RecvError) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl Display for SubscriptionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionError::BadKind { expected, actual } => write!(
                f,
                "Requested {} downlink but a {} downlink was already open for that lane.",
                expected, actual
            ),
            SubscriptionError::DownlinkTaskStopped => {
                write!(f, "The downlink task has already stopped.")
            }
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl SubscriptionError {
    pub fn bad_kind(expected: DownlinkKind, actual: DownlinkKind) -> SubscriptionError {
        SubscriptionError::BadKind { expected, actual }
    }
}

pub type Result<T> = std::result::Result<T, SubscriptionError>;

pub enum DownlinkRequest {
    Value(
        Value,
        AbsolutePath,
        Request<Result<(ValueDownlink, ValueReceiver)>>,
    ),
    Map(AbsolutePath, Request<Result<(MapDownlink, MapReceiver)>>),
}

struct DownlinkTask<R> {
    config: Arc<dyn Config>,
    value_downlinks: HashMap<AbsolutePath, ValueDownlink>,
    map_downlinks: HashMap<AbsolutePath, MapDownlink>,
    router: R,
}

impl<R> DownlinkTask<R>
where
    R: Router,
{
    fn new<C>(config: Arc<C>, router: R) -> DownlinkTask<R>
    where
        C: Config + 'static,
    {
        DownlinkTask {
            config,
            value_downlinks: HashMap::new(),
            map_downlinks: HashMap::new(),
            router,
        }
    }

    async fn create_new_value(
        &mut self,
        init: Value,
        path: AbsolutePath,
    ) -> (ValueDownlink, ValueReceiver) {
        use crate::downlink::model::value::*;

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await;

        //TODO Do something with invalid envelopes rather than discarding them.
        let updates = incoming.filter_map(|env| envelopes::value::try_from_envelope(env).ok());

        let sink_path = path.clone();
        let cmd_sink = sink
            .comap(move |cmd: Command<SharedValue>| envelopes::value_envelope(&sink_path, cmd).1);
        let buffer_size = config.buffer_size.get();
        let (dl, rec) = match config.mux_mode {
            MuxMode::Queue(n) => {
                let (dl, rec) =
                    create_queue_downlink(init, updates, cmd_sink, buffer_size, n.get());
                (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
            }
            MuxMode::Dropping => {
                let (dl, rec) = create_dropping_downlink(init, updates, cmd_sink, buffer_size);
                (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
            }
            MuxMode::Buffered(n) => {
                let (dl, rec) =
                    create_buffered_downlink(init, updates, cmd_sink, buffer_size, n.get());
                (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
            }
        };
        self.value_downlinks.insert(path, dl.clone());
        (dl, rec)
    }

    async fn create_new_map(&mut self, path: AbsolutePath) -> (MapDownlink, MapReceiver) {
        use crate::downlink::model::map::*;

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await;

        //TODO Do something with invalid envelopes rather than discarding them.
        let updates = incoming.filter_map(|env| envelopes::map::try_from_envelope(env).ok());

        let sink_path = path.clone();
        let cmd_sink = sink.comap(move |cmd: Command<MapModification<Arc<Value>>>| {
            envelopes::map_envelope(&sink_path, cmd).1
        });
        let buffer_size = config.buffer_size.get();
        let (dl, rec) = match config.mux_mode {
            MuxMode::Queue(n) => {
                let (dl, rec) = create_queue_downlink(updates, cmd_sink, buffer_size, n.get());
                (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
            }
            MuxMode::Dropping => {
                let (dl, rec) = create_dropping_downlink(updates, cmd_sink, buffer_size);
                (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
            }
            MuxMode::Buffered(n) => {
                let (dl, rec) = create_buffered_downlink(updates, cmd_sink, buffer_size, n.get());
                (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
            }
        };
        self.map_downlinks.insert(path, dl.clone());
        (dl, rec)
    }

    async fn run<Req>(mut self, requests: Req)
    where
        Req: Stream<Item = DownlinkRequest>,
    {
        pin_mut!(requests);

        let mut pinned_requests: Pin<&mut Req> = requests;

        while let Some(request) = pinned_requests.next().await {
            match request {
                DownlinkRequest::Value(init, path, value_req) => {
                    let dl = match self.value_downlinks.get(&path) {
                        Some(dl) => {
                            let mut dl_clone = dl.clone();
                            match dl_clone.subscribe().await {
                                Ok(rec) => Ok((dl_clone, rec)),
                                Err(_) => {
                                    self.value_downlinks.remove(&path);
                                    Ok(self.create_new_value(init, path).await)
                                },
                            }
                        }
                        _ => match self.map_downlinks.get(&path) {
                            Some(_) => Err(SubscriptionError::bad_kind(
                                DownlinkKind::Value,
                                DownlinkKind::Map,
                            )),
                            _ => Ok(self.create_new_value(init, path).await),
                        },
                    };
                    let _ = value_req.send(dl);
                }
                DownlinkRequest::Map(path, map_req) => {
                    let dl = match self.map_downlinks.get(&path) {
                        Some(dl) => {
                            let mut dl_clone = dl.clone();
                            match dl_clone.subscribe().await {
                                Ok(rec) => Ok((dl_clone, rec)),
                                Err(_) => {
                                    self.map_downlinks.remove(&path);
                                    Ok(self.create_new_map(path).await)
                                },
                            }
                        }
                        _ => match self.value_downlinks.get(&path) {
                            Some(_) => Err(SubscriptionError::bad_kind(
                                DownlinkKind::Map,
                                DownlinkKind::Value,
                            )),
                            _ => Ok(self.create_new_map(path).await),
                        },
                    };
                    let _ = map_req.send(dl);
                }
            }
        }
    }
}
