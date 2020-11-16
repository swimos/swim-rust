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

use crate::routing::error::{ConnectionError, ResolutionError, RouterError, Unresolvable};
use crate::routing::ws::{CloseReason, JoinedStreamSink};
use crate::routing::{
    ConnectionDropped, Route, RoutingAddr, ServerRouter, ServerRouterFactory, TaggedEnvelope,
    TaggedSender,
};
use futures::future::{ready, BoxFuture};
use futures::task::{Context, Poll};
use futures::{ready, FutureExt, Sink, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use swim_common::sink::{MpscSink, SinkSendError};
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[derive(Debug)]
struct Entry {
    route: Route<TaggedSender>,
    on_drop: promise::Sender<ConnectionDropped>,
    countdown: u8,
}

#[derive(Debug, Default)]
pub struct LocalRoutesInner {
    routes: HashMap<RoutingAddr, Entry>,
    uri_mappings: HashMap<RelativeUri, (RoutingAddr, u8)>,
    counter: u32,
}

#[derive(Debug, Clone)]
pub struct LocalRoutes(RoutingAddr, Arc<Mutex<LocalRoutesInner>>);

impl LocalRoutes {
    pub(crate) fn new(owner_addr: RoutingAddr) -> Self {
        LocalRoutes(owner_addr, Default::default())
    }
}

impl ServerRouter for LocalRoutes {
    type Sender = TaggedSender;

    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, ResolutionError>> {
        let lock = self.1.lock();
        let result = if let Some(Entry {
            route, countdown, ..
        }) = lock.routes.get(&addr)
        {
            if *countdown == 0 {
                Ok(route.clone())
            } else {
                Err(ResolutionError::Unresolvable(Unresolvable(addr)))
            }
        } else {
            Err(ResolutionError::Unresolvable(Unresolvable(addr)))
        };
        ready(result).boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        let mut lock = self.1.lock();
        let result = if host.is_some() {
            Err(RouterError::ConnectionFailure(ConnectionError::Resolution))
        } else {
            if let Some((addr, countdown)) = lock.uri_mappings.get_mut(&route) {
                if *countdown == 0 {
                    Ok(*addr)
                } else {
                    *countdown -= 1;
                    let addr = *addr;
                    if let Some(Entry { countdown, .. }) = lock.routes.get_mut(&addr) {
                        *countdown -= 1;
                    }
                    // A non-fatal error that will allow a retry.
                    Err(RouterError::ConnectionFailure(ConnectionError::Warp(
                        "Oh no!".to_string(),
                    )))
                }
            } else {
                Err(RouterError::NoAgentAtRoute(route))
            }
        };
        ready(result).boxed()
    }
}

impl LocalRoutes {
    pub fn add_with_countdown(
        &self,
        uri: RelativeUri,
        countdown: u8,
    ) -> mpsc::Receiver<TaggedEnvelope> {
        let LocalRoutes(owner_addr, inner) = self;
        let LocalRoutesInner {
            routes,
            uri_mappings,
            counter,
        } = &mut *inner.lock();
        if uri_mappings.contains_key(&uri) {
            panic!("Duplicate registration.");
        } else {
            let id = RoutingAddr::local(*counter);
            *counter += 1;
            uri_mappings.insert(uri, (id, countdown));
            let (tx, rx) = mpsc::channel(8);
            let (drop_tx, drop_rx) = promise::promise();
            let route = Route::new(TaggedSender::new(*owner_addr, tx), drop_rx);
            routes.insert(
                id,
                Entry {
                    route,
                    on_drop: drop_tx,
                    countdown,
                },
            );
            rx
        }
    }

    pub fn add(&self, uri: RelativeUri) -> mpsc::Receiver<TaggedEnvelope> {
        self.add_with_countdown(uri, 0)
    }
}

impl ServerRouterFactory for LocalRoutes {
    type Router = LocalRoutes;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        let LocalRoutes(_, inner) = self;
        LocalRoutes(addr, inner.clone())
    }
}

pub struct TwoWayMpsc<T, E> {
    tx: MpscSink<T>,
    rx: mpsc::Receiver<Result<T, E>>,
    failures: Box<dyn Fn(&T) -> Option<E> + Send + Unpin>,
}

impl<T, E> TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn new<F>(tx: mpsc::Sender<T>, rx: mpsc::Receiver<Result<T, E>>, failures: F) -> Self
    where
        F: Fn(&T) -> Option<E> + Send + Unpin + 'static,
    {
        TwoWayMpsc {
            tx: MpscSink::wrap(tx),
            rx,
            failures: Box::new(failures),
        }
    }
}

impl<T, E> Sink<T> for TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    E: From<SinkSendError<T>>,
{
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(ready!(self.get_mut().tx.poll_ready_unpin(cx))?))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        if let Some(err) = (self.as_ref().get_ref().failures)(&item) {
            Err(err)
        } else {
            self.get_mut().tx.start_send_unpin(item)?;
            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(ready!(self.get_mut().tx.poll_flush_unpin(cx))?))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(ready!(self.get_mut().tx.poll_close_unpin(cx))?))
    }
}

impl<T, E> Stream for TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    E: From<SinkSendError<T>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_next_unpin(cx)
    }
}

impl<T, E> JoinedStreamSink<T, E> for TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    E: From<SinkSendError<T>>,
{
    type CloseFut = BoxFuture<'static, Result<(), E>>;

    fn close(&mut self, _reason: Option<CloseReason>) -> Self::CloseFut {
        ready(Ok(())).boxed()
    }
}
