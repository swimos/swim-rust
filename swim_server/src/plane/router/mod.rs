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

use crate::plane::error::ResolutionError;
use crate::plane::PlaneRequest;
use crate::routing::{RoutingAddr, ServerRouter, TaggedSender};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_common::request::Request;
use swim_common::routing::RoutingError;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Creates [`PlaneRouter`] instances by cloning a channel back to the plane.
#[derive(Debug)]
pub struct PlaneRouterFactory {
    request_sender: mpsc::Sender<PlaneRequest>,
}

impl PlaneRouterFactory {
    /// Create a factory from a channel back to the owning plane.
    pub(super) fn new(request_sender: mpsc::Sender<PlaneRequest>) -> Self {
        PlaneRouterFactory { request_sender }
    }

    /// Create a router instance for a specific endpoint.
    pub fn create(&self, tag: RoutingAddr) -> PlaneRouter {
        PlaneRouter::new(tag, self.request_sender.clone())
    }
}

/// An implementation of [`ServerRouter`] tied to a plane.
#[derive(Debug, Clone)]
pub struct PlaneRouter {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<PlaneRequest>,
}

impl PlaneRouter {
    fn new(tag: RoutingAddr, request_sender: mpsc::Sender<PlaneRequest>) -> Self {
        PlaneRouter {
            tag,
            request_sender,
        }
    }
}

impl ServerRouter for PlaneRouter {

    fn get_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<TaggedSender, RoutingError>> {
        async move {
            let PlaneRouter {
                tag,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(PlaneRequest::Endpoint {
                    id: addr,
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(RoutingError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(sender)) => Ok(TaggedSender::new(*tag, sender)),
                    Ok(Err(_)) => Err(RoutingError::HostUnreachable),
                    Err(_) => Err(RoutingError::RouterDropped),
                }
            }
        }
        .boxed()
    }

    fn resolve(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, ResolutionError>> {
        async move {
            let PlaneRouter { request_sender, .. } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(PlaneRequest::Resolve {
                    host,
                    name: route,
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(ResolutionError::NoRoute(RoutingError::RouterDropped))
            } else {
                match rx.await {
                    Ok(Ok(addr)) => Ok(addr),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(ResolutionError::NoRoute(RoutingError::RouterDropped)),
                }
            }
        }
        .boxed()
    }
}