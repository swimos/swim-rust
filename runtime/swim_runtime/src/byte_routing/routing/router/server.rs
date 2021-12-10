// Copyright 2015-2021 SWIM.AI inc.
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

use crate::byte_routing::routing::router::error::{RouterError, RouterErrorKind};
use crate::byte_routing::routing::router::models::{PlaneRoutingRequest, RemoteRoutingRequest};
use crate::byte_routing::routing::{Address, RawRoute};
use crate::routing::{RoutingAddr, RoutingAddrKind};
use std::convert::identity;
use std::future::Future;
use swim_utilities::future::request::Request;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct ServerRouter {
    plane: mpsc::Sender<PlaneRoutingRequest>,
    remote: mpsc::Sender<RemoteRoutingRequest>,
}

impl ServerRouter {
    pub fn new(
        plane: mpsc::Sender<PlaneRoutingRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> ServerRouter {
        ServerRouter { plane, remote }
    }

    async fn exec<Func, Fut, E, T>(&self, op: Func) -> Result<T, RouterError>
    where
        Func: FnOnce(oneshot::Sender<Result<T, RouterError>>) -> Fut,
        Fut: Future<Output = Result<(), SendError<E>>>,
    {
        let (callback_tx, callback_rx) = oneshot::channel();

        op(callback_tx)
            .await
            .map_err(|_| RouterError::new(RouterErrorKind::RouterDropped))?;
        callback_rx
            .await
            .map_err(|_| RouterError::new(RouterErrorKind::RouterDropped))
            .and_then(identity)
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<RawRoute, RouterError> {
        match addr.discriminate() {
            RoutingAddrKind::Remote => {
                let tx = &self.remote;
                self.exec(|callback| {
                    tx.send(RemoteRoutingRequest::Endpoint {
                        addr,
                        request: Request::new(callback),
                    })
                })
                .await
            }
            RoutingAddrKind::Plane => {
                let tx = &self.plane;
                self.exec(|callback| {
                    tx.send(PlaneRoutingRequest::Endpoint {
                        addr,
                        request: Request::new(callback),
                    })
                })
                .await
            }
            RoutingAddrKind::Client => {
                // todo
                Err(RouterError::new(RouterErrorKind::Resolution))
            }
        }
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RouterError>
    where
        A: Into<Address>,
    {
        let (host, uri) = match address.into() {
            Address::Local(uri) => (None, uri),
            Address::Remote(host, uri) => (Some(host), uri),
        };

        let tx = &self.plane;
        self.exec(|callback| {
            tx.send(PlaneRoutingRequest::Resolve {
                host,
                uri,
                request: Request::new(callback),
            })
        })
        .await
    }
}
