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

use crate::byte_routing::message::Message;
use crate::byte_routing::routing::router::ServerRouter;
use crate::byte_routing::routing::router::{RouterError, RouterErrorKind};
use crate::byte_routing::routing::{RawRoute, Route};
use crate::byte_routing::Taggable;
use crate::compat::{RawRequestMessageEncoder, ResponseMessageEncoder};
use crate::routing::RoutingAddr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error as IoError, ErrorKind};
use std::str::FromStr;
use swim_model::path::RelativePath;
use swim_utilities::errors::Recoverable;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri};
use swim_warp::envelope::HeaderParseErr;
use thiserror::Error;
use tokio::time::sleep;
use tokio_util::codec::Encoder;

// todo this should have a task that automatically prunes handles to the agents every N minutes
pub struct Dispatcher {
    tag: RoutingAddr,
    retry_strategy: RetryStrategy,
    router: ServerRouter,
    agents: HashMap<RelativePath, Route<RawRequestMessageEncoder>>,
    downlinks: HashMap<RelativePath, Route<ResponseMessageEncoder>>,
}

impl Dispatcher {
    pub fn new(
        tag: RoutingAddr,
        retry_strategy: RetryStrategy,
        router: ServerRouter,
    ) -> Dispatcher {
        Dispatcher {
            tag,
            retry_strategy,
            router,
            agents: HashMap::default(),
            downlinks: HashMap::default(),
        }
    }

    pub fn register_downlink(&mut self, path: RelativePath, route: RawRoute) {
        self.downlinks
            .insert(path, Route::new(self.tag, route.writer, response_encoder()));
    }

    pub async fn dispatch(&mut self, message: Message<'_>) -> Result<(), DispatchError> {
        let Dispatcher {
            tag,
            retry_strategy,
            router,
            agents,
            downlinks,
        } = self;

        match message {
            Message::Request(message) => {
                dispatch(
                    *tag,
                    *retry_strategy,
                    router,
                    agents,
                    message.path.clone(),
                    message,
                    request_encoder,
                )
                .await
            }
            Message::Response(message) => {
                dispatch(
                    *tag,
                    *retry_strategy,
                    router,
                    downlinks,
                    message.path.clone(),
                    message,
                    response_encoder,
                )
                .await
            }
        }
    }
}

const fn request_encoder() -> RawRequestMessageEncoder {
    RawRequestMessageEncoder
}

const fn response_encoder() -> ResponseMessageEncoder {
    ResponseMessageEncoder
}

async fn dispatch<E, I>(
    tag: RoutingAddr,
    mut retry_strategy: RetryStrategy,
    router: &mut ServerRouter,
    map: &mut HashMap<RelativePath, Route<E>>,
    target: RelativePath,
    message: I,
    encoder_fac: fn() -> E,
) -> Result<(), DispatchError>
where
    E: Encoder<I::Out, Error = IoError>,
    I: Clone + Taggable,
{
    loop {
        // If the handle returned already existed, then it's possible that the route has timed out
        // between it being returned and the message send operation. In order to deliver the
        // message, we need to keep a copy of it in case the send operation fails as we will need to
        // reopen the route and then send it again. For now, cloning the message will suffice but a
        // more efficient operation will need to be implemented as if there is an error the message
        // is not returned like with MPSC channels.
        //
        // todo
        let dispatch_result = try_dispatch(
            tag,
            router,
            map,
            target.clone(),
            message.clone(),
            encoder_fac,
        )
        .await;
        match dispatch_result {
            Ok(()) => break Ok(()),
            Err(e) => {
                if e.is_fatal() {
                    break Err(e);
                } else {
                    match retry_strategy.next() {
                        Some(Some(duration)) => sleep(duration).await,
                        Some(None) => {}
                        None => break Err(e),
                    }
                }
            }
        }
    }
}

async fn try_dispatch<E, I>(
    tag: RoutingAddr,
    router: &mut ServerRouter,
    map: &mut HashMap<RelativePath, Route<E>>,
    target: RelativePath,
    message: I,
    encoder_fac: fn() -> E,
) -> Result<(), DispatchError>
where
    E: Encoder<I::Out, Error = IoError>,
    I: Taggable,
{
    let route = match map.entry(target) {
        Entry::Occupied(mut entry) => {
            if entry.get_mut().is_closed() {
                let route = try_open_route(tag, router, entry.key().clone(), encoder_fac).await?;
                *entry.get_mut() = route;
            }
            entry.into_mut()
        }
        Entry::Vacant(entry) => {
            let route = try_open_route(tag, router, entry.key().clone(), encoder_fac).await?;
            entry.insert(route)
        }
    };

    route.send(message).await.map_err(DispatchError::Io)
}

async fn try_open_route<E>(
    tag: RoutingAddr,
    router: &mut ServerRouter,
    target: RelativePath,
    encoder_fac: fn() -> E,
) -> Result<Route<E>, RouterError> {
    let target_addr = router
        .lookup(RelativeUri::from_str(target.node.as_str())?)
        .await?;

    let RawRoute { writer } = router.resolve_sender(target_addr).await?;
    Ok(Route::new(tag, writer, encoder_fac()))
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("Peer sent a malformatted message")]
    Malformatted,
    #[error("{0}")]
    Io(IoError),
    #[error("{0}")]
    Router(#[from] RouterError),
}

impl From<BadRelativeUri> for RouterError {
    fn from(e: BadRelativeUri) -> Self {
        RouterError::with_cause(RouterErrorKind::Resolution, e)
    }
}

impl From<HeaderParseErr> for DispatchError {
    fn from(_: HeaderParseErr) -> Self {
        DispatchError::Malformatted
    }
}

impl Recoverable for DispatchError {
    fn is_fatal(&self) -> bool {
        match self {
            DispatchError::Io(e) => match e.kind() {
                ErrorKind::BrokenPipe => false,
                _ => true,
            },
            _ => true,
        }
    }
}
