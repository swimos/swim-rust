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

use crate::byte_routing::codec::TaggedMessage;
use crate::byte_routing::routing::{RawRoute, Route, Router};
use crate::compat::{RawRequestMessageEncoder, ResponseMessageEncoder};
use crate::error::{ResolutionError, ResolutionErrorKind, RouterError};
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
pub struct Dispatcher<R> {
    retry_strategy: RetryStrategy,
    router: R,
    agents: HashMap<RelativePath, Route<RawRequestMessageEncoder>>,
    downlinks: HashMap<RelativePath, Route<ResponseMessageEncoder>>,
}

impl<R> Dispatcher<R>
where
    R: Router,
{
    pub fn new(retry_strategy: RetryStrategy, router: R) -> Dispatcher<R> {
        Dispatcher {
            retry_strategy,
            router,
            agents: HashMap::default(),
            downlinks: HashMap::default(),
        }
    }

    pub fn register_downlink(&mut self, addr: RelativePath, route: RawRoute) {
        self.downlinks
            .insert(addr, route.into_framed(response_encoder()));
    }

    pub async fn dispatch(&mut self, message: TaggedMessage<'_>) -> Result<(), DispatchError> {
        let Dispatcher {
            retry_strategy,
            router,
            agents,
            downlinks,
        } = self;

        match message {
            TaggedMessage::Request(message) => {
                dispatch(
                    *retry_strategy,
                    router,
                    agents,
                    message.path.clone(),
                    message,
                    request_encoder,
                )
                .await
            }
            TaggedMessage::Response(message) => {
                dispatch(
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

fn request_encoder() -> RawRequestMessageEncoder {
    RawRequestMessageEncoder
}

fn response_encoder() -> ResponseMessageEncoder {
    ResponseMessageEncoder
}

async fn dispatch<R, E, I>(
    mut retry_strategy: RetryStrategy,
    router: &mut R,
    map: &mut HashMap<RelativePath, Route<E>>,
    target: RelativePath,
    message: I,
    encoder_fac: fn() -> E,
) -> Result<(), DispatchError>
where
    R: Router,
    E: Encoder<I, Error = IoError>,
    I: Clone,
{
    loop {
        let dispatch_result =
            try_dispatch(router, map, target.clone(), message.clone(), encoder_fac).await;
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

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("Peer sent a malformatted message")]
    Malformatted,
    #[error("{0}")]
    Io(IoError),
    #[error("{0}")]
    Resolution(ResolutionError),
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

async fn try_dispatch<R, E, I>(
    router: &mut R,
    map: &mut HashMap<RelativePath, Route<E>>,
    target: RelativePath,
    message: I,
    encoder_fac: fn() -> E,
) -> Result<(), DispatchError>
where
    R: Router,
    E: Encoder<I, Error = IoError>,
    I: Clone,
{
    let route = match map.entry(target) {
        Entry::Occupied(mut entry) => {
            if entry.get_mut().is_closed() {
                let route = try_open_route(router, entry.key().clone(), encoder_fac).await?;
                *entry.get_mut() = route;
            }
            entry.into_mut()
        }
        Entry::Vacant(entry) => {
            let route = try_open_route(router, entry.key().clone(), encoder_fac).await?;
            entry.insert(route)
        }
    };

    // If the handle returned above already existed, then it's possible that the route has timed
    // out between it being returned and here. In order to deliver the message, we need to keep a
    // copy of it in case the send operation fails as we will need to reopen the route and then send
    // it again. For now, cloning the message will suffice but a more efficient operation will need
    // to be implemented as if there is an error the message is not returned like with MPSC
    // channels.
    //
    // todo
    match route.send(message.clone()).await {
        Ok(()) => Ok(()),
        Err(e) => Err(DispatchError::Io(e)),
    }
}

impl From<ResolutionError> for DispatchError {
    fn from(e: ResolutionError) -> Self {
        DispatchError::Resolution(e)
    }
}

impl From<BadRelativeUri> for ResolutionError {
    fn from(e: BadRelativeUri) -> Self {
        ResolutionError::new(ResolutionErrorKind::Unresolvable, Some(e.to_string()))
    }
}

impl From<RouterError> for ResolutionError {
    fn from(e: RouterError) -> Self {
        ResolutionError::new(ResolutionErrorKind::Unresolvable, Some(e.to_string()))
    }
}

impl From<HeaderParseErr> for DispatchError {
    fn from(_: HeaderParseErr) -> Self {
        DispatchError::Malformatted
    }
}

async fn try_open_route<R, E>(
    router: &mut R,
    target: RelativePath,
    encoder_fac: fn() -> E,
) -> Result<Route<E>, ResolutionError>
where
    R: Router,
{
    let target_addr = router
        .lookup(None, RelativeUri::from_str(target.node.as_str())?)
        .await?;

    Ok(router
        .resolve_sender(target_addr)
        .await?
        .into_framed(encoder_fac()))
}
