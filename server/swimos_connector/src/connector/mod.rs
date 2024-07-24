// Copyright 2015-2024 Swim Inc.
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

use std::error::Error;

use futures::{TryStream, TryStreamExt};
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt, TryHandlerActionExt},
};
use swimos_utilities::trigger;

use crate::generic::ConnectorAgent;

pub trait Connector {
    type StreamError: Error + Send + 'static;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::StreamError>, Self::StreamError>;

    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_;

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_;
}

pub trait ConnectorHandler: EventHandler<ConnectorAgent> + Send + 'static {}

impl<H> ConnectorHandler for H where H: EventHandler<ConnectorAgent> + Send + 'static {}

pub trait ConnectorStream<E>:
    TryStream<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}

impl<S, E> ConnectorStream<E> for S where
    S: TryStream<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}

pub fn suspend_connector<E, C>(
    mut next: C,
) -> impl HandlerAction<ConnectorAgent, Completion = ()> + Send + 'static
where
    C: ConnectorStream<E> + Send + Unpin + 'static,
    E: std::error::Error + Send + 'static,
{
    let context: HandlerContext<ConnectorAgent> = HandlerContext::default();
    let fut = async move {
        let maybe_result = next.try_next().await.transpose();
        maybe_result
            .map(move |result| {
                let h = context.value(result).try_handler();
                h.followed_by(suspend_connector(next))
            })
            .discard()
            .boxed_local()
    };
    context.suspend(fut)
}
