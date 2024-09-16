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

#[cfg(test)]
mod tests;

pub mod generic;
pub mod relay;

use std::error::Error;

use futures::{TryStream, TryStreamExt};
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt, TryHandlerActionExt},
};
use swimos_utilities::trigger;
use thiserror::Error;

/// An error type that is produced by the [`crate::ConnectorLifecycle`] if the [`crate::Connector`] that it wraps
/// fails to complete the initialization phase correctly.
#[derive(Clone, Copy, Default, Debug, Error)]
#[error("The connector initialization failed to complete.")]
pub struct ConnectorInitError;

/// A connector is a specialized [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) that provides an
/// agent that acts as an ingress point for a Swim application for some external data source.
///
/// This provides no lanes, by default, but allows for them to be added dynamically by the lifecycle. The lanes that a connector
/// registers can be derived from static configuration or inferred from the external data source itself. Currently,
/// it is only possible to register dynamic lanes in the initialization phase of the agent (during the `on_start`
/// event). This restriction should be relaxed in the future.
///
/// The core of a connector is the [create_stream](Connector::create_stream) method that creates a fallible
/// stream that consumes events from the external data source and converts them into [event handlers](EventHandler)
/// that modify the state of the agent. This stream is suspended into the agents task and will be polled repeatedly
/// until it either terminates (or fails) that will cause the connector agent to stop.
pub trait Connector<A> {
    /// The type of the errors produced by the connector stream.
    type StreamError: Error + Send + 'static;

    /// Create an asynchronous stream that consumes events from the external data source and produces [event handlers](EventHandler)
    /// from them which modify the state of the agent.
    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<A, Self::StreamError>, Self::StreamError>;

    /// Initialize the connector. All required lanes should be created by this handler.
    ///
    /// # Arguments
    /// * `init_complete` - The provided handler must trigger this when the initialization is complete. The connector stream will only
    ///   be started after this happens. If this is dropped, the connector will fail with an error.
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<A> + '_;

    /// This event handler will be executed before the connector stops (unless if fails with an error). This should be
    /// used to perform any required clean-up.
    fn on_stop(&self) -> impl EventHandler<A> + '_;
}

/// A trait for fallible streams of event handlers that are returned by a [`Connector`].
pub trait ConnectorStream<A, E>:
    TryStream<Ok: EventHandler<A>, Error = E> + Send + Unpin + 'static
{
}

impl<A, S, E> ConnectorStream<A, E> for S where
    S: TryStream<Ok: EventHandler<A>, Error = E> + Send + Unpin + 'static
{
}

/// Suspend a connector stream into the agent task. The stream will be polled, and the event handlers it
/// returns executed, until it either ends of fails with an error.
///
/// # Arguments
///
/// * `next` - The connector stream.
pub fn suspend_connector<A, E, C>(
    mut next: C,
) -> impl HandlerAction<A, Completion = ()> + Send + 'static
where
    C: ConnectorStream<A, E> + Send + Unpin + 'static,
    E: std::error::Error + Send + 'static,
    A: 'static,
{
    let context: HandlerContext<A> = HandlerContext::default();
    let fut = async move {
        let maybe_result = next.try_next().await.transpose();
        maybe_result
            .map(move |result| {
                context
                    .value(result)
                    .try_handler()
                    .and_then(|h: C::Ok| h.followed_by(suspend_connector(next)).boxed_local())
            })
            .discard()
    };
    context.suspend(fut)
}
