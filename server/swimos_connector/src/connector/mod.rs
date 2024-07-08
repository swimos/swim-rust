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

use futures::{Future, FutureExt};
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt, TryHandlerActionExt},
};

use crate::generic::GenericConnectorAgent;

pub trait Connector {
    type StreamError: Error + Send + 'static;
    type ConnectorState: ConnectorNext<Self::StreamError> + Send + 'static;

    fn create_state(&self) -> Result<Self::ConnectorState, Self::StreamError>;

    fn on_start(&self) -> impl EventHandler<GenericConnectorAgent> + '_;
    fn on_stop(&self) -> impl EventHandler<GenericConnectorAgent> + '_;
}

pub trait ConnectorNext<Err>: Sized {
    fn next_state(
        self,
    ) -> impl Future<
        Output: HandlerAction<GenericConnectorAgent, Completion = Option<Result<Self, Err>>>
                    + Send
                    + 'static,
    > + Send
           + 'static;

    fn commit(
        self,
    ) -> Result<
        impl Future<
                Output: HandlerAction<GenericConnectorAgent, Completion = Result<Self, Err>>
                            + Send
                            + 'static,
            > + Send
            + 'static,
        Self,
    >;
}

pub fn suspend_connector<E, C>(
    next: C,
) -> impl HandlerAction<GenericConnectorAgent, Completion = ()> + Send + 'static
where
    C: ConnectorNext<E> + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    let context: HandlerContext<GenericConnectorAgent> = HandlerContext::default();
    context.suspend(next.next_state().map(move |handler| {
        handler
            .map(|opt: Option<_>| opt.transpose())
            .try_handler()
            .and_then(|maybe_connector: Option<_>| maybe_connector.map(suspend_connector))
            .discard()
            .boxed_local()
    }))
}
