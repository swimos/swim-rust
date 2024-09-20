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

use futures::future::BoxFuture;
use std::collections::HashMap;
use std::marker::PhantomData;
use swimos_agent::agent_model::AgentModel;
use swimos_agent::agent_model::AgentSpec;
use swimos_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult};
use swimos_utilities::routing::RouteUri;

use crate::{Connector, ConnectorLifecycle};

/// A convenience type to register a [connector](Connector) as an agent route.
pub struct ConnectorModel<F, A> {
    connector_fac: F,
    _ty: PhantomData<A>,
}

/// Trait for factory types that create a connector.
pub trait ConnectorFactory<A> {
    type ConnectorType: Connector<A> + Send + 'static;

    fn make_connector(&self) -> Self::ConnectorType;
}

impl<F, C, A> ConnectorFactory<A> for F
where
    F: Fn() -> C,
    C: Connector<A> + Send + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        (self)()
    }
}

impl<C, A> ConnectorFactory<A> for CloneFactory<C>
where
    C: Connector<A> + Clone + Send + Sync + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        self.0.clone()
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct CloneFactory<C>(C);

impl<C, A> ConnectorModel<CloneFactory<C>, A>
where
    C: Connector<A> + Clone + Send + Sync + 'static,
{
    /// Create a connector agent model for a connector that can be cloned and shared between threads.
    pub fn new(connector: C) -> Self {
        ConnectorModel {
            connector_fac: CloneFactory(connector),
            _ty: Default::default(),
        }
    }
}

impl<F, A, C> ConnectorModel<F, A>
where
    C: Connector<A> + Send + 'static,
    F: Fn() -> C + Send + Sync + 'static,
{
    /// Create a connector agent model for a factory that will create the connector lifecycle. This
    /// is required for the case where the connector itself is not [`Sync`].
    pub fn for_fn(connector_fac: F) -> Self {
        ConnectorModel {
            connector_fac,
            _ty: Default::default(),
        }
    }
}

impl<F, A> Agent for ConnectorModel<F, A>
where
    F: ConnectorFactory<A> + Clone + Send + Sync + 'static,
    A: Default + AgentSpec + 'static,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let fac = self.connector_fac.clone();
        let agent_model = AgentModel::from_fn(A::default, move || {
            ConnectorLifecycle::new(fac.make_connector())
        });
        agent_model.run(route, route_params, config, context)
    }
}
