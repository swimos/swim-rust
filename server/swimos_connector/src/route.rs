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

use std::collections::HashMap;

use futures::future::BoxFuture;
use swimos_agent::agent_model::AgentModel;
use swimos_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult};
use swimos_utilities::routing::RouteUri;

use crate::{
    BaseConnector, ConnectorAgent, EgressConnector, EgressConnectorLifecycle, IngressConnector,
    IngressConnectorLifecycle,
};

/// A convenience type to register a [connector](IngressConnector) as an agent route.
pub struct IngressConnectorModel<F> {
    connector_fac: F,
}

/// A convenience type to register a [connector](EgressConnector) as an agent route.
pub struct EgressConnectorModel<F> {
    connector_fac: F,
}

/// Trait for factory types that create a connector.
pub trait ConnectorFactory {
    type ConnectorType: BaseConnector + Send + 'static;

    fn make_connector(&self) -> Self::ConnectorType;
}

impl<F, C> ConnectorFactory for F
where
    F: Fn() -> C,
    C: BaseConnector + Send + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        (self)()
    }
}

impl<C> ConnectorFactory for CloneFactory<C>
where
    C: BaseConnector + Clone + Send + Sync + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        self.0.clone()
    }
}

#[doc(hidden)]
pub struct CloneFactory<C>(C);

impl<C> IngressConnectorModel<CloneFactory<C>>
where
    C: IngressConnector + Clone + Send + Sync + 'static,
{
    /// Create a connector agent model for a connector that can be cloned and shared between threads.
    pub fn new(connector: C) -> Self {
        IngressConnectorModel {
            connector_fac: CloneFactory(connector),
        }
    }
}

impl<C> EgressConnectorModel<CloneFactory<C>>
where
    C: EgressConnector + Clone + Send + Sync + 'static,
{
    /// Create a connector agent model for a connector that can be cloned and shared between threads.
    pub fn new(connector: C) -> Self {
        EgressConnectorModel {
            connector_fac: CloneFactory(connector),
        }
    }
}

impl<F, C> IngressConnectorModel<F>
where
    C: IngressConnector + Send + 'static,
    F: Fn() -> C + Send + Sync + 'static,
{
    /// Create a connector agent model for a factory that will create the connector lifecycle. This
    /// is required for the case where the connector itself is not [`Sync`].
    pub fn for_fn(connector_fac: F) -> Self {
        IngressConnectorModel { connector_fac }
    }
}

impl<F, C> EgressConnectorModel<F>
where
    C: EgressConnector + Send + 'static,
    F: Fn() -> C + Send + Sync + 'static,
{
    /// Create a connector agent model for a factory that will create the connector lifecycle. This
    /// is required for the case where the connector itself is not [`Sync`].
    pub fn for_fn(connector_fac: F) -> Self {
        EgressConnectorModel { connector_fac }
    }
}

impl<F> Agent for IngressConnectorModel<F>
where
    F: ConnectorFactory + Clone + Send + Sync + 'static,
    F::ConnectorType: IngressConnector,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let fac = self.connector_fac.clone();
        let agent_model = AgentModel::from_fn(ConnectorAgent::default, move || {
            IngressConnectorLifecycle::new(fac.make_connector())
        });
        agent_model.run(route, route_params, config, context)
    }
}

impl<F> Agent for EgressConnectorModel<F>
where
    F: ConnectorFactory + Clone + Send + Sync + 'static,
    F::ConnectorType: EgressConnector,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let fac = self.connector_fac.clone();
        let agent_model = AgentModel::from_fn(ConnectorAgent::default, move || {
            EgressConnectorLifecycle::new(fac.make_connector())
        });
        agent_model.run(route, route_params, config, context)
    }
}
