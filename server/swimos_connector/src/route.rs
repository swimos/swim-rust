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

use crate::{Connector, ConnectorAgent, ConnectorLifecycle};

/// A convenience type to register a [connector](Connector) as an agent route.
pub struct ConnectorModel<F> {
    connector_fac: F,
}

pub trait ConnectorFactory {
    type ConnectorType: Connector + Send + 'static;

    fn make_connector(&self) -> Self::ConnectorType;
}

impl<F, C> ConnectorFactory for F
where
    F: Fn() -> C,
    C: Connector + Send + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        (self)()
    }
}

impl<C> ConnectorFactory for CloneFactory<C>
where
    C: Connector + Clone + Send + Sync + 'static,
{
    type ConnectorType = C;

    fn make_connector(&self) -> Self::ConnectorType {
        self.0.clone()
    }
}

#[doc(hidden)]
pub struct CloneFactory<C>(C);

impl<C> ConnectorModel<CloneFactory<C>>
where
    C: Connector + Clone + Send + Sync + 'static,
{
    pub fn new(connector: C) -> Self {
        ConnectorModel {
            connector_fac: CloneFactory(connector),
        }
    }
}

impl<F, C> ConnectorModel<F>
where
    C: Connector + Send + 'static,
    F: Fn() -> C + Send + Sync + 'static,
{
    pub fn for_fn(connector_fac: F) -> Self {
        ConnectorModel { connector_fac }
    }
}

impl<F> Agent for ConnectorModel<F>
where
    F: ConnectorFactory + Clone + Send + Sync + 'static,
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
            ConnectorLifecycle::new(fac.make_connector())
        });
        agent_model.run(route, route_params, config, context)
    }
}
