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

use futures::Stream;
use swimos_agent::event_handler::EventHandler;

use crate::generic::GenericConnectorAgent;

pub trait Connector {

    type ConnectorData;

    fn connector_stream(&mut self) -> impl Stream<Item = Self::ConnectorData> + Send + 'static;

    fn on_start(&mut self) -> impl EventHandler<GenericConnectorAgent> + '_;
    fn on_stop(&mut self) -> impl EventHandler<GenericConnectorAgent> + '_;
    fn on_data(&mut self, data: Self::ConnectorData) -> impl EventHandler<GenericConnectorAgent> + '_;

}
