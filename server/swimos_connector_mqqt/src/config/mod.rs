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

use swimos_form::Form;

/// Configuration parameters for the MQTT ingress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "mqtt")]
pub struct MqttIngressConfiguration {
    /// The MQTT connection Url.
    pub url: String,
    /// Specifications for the value lanes to define for the connector.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    /// The MQTT topics to subscribe to.
    pub subscription: Subscription,
    pub keep_alive_secs: Option<u64>,
    pub max_packet_size: Option<usize>,
    pub client_channel_size: Option<usize>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct IngressValueLaneSpec {
    pub name: String,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct IngressMapLaneSpec {
    pub name: String,
}

/// Configuration parameters for the MQTT egress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "mqtt")]
pub struct MqttEgressConfiguration {
    /// The MQTT connection Url.
    pub url: String,
    /// The MQTT topic.
    pub topic: String,
    /// Descriptors for the value lanes of the connector agent and how to extract messages
    /// from them to send to the egress sink.
    pub value_lanes: Vec<EgressLaneSpec>,
    /// Descriptors for the map lanes of the connector agent and how to extract messages
    /// from them to send to the egress sink.
    pub map_lanes: Vec<EgressLaneSpec>,
    /// Descriptors for the value downlinks (to remote lanes) of the connector agent and how to extract messages
    /// from the received events to send to the egress sink.
    pub value_downlinks: Vec<EgressDownlinkSpec>,
    /// Descriptors for the map downlinks (to remote lanes) of the connector agent and how to extract messages
    /// from the received events to send to the egress sink.
    pub map_downlinks: Vec<EgressDownlinkSpec>,
    pub keep_alive_secs: Option<u64>,
    pub max_packet_size: Option<usize>,
    pub max_inflight: Option<u32>,
    pub client_channel_size: Option<usize>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct EgressLaneSpec {
    pub name: String,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct EgressDownlinkSpec {
    //pub address: Address<String>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum Subscription {
    #[form(tag = "topic")]
    Topic(#[form(header_body)] String),
    #[form(tag = "topics")]
    Topics(#[form(header_body)] Vec<String>),
    #[form(tag = "filter")]
    Filters(#[form(header_body)] Vec<String>),
}
