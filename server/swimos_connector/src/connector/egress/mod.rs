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

use std::{collections::HashMap, error::Error, time::Duration};

use swimos_api::address::Address;
use swimos_model::Value;

use super::{BaseConnector, ConnectorFuture};

/// An egress connector is a specialized [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) that provides
/// an agent that acts as an egress point for a Swim application to some external data source.
///
/// It is intended to be used with the generic [connector agent](crate::ConnectorAgent) model type. This provides no
/// lanes, by default, but allows for them to be added dynamically by the lifecycle. The lanes that a connector
/// registers can be derived from static configuration or inferred from the external data source itself. Currently,
/// it is only possible to register dynamic lanes in the initialization phase of the agent (during the `on_start`
/// event). This restriction should be relaxed in the future.
///
/// When the connector starts, it will open some number of value and map lanes. Additionally, a number of event and
/// map-event downlinks may be opened to remote lanes on other agents. Each time a changes is made to one of the
/// lanes or an update is received on a downlinks, a message will be sent on a sender, crated by the
/// [`EgressConnector::make_sender`] method.
///
/// Note that the sender must implement [`Clone`] so that it can be shared between the agent's lanes and the
/// downlinks.
pub trait EgressConnector: BaseConnector {
    /// The type of the errors produced by the connector.
    type SendError: Error + Send + 'static;

    /// The type of the sender created by this connector.
    type Sender: EgressConnectorSender<Self::SendError> + 'static;

    /// Open the downlinks required by the connector. This is called during the agent's `on_start`
    /// event.
    ///
    /// # Arguments
    /// * `context` - The connector makes calls to the context to request the downlinks.
    fn open_downlinks(&self, context: &mut dyn EgressContext);

    /// Create sender for the connector which is used to send messages to the external data sink. This is called
    /// exactly ones during the agent's `on_start` event but must implement [`Clone`] so that copies can be passed
    /// to any downlinks that are opened.
    ///
    /// # Arguments
    /// * `agent_params` - Parameters taken from the route of the agent instance.
    fn make_sender(
        &self,
        agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::SendError>;
}

/// A reference to an egress context is passed to an [`EgressConnector`] when it starts allowing it
/// to request that downlinks be opened to remote lanes.
pub trait EgressContext {
    /// Request an event downlink to a remote lane.
    ///
    /// # Arguments
    /// * `address` - The address of the remote lane.
    fn open_event_downlink(&mut self, address: Address<String>);

    /// Request a map-event downlink to a remote lane.
    ///
    /// # Arguments
    /// * `address` - The address of the remote lane.
    fn open_map_downlink(&mut self, address: Address<String>);
}

/// Possible results of sending a message to the external sink.
pub enum SendResult<F, E> {
    /// The process of attempting to send the message can begin. When the provided future completes,
    /// the operation will have completed (either successfully, or not). The operation either may, or may
    /// not be sent regardless of whether the future is polled, depending on the implementation.
    Suspend(F),
    /// The external sink is currently busy and the message cannot be sent. A timer event should be
    /// generated after the specified delay, using the specified ID.
    RequestCallback(Duration, u64),
    /// The sender has failed and should not be used again.
    Fail(E),
}

impl<F, E: std::fmt::Debug> std::fmt::Debug for SendResult<F, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Suspend(_) => f.debug_tuple("Suspend").field(&"...").finish(),
            Self::RequestCallback(arg0, arg1) => f
                .debug_tuple("RequestCallback")
                .field(arg0)
                .field(arg1)
                .finish(),
            Self::Fail(arg0) => f.debug_tuple("Fail").field(arg0).finish(),
        }
    }
}

/// The source of a message sent by an egress connector.
#[derive(Debug, Clone, Copy)]
pub enum MessageSource<'a> {
    /// The message was generated by an update to the specified lane.
    Lane(&'a str),
    /// The message was generated by an update received on a downlink to the specified address.
    Downlink(&'a Address<String>),
}

/// A sender that can dispatch messages to an external data sink. Senders must implement [`Clone`] so that
/// copies can be produced for each downlink opened by the connector. The copies may either be handles to
/// a single shared sender or entirely independent, depending on the particular implementation.
///
/// When a message is generated by the connector, a call to [send](`EgressConnectorSender::send`) will be
/// made. This may reject the message (returning [`None`]) or produce one of three possible results:
///
/// * If the message can be dispatched, a future that will complete when the message has been processed
///   (either successfully or with an error).
/// * If the sink is a busy the sender may store the message internally and request a callback at some
///   point in the future, specifying an association ID. When the specified time has elapsed, the connector
///   will make a call to [timer_event](`EgressConnectorSender::timer_event`) with the ID. The sender may
///   then make another attempt to send the message.
/// * If the sender has failed permanently, an error can be returned after which the sender should not be
///   used again.
pub trait EgressConnectorSender<SendError>: Send + Clone {
    /// Request for a message to be dispatched.
    ///
    /// # Arguments
    /// * `source` - The lane or downlink that produced the message.
    /// * `key` - A key associated with the message.
    /// * `value` - The body of the message.
    fn send(
        &self,
        source: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> Option<SendResult<impl ConnectorFuture<SendError>, SendError>>;

    /// Called after a [RequestCallback](`SendResult::RequestCallback`) result is produced by a call to
    /// [send](`EgressConnectorSender::send`) or [timer_event](`EgressConnectorSender::timer_event`)
    /// (after the requested delay). This gives the sender another opportunity to send a message if the
    /// sink is busy.
    ///
    /// # Arguments
    /// * `timer_id` - The association ID provided in the callback request.
    fn timer_event(
        &self,
        timer_id: u64,
    ) -> Option<SendResult<impl ConnectorFuture<SendError>, SendError>>;
}
