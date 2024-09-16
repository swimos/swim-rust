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

use crate::{
    connectors::{
        relay::handler::CommandHandler,
        relay::selector::{RecordSelectors, Selectors},
    },
    deserialization::{
        BoxMessageDeserializer, Computed, Deferred, MessageDeserializer, MessagePart, MessageView,
    },
    relay::{ConnectorHandlerContext, RelayConnectorAgent, RelayError},
};
use std::sync::Arc;
use std::vec::IntoIter;
use swimos_agent::event_handler::{EventHandler, Sequentially};
use swimos_model::Value;

/// The `Relay` trait defines a mechanism for processing a record and constructing an event handler.
/// Typically, a `Relay` will extract a subcomponent from the record and return a [`swimos_agent::event_handler::SendCommand`]
/// for forwarding the data to a lane.
///
/// It should not generally be necessary to implement this trait unless the [`AgentRelay`] is not
/// flexible enough.
pub trait Relay: Clone + Send + Sync {
    /// The type of event handler that this relay returns.
    type Handler: EventHandler<RelayConnectorAgent> + 'static;

    /// Attempt to process a record and build an event handler.
    ///
    /// # Arguments
    /// * `message` - an uninterpreted view of the components of a message; topic, key and value.
    /// * `context` - connector handler context for building event handlers.
    ///
    /// # Example
    ///
    /// ```
    /// use swimos_connector::{
    ///     deserialization::MessageView,
    ///     relay::{ConnectorHandlerContext, Relay, RelayError}
    /// };
    /// use swimos_agent::event_handler::{Discard, HandlerActionExt, SendCommand};
    /// use swimos_api::address::Address;
    /// use swimos_model::Value;
    ///
    /// #[derive(Clone)]
    /// struct ToTextRelay;
    ///
    /// impl Relay for ToTextRelay {
    ///     type Handler = Discard<SendCommand<String, Value>>;
    ///
    ///     fn on_record<'a>(
    ///         &'a self,
    ///         message: MessageView<'a>,
    ///         context: ConnectorHandlerContext,
    ///     ) -> Result<Self::Handler, RelayError> {
    ///         let MessageView {
    ///             payload,
    ///             ..
    ///         } = message;
    ///
    ///         let body = match std::str::from_utf8(payload) {
    ///             Ok(body) => Value::from(body),
    ///             Err(e) => return Err(RelayError::deserialization(e)),
    ///         };
    ///         Ok(Discard::new(SendCommand::new(Address::new(None, "node".to_string(), "lane".to_string()), body, false)))
    ///     }
    /// }
    /// ```
    fn on_record<'a>(
        &'a self,
        message: MessageView<'a>,
        context: ConnectorHandlerContext,
    ) -> Result<Self::Handler, RelayError>;
}

impl<R> Relay for &R
where
    R: Relay + Send + Sync,
{
    type Handler = R::Handler;

    fn on_record<'a>(
        &'a self,
        message: MessageView<'a>,
        context: ConnectorHandlerContext,
    ) -> Result<Self::Handler, RelayError> {
        R::on_record(self, message, context)
    }
}

/// A [`Relay`] which delegates to a function when [`Relay::on_record`] is called.
#[derive(Clone)]
pub struct FnRelay<F> {
    fun: F,
}

impl<F> FnRelay<F> {
    pub fn new(fun: F) -> FnRelay<F> {
        FnRelay { fun }
    }
}

impl<F> From<F> for FnRelay<F> {
    fn from(f: F) -> FnRelay<F> {
        FnRelay::new(f)
    }
}

impl<H, F> Relay for FnRelay<F>
where
    F: Fn(MessageView, ConnectorHandlerContext) -> Result<H, RelayError> + Clone + Send + Sync,
    H: EventHandler<RelayConnectorAgent> + 'static,
{
    type Handler = H;

    fn on_record<'a>(
        &'a self,
        message: MessageView<'a>,
        context: ConnectorHandlerContext,
    ) -> Result<Self::Handler, RelayError> {
        (self.fun)(message, context)
    }
}

struct Inner {
    selectors: RecordSelectors,
    key: BoxMessageDeserializer,
    value: BoxMessageDeserializer,
}

/// A configurable [`Relay`] implementation which uses a family of [`RecordSelectors`] for building
/// commands for sending to agents.
///
/// This [`Relay`] will lazily deserialize records that it receives only when the [`RecordSelectors`]
/// require access to either the key or payload of a record.
#[derive(Clone)]
pub struct AgentRelay {
    inner: Arc<Inner>,
}

impl AgentRelay {
    /// Builds a new [`AgentRelay`].
    ///
    /// # Arguments
    /// * `selectors` - a collection of selectors that will be used to build a number of commands to
    ///   send to agents.
    /// * `key` - the key deserializer.
    /// * `value` - the value deserializer.
    ///
    /// # Example
    ///
    /// ```
    /// use swimos_connector::{
    ///     deserialization::{MessageDeserializer, MessagePart, MessageView},
    ///     relay::{
    ///         AgentRelay, LaneSelector, NodeSelector, PayloadSelector, RecordSelectors,
    ///     Selectors,
    ///     }
    /// };
    /// # use std::error::Error;
    /// use std::str::{FromStr, Utf8Error};
    /// use swimos_model::Value;
    ///
    /// # fn main() -> Result<(), Box<dyn Error + 'static>> {
    /// #[derive(Copy, Clone)]
    /// struct Text;
    ///
    /// impl MessageDeserializer for Text {
    ///    type Error = Utf8Error;
    ///
    ///    fn deserialize<'a>(
    ///        &'a self,
    ///        message: &'a MessageView<'a>,
    ///        part: MessagePart,
    ///    ) -> Result<Value, Self::Error> {
    ///        let buf = match part {
    ///            MessagePart::Key => message.key,
    ///            MessagePart::Payload => message.payload,
    ///        };
    ///
    ///        Ok(Value::from(std::str::from_utf8(buf)?))
    ///     }
    ///  }
    ///
    /// let node = NodeSelector::from_str("/nodes/$key")?;
    /// let lane = LaneSelector::from_str("name")?;
    /// let payload = PayloadSelector::value("$value.temperature", true)?;
    /// let selectors = RecordSelectors::from(Selectors::new(node, lane, payload));
    ///
    /// AgentRelay::new(selectors, Text, Text);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<S, K, V>(selectors: S, key: K, value: V) -> AgentRelay
    where
        S: Into<RecordSelectors>,
        K: MessageDeserializer + Send + Sync + 'static,
        V: MessageDeserializer + Send + Sync + 'static,
        K::Error: Send + 'static,
        V::Error: Send + 'static,
    {
        AgentRelay {
            inner: Arc::new(Inner {
                selectors: selectors.into(),
                key: key.boxed(),
                value: value.boxed(),
            }),
        }
    }
}

impl Relay for AgentRelay {
    type Handler = Sequentially<IntoIter<CommandHandler>, CommandHandler>;

    fn on_record<'a>(
        &'a self,
        message: MessageView<'a>,
        _context: ConnectorHandlerContext,
    ) -> Result<Self::Handler, RelayError> {
        let Inner {
            selectors,
            key,
            value,
        } = self.inner.as_ref();

        let mut key = Computed::new(|| key.deserialize(&message, MessagePart::Key));
        let mut value = Computed::new(|| value.deserialize(&message, MessagePart::Payload));
        let topic = Value::text(message.topic());

        selectors
            .into_iter()
            .try_fold(Vec::new(), |mut handlers, selectors| {
                match on_record(selectors, &topic, &mut key, &mut value) {
                    Ok(Some(handler)) => {
                        handlers.push(handler);
                        Ok(handlers)
                    }
                    Ok(None) => Ok(handlers),
                    Err(e) => Err(e),
                }
            })
            .map(Sequentially::new)
    }
}

fn on_record<K, V>(
    selectors: &Selectors,
    topic: &Value,
    key: &mut K,
    value: &mut V,
) -> Result<Option<CommandHandler>, RelayError>
where
    K: Deferred,
    V: Deferred,
{
    selectors.with(|node, lane, payload| {
        let node_uri = node.select(key, value, topic)?;
        let lane_uri = lane.select(key, value, topic)?;
        payload.select(node_uri, lane_uri, key, value, topic)
    })
}
