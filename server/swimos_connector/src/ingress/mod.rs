mod lanes;
#[cfg(feature = "pubsub")]
pub mod pubsub;
#[cfg(test)]
mod tests;
pub use lanes::*;

//
// use crate::deser::{BoxMessageDeserializer, Deferred, MessageView};
// use crate::selector::{
//     InvalidLanes, PubSubMapLaneSelector, PubSubSelector, PubSubValueLaneSelector, Relays,
//     SelectHandler, SelectorError,
// };
// use crate::ConnectorAgent;
// use frunk::hlist;
// use std::collections::HashSet;
// use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
// use swimos_model::Value;
// use tracing::trace;
//
// // Uses the information about the lanes of the agent to convert messages into event handlers that update the lanes.
// pub struct MessageSelector {
//     key_deserializer: BoxMessageDeserializer,
//     value_deserializer: BoxMessageDeserializer,
//     lanes: Lanes,
//     relays: Relays<PubSubSelector>,
// }
//
// impl MessageSelector {
//     pub fn new(
//         key_deserializer: BoxMessageDeserializer,
//         value_deserializer: BoxMessageDeserializer,
//         lanes: Lanes,
//         relays: Relays<PubSubSelector>,
//     ) -> Self {
//         MessageSelector {
//             key_deserializer,
//             value_deserializer,
//             lanes,
//             relays,
//         }
//     }
//
//     pub fn handle_message<'a>(
//         &self,
//         message: &'a MessageView<'a>,
//     ) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, SelectorError> {
//         let MessageSelector {
//             key_deserializer,
//             value_deserializer,
//             lanes,
//             relays,
//         } = self;
//
//         let value_lanes = lanes.value_lanes();
//         let map_lanes = lanes.map_lanes();
//
//         trace!(topic = { message.topic() }, "Handling a message.");
//
//         let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
//         let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
//         let mut relay_handlers = Vec::with_capacity(relays.len());
//
//         {
//             let topic = Value::text(message.topic());
//             let key = Deferred::new(message.key, key_deserializer);
//             let value = Deferred::new(message.payload, value_deserializer);
//             let mut args = hlist![topic, key, value];
//
//             for value_lane in value_lanes {
//                 value_lane_handlers.push(value_lane.select_handler(&mut args)?);
//             }
//             for map_lane in map_lanes {
//                 map_lane_handlers.push(map_lane.select_handler(&mut args)?);
//             }
//             for relay in relays {
//                 relay_handlers.push(relay.select_handler(&mut args)?);
//             }
//         }
//
//         let handler = Sequentially::new(value_lane_handlers)
//             .followed_by(Sequentially::new(map_lane_handlers))
//             .followed_by(Sequentially::new(relay_handlers));
//         Ok(handler)
//     }
// }
