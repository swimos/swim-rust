// use crate::{LaneSelector, NodeSelector, ParseError, PayloadSelector, Relay, Relays};
use swimos_form::Form;

/// Specification of a value lane for the connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "ValueLaneSpec")]
pub struct IngressValueLaneSpec {
    /// A name to use for the lane. If not specified, the connector will attempt to infer one from the selector.
    pub name: Option<String>,
    /// String representation of a selector to extract values for the lane from messages.
    pub selector: String,
    /// Whether the lane is required. If this is `true` and the selector returns nothing for a Message, the
    /// connector will fail with an error.
    pub required: bool,
}

impl IngressValueLaneSpec {
    /// # Arguments
    /// * `name` - A name to use for the lane. If not specified the connector will attempt to infer a name from the selector.
    /// * `selector` - String representation of the selector to extract values from the message.
    /// * `required` - Whether the lane is required. If this is `true` and the selector returns nothing for a Message, the
    ///   connector will fail with an error.
    pub fn new<S: Into<String>>(name: Option<S>, selector: S, required: bool) -> Self {
        IngressValueLaneSpec {
            name: name.map(Into::into),
            selector: selector.into(),
            required,
        }
    }
}

/// Specification of a value lane for the connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "MapLaneSpec")]
pub struct IngressMapLaneSpec {
    /// The name of the lane.
    pub name: String,
    /// String representation of a selector to extract the map keys from the messages.
    pub key_selector: String,
    /// String representation of a selector to extract the map values from the messages.
    pub value_selector: String,
    /// Whether to remove an entry from the map if the value selector does not return a value. Otherwise, missing
    /// values will be treated as a failed extraction from the message.
    pub remove_when_no_value: bool,
    /// Whether the lane is required. If this is `true` and the selector returns nothing for a Message, the
    /// connector will fail with an error.
    pub required: bool,
}

impl IngressMapLaneSpec {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `key_selector` - String representation of a selector to extract the map keys from the messages.
    /// * `value_selector` - String representation of a selector to extract the map values from the messages.
    /// * `remove_when_no_value` - Whether to remove an entry from the map if the value selector does not return a value. Otherwise, missing
    ///   values will be treated as a failed extraction from the message.
    /// * `required` - Whether the lane is required. If this is `true` and the selector returns nothing for a Message, the
    ///   connector will fail with an error.
    pub fn new<S: Into<String>>(
        name: S,
        key_selector: S,
        value_selector: S,
        remove_when_no_value: bool,
        required: bool,
    ) -> Self {
        IngressMapLaneSpec {
            name: name.into(),
            key_selector: key_selector.into(),
            value_selector: value_selector.into(),
            remove_when_no_value,
            required,
        }
    }
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "ValueRelaySpec")]
pub struct ValueRelaySpecification {
    pub node: String,
    pub lane: String,
    pub payload: String,
    pub required: bool,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "MapRelaySpec")]
pub struct MapRelaySpecification {
    pub node: String,
    pub lane: String,
    pub key: String,
    pub value: String,
    pub required: bool,
    pub remove_when_no_value: bool,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum RelaySpecification {
    Value(ValueRelaySpecification),
    Map(MapRelaySpecification),
}
//
// impl TryFrom<Vec<RelaySpecification>> for Relays {
//     type Error = ParseError;
//
//     fn try_from(value: Vec<RelaySpecification>) -> Result<Self, Self::Error> {
//         let mut chain = Vec::with_capacity(value.len());
//
//         for spec in value {
//             match spec {
//                 RelaySpecification::Value(ValueRelaySpecification {
//                     node,
//                     lane,
//                     payload,
//                     required,
//                 }) => {
//                     let node = NodeSelector::from_str(node.as_str())?;
//                     let lane = LaneSelector::from_str(lane.as_str())?;
//                     let payload = PayloadSelector::value(payload.as_str(), required)?;
//
//                     chain.push(Relay::new(node, lane, payload));
//                 }
//                 RelaySpecification::Map(MapRelaySpecification {
//                     node,
//                     lane,
//                     key,
//                     value,
//                     required,
//                     remove_when_no_value,
//                 }) => {
//                     let node = NodeSelector::from_str(node.as_str())?;
//                     let lane = LaneSelector::from_str(lane.as_str())?;
//                     let payload = PayloadSelector::map(
//                         key.as_str(),
//                         value.as_str(),
//                         required,
//                         remove_when_no_value,
//                     )?;
//
//                     chain.push(Relay::new(node, lane, payload));
//                 }
//             }
//         }
//
//         Ok(Relays::new(chain))
//     }
// }
