use crate::{
    selector::{InterpretableSelector, Relay, Relays},
    BadSelector,
};
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

/// Specification of a value relay for the connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "ValueRelaySpec")]
pub struct ValueRelaySpecification {
    /// A node URI selector. See [`crate::selector::NodeSelector`] for more information.
    pub node: String,
    /// A lane URI selector. See [`crate::selector::LaneSelector`] for more information.
    pub lane: String,
    /// A payload URI selector. See [`crate::selector::RelayPayloadSelector::value`] for more information.
    pub payload: String,
    /// Whether the payload selector must yield a value. If it does not, then the selector will
    /// yield an error.
    pub required: bool,
}

/// Specification of a map relay for the connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "MapRelaySpec")]
pub struct MapRelaySpecification {
    /// A node URI selector. See [`crate::selector::NodeSelector`] for more information.
    pub node: String,
    /// A lane URI selector. See [`crate::selector::LaneSelector`] for more information.
    pub lane: String,
    /// A payload URI selector. See [`crate::selector::RelayPayloadSelector::map`] for more information.
    pub key: String,
    /// A payload URI selector. See [`crate::selector::RelayPayloadSelector::map`] for more information.
    pub value: String,
    /// Whether the payload selector must yield a value. If it does not, then the selector will
    /// yield an error.
    pub required: bool,
    /// If the value selector fails to select, then it will emit a map remove command to remove the
    /// corresponding entry.
    pub remove_when_no_value: bool,
}

/// Specification of a relay for the connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum RelaySpecification {
    /// Specification of a value relay for the connector.
    Value(ValueRelaySpecification),
    /// Specification of a map relay for the connector.
    Map(MapRelaySpecification),
}

impl<S> TryFrom<Vec<RelaySpecification>> for Relays<S>
where
    S: InterpretableSelector,
{
    type Error = BadSelector;

    fn try_from(value: Vec<RelaySpecification>) -> Result<Self, Self::Error> {
        use crate::selector::{
            parse_lane_selector, parse_map_selector, parse_node_selector, parse_value_selector,
        };

        let mut chain: Vec<Relay<S>> = Vec::with_capacity(value.len());

        for spec in value {
            match spec {
                RelaySpecification::Value(ValueRelaySpecification {
                    node,
                    lane,
                    payload,
                    required,
                }) => {
                    let relay = Relay::new(
                        parse_node_selector(node.as_str())?,
                        parse_lane_selector(lane.as_str())?,
                        parse_value_selector(payload.as_str(), required)?,
                    );
                    chain.push(relay);
                }
                RelaySpecification::Map(MapRelaySpecification {
                    node,
                    lane,
                    key,
                    value,
                    required,
                    remove_when_no_value,
                }) => {
                    let relay = Relay::new(
                        parse_node_selector(node.as_str())?,
                        parse_lane_selector(lane.as_str())?,
                        parse_map_selector(
                            key.as_str(),
                            value.as_str(),
                            required,
                            remove_when_no_value,
                        )?,
                    );
                    chain.push(relay);
                }
            }
        }

        Ok(Relays::new(chain))
    }
}
