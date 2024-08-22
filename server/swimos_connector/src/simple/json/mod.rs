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

#[cfg(test)]
mod tests;

mod selector;

use crate::{
    simple::{
        json::selector::{
            BasicSelector, LaneSelectorPattern, NodeSelectorPattern, Part, PayloadSelectorPattern,
            Selectors,
        },
        selector::{CompiledSelector, Selector},
        ConnectorHandlerContext, RelayError, SimpleConnectorHandler,
    },
    Computed, Deferred, DeserializationError,
};
use serde_json::Value;
use std::fmt::Debug;
use swimos_agent::event_handler::{ConstHandler, Either, HandlerActionExt};
use swimos_model::Item;

fn deferred_deserializer<'r>(input: impl AsRef<[u8]> + 'r) -> impl Deferred<Out=Value> + 'r {
    Computed::new(move || serde_json::from_slice(input.as_ref()).map_err(DeserializationError::new))
}

#[derive(Debug, Clone)]
pub struct JsonRelay {
    selectors: Selectors,
}

impl JsonRelay {
    pub fn new(
        node: NodeSelectorPattern,
        lane: LaneSelectorPattern,
        payload: PayloadSelectorPattern,
    ) -> JsonRelay {
        JsonRelay {
            selectors: Selectors::new(node, lane, payload),
        }
    }

    pub fn on_record<'d>(
        &self,
        key: &'d [u8],
        value: &'d [u8],
        context: ConnectorHandlerContext,
    ) -> Result<impl SimpleConnectorHandler + 'd, RelayError> {
        on_record(
            &self.selectors,
            deferred_deserializer(key),
            deferred_deserializer(value),
            context,
        )
    }
}

enum SelectorPart<'l> {
    Static(&'l str),
    KeySelector(CompiledSelector<BasicSelector<'l>>),
    ValueSelector(CompiledSelector<BasicSelector<'l>>),
}

fn selector_iter<'a, I>(iter: I) -> impl Iterator<Item=SelectorPart<'a>>
where
    I: Iterator<Item=Part<'a>>,
{
    iter.map(|item| match item {
        Part::Static(p) => SelectorPart::Static(p),
        Part::KeySelector(selectors) => SelectorPart::KeySelector(CompiledSelector::chain(
            selectors.into_iter().map(BasicSelector::field),
        )),
        Part::ValueSelector(selectors) => SelectorPart::ValueSelector(CompiledSelector::chain(
            selectors.into_iter().map(BasicSelector::field),
        )),
    })
}

fn build_uri<'a, I, K, V>(
    pattern: I,
    raw_key: &mut K,
    raw_value: &mut V,
) -> Result<String, RelayError>
where
    I: IntoIterator<Item=Part<'a>>,
    K: Deferred<Out=Value>,
    V: Deferred<Out=Value>,
{
    let iter = selector_iter(pattern.into_iter());
    let mut node_uri = String::new();

    for elem in iter {
        match elem {
            SelectorPart::Static(p) => node_uri.push_str(p),
            SelectorPart::KeySelector(s) => {
                let key = raw_key.get()?;
                match s.select(key) {
                    Some(value) => accumulate_into(&mut node_uri, value)?,
                    None => return Err(RelayError::MissingField(format!("$key{}", s.name()))),
                }
            }
            SelectorPart::ValueSelector(s) => {
                let value = raw_value.get()?;
                match s.select(value) {
                    Some(value) => accumulate_into(&mut node_uri, value)?,
                    None => return Err(RelayError::MissingField(format!("$value{}", s.name()))),
                }
            }
        }
    }

    Ok(node_uri)
}

fn on_record<K, V>(
    selectors: &Selectors,
    mut raw_key: K,
    mut raw_value: V,
    context: ConnectorHandlerContext,
) -> Result<impl SimpleConnectorHandler, RelayError>
where
    K: Deferred<Out=Value>,
    V: Deferred<Out=Value>,
{
    let node_uri = build_uri(selectors.node(), &mut raw_key, &mut raw_value)?;
    let lane_uri = build_uri(selectors.lane(), &mut raw_key, &mut raw_value)?;
    let payload = get_payload(selectors.payload(), raw_key, raw_value)?;

    let handler = match payload {
        Some(payload) => Either::Left(context.send_command(None, node_uri, lane_uri, payload)),
        None => Either::Right(ConstHandler::default()),
    };

    Ok(handler.discard())
}

fn get_payload<K, V>(
    selector: &PayloadSelectorPattern,
    raw_key: K,
    raw_value: V,
) -> Result<Option<swimos_model::Value>, RelayError>
where
    K: Deferred<Out=Value>,
    V: Deferred<Out=Value>,
{
    match selector.as_part() {
        Part::Static(path) => Ok(Some(swimos_model::Value::from(path.to_string()))),
        Part::KeySelector(selectors) => {
            let selector = CompiledSelector::chain(selectors.into_iter().map(BasicSelector::field));
            Ok(selector
                .select_owned(raw_key.take()?)
                .map(convert_json_value))
        }
        Part::ValueSelector(selectors) => {
            let selector = CompiledSelector::chain(selectors.into_iter().map(BasicSelector::field));
            Ok(selector
                .select_owned(raw_value.take()?)
                .map(convert_json_value))
        }
    }
}

fn accumulate_into(output: &mut String, input: &Value) -> Result<(), RelayError> {
    match input {
        Value::Null => {}
        Value::Bool(v) => output.push_str(if *v { "true" } else { "false" }),
        Value::Number(v) => output.push_str(&v.to_string()),
        Value::String(v) => output.push_str(v),
        Value::Array(_) | Value::Object(_) => {
            return Err(RelayError::InvalidRecord(input.to_string()))
        }
    }
    Ok(())
}

fn convert_json_value(input: Value) -> swimos_model::Value {
    match input {
        Value::Null => swimos_model::Value::Extant,
        Value::Bool(p) => swimos_model::Value::BooleanValue(p),
        Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                swimos_model::Value::UInt64Value(i)
            } else if let Some(i) = n.as_i64() {
                swimos_model::Value::Int64Value(i)
            } else {
                swimos_model::Value::Float64Value(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => swimos_model::Value::Text(s.into()),
        Value::Array(arr) => swimos_model::Value::record(
            arr.into_iter()
                .map(|v| Item::ValueItem(convert_json_value(v)))
                .collect(),
        ),
        Value::Object(obj) => swimos_model::Value::record(
            obj.into_iter()
                .map(|(k, v)| {
                    Item::Slot(swimos_model::Value::Text(k.into()), convert_json_value(v))
                })
                .collect(),
        ),
    }
}
