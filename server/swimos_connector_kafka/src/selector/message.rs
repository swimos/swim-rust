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

use std::borrow::Cow;

use swimos_model::Value;

use crate::{
    config::{ExtractionSpec, TopicSpecifier},
    selector::make_chain_selector,
    BadSelector, InvalidExtractor,
};

use super::{
    parse_raw_selector, ChainSelector, RawSelectorDescriptor, Selector, SelectorComponent,
};

pub struct MessageSelector {
    topic: TopicSelector,
    key: Option<FieldSelector>,
    payload: Option<FieldSelector>,
}

impl MessageSelector {
    pub fn select_topic<'a>(&'a self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a str> {
        let MessageSelector { topic, .. } = self;
        match topic {
            TopicSelector::Fixed(s) => Some(s.as_str()),
            TopicSelector::Selector(sel) => match sel.select(key, value) {
                Some(Value::Text(s)) => Some(s.as_str()),
                _ => None,
            },
        }
    }

    pub fn select_key<'a>(&self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a Value> {
        let MessageSelector { key: key_sel, .. } = self;
        match key_sel {
            Some(sel) => sel.select(key, value),
            None => None,
        }
    }

    pub fn select_payload<'a>(
        &self,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> Option<&'a Value> {
        let MessageSelector { payload, .. } = self;
        match payload {
            Some(sel) => sel.select(key, value),
            None => Some(value),
        }
    }
}

pub struct FieldSelector {
    part: KeyOrValue,
    selector: ChainSelector,
}

impl FieldSelector {
    pub fn select<'a>(&self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a Value> {
        let FieldSelector { part, selector } = self;
        match part {
            KeyOrValue::Key => key.and_then(|k| selector.select(k)),
            KeyOrValue::Value => selector.select(value),
        }
    }
}

pub enum KeyOrValue {
    Key,
    Value,
}

pub enum TopicSelector {
    Fixed(String),
    Selector(FieldSelector),
}

enum TopicSelectorSpec<'a> {
    Fixed(Cow<'a, str>),
    Selector(FieldSelectorSpec<'a>),
}

impl<'a> From<FieldSelectorSpec<'a>> for FieldSelector {
    fn from(value: FieldSelectorSpec<'a>) -> Self {
        let FieldSelectorSpec {
            part,
            index,
            components,
        } = value;
        FieldSelector {
            part,
            selector: make_chain_selector(index, &components),
        }
    }
}

impl<'a> From<MessageSelectorSpec<'a>> for MessageSelector {
    fn from(value: MessageSelectorSpec<'a>) -> Self {
        let MessageSelectorSpec {
            topic,
            key,
            payload,
        } = value;
        let topic = match topic {
            TopicSelectorSpec::Fixed(s) => TopicSelector::Fixed(s.to_string()),
            TopicSelectorSpec::Selector(spec) => TopicSelector::Selector(spec.into()),
        };
        MessageSelector {
            topic,
            key: key.map(Into::into),
            payload: payload.map(Into::into),
        }
    }
}

struct MessageSelectorSpec<'a> {
    topic: TopicSelectorSpec<'a>,
    key: Option<FieldSelectorSpec<'a>>,
    payload: Option<FieldSelectorSpec<'a>>,
}

struct FieldSelectorSpec<'a> {
    part: KeyOrValue,
    index: Option<usize>,
    components: Vec<SelectorComponent<'a>>,
}

impl<'a> TryFrom<RawSelectorDescriptor<'a>> for FieldSelectorSpec<'a> {
    type Error = BadSelector;

    fn try_from(value: RawSelectorDescriptor<'a>) -> Result<Self, Self::Error> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = value;
        match part {
            "$key" => Ok(FieldSelectorSpec {
                part: KeyOrValue::Key,
                index,
                components,
            }),
            "$value" => Ok(FieldSelectorSpec {
                part: KeyOrValue::Value,
                index,
                components,
            }),
            _ => Err(BadSelector::InvalidRoot),
        }
    }
}

/// Attempt to parse a field selector from a string.
fn parse_field_selector(descriptor: &str) -> Result<FieldSelectorSpec<'_>, BadSelector> {
    parse_raw_selector(descriptor)?.try_into()
}

impl MessageSelector {
    pub fn try_from_ext_spec(
        spec: &ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let spec = MessageSelectorSpec::try_from_ext_spec(spec, fixed_topic)?;
        Ok(spec.into())
    }
}

impl<'a> MessageSelectorSpec<'a> {
    fn try_from_ext_spec(
        spec: &'a ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let ExtractionSpec {
            topic_specifier,
            key_selector,
            payload_selector,
        } = spec;
        let topic = match topic_specifier {
            TopicSpecifier::Fixed => {
                if let Some(top) = fixed_topic {
                    TopicSelectorSpec::Fixed(Cow::Owned(top.to_string()))
                } else {
                    return Err(InvalidExtractor::NoTopic);
                }
            }
            TopicSpecifier::Specified(t) => TopicSelectorSpec::Fixed(Cow::Borrowed(t.as_str())),
            TopicSpecifier::Selector(s) => {
                TopicSelectorSpec::Selector(parse_field_selector(s.as_str())?)
            }
        };
        let key = key_selector
            .as_ref()
            .map(|s| parse_field_selector(s))
            .transpose()?;
        let payload = payload_selector
            .as_ref()
            .map(|s| parse_field_selector(s))
            .transpose()?;
        Ok(MessageSelectorSpec {
            topic,
            key,
            payload,
        })
    }
}