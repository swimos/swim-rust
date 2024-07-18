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

use std::{num::ParseIntError, sync::OnceLock};

use regex::Regex;
use swimos_model::{Attr, Item, Text, Value};
use thiserror::Error;

use crate::MessagePart;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageField {
    Key,
    Value,
    Topic,
}

impl From<MessagePart> for MessageField {
    fn from(value: MessagePart) -> Self {
        match value {
            MessagePart::Key => MessageField::Key,
            MessagePart::Value => MessageField::Value,
        }
    }
}

pub trait Deferred<'a> {
    fn get(&'a mut self) -> &'a Value;
}

pub trait Selector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value>;
}

struct Immediate {
    inner: Value,
}

impl<'a> Deferred<'a> for Immediate {
    fn get(&'a mut self) -> &'a Value {
        &self.inner
    }
}

struct Computed<F> {
    inner: Option<Value>,
    f: F,
}

impl<'a, F> Deferred<'a> for Computed<F>
where
    F: Fn() -> Value,
{
    fn get(&'a mut self) -> &'a Value {
        let Computed { inner, f } = self;
        if let Some(v) = inner {
            v
        } else {
            *inner = Some(f());
            inner.as_ref().expect("Should be defined.")
        }
    }
}

struct RecordSelector<S> {
    part: MessageField,
    selector: S,
}

impl<S: Selector> RecordSelector<S> {
    pub fn select<'a, K, V>(
        &self,
        topic: &'a Value,
        key: &'a mut K,
        value: &'a mut V,
    ) -> Option<&'a Value>
    where
        K: Deferred<'a> + 'a,
        V: Deferred<'a> + 'a,
    {
        let RecordSelector { part, selector } = self;
        match part {
            MessageField::Key => selector.select(key.get()),
            MessageField::Value => selector.select(value.get()),
            MessageField::Topic => Some(topic),
        }
    }
}

pub struct IdentitySelector;

impl Selector for IdentitySelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        Some(value)
    }
}

pub struct AttrSelector {
    select_name: String,
}

impl AttrSelector {
    fn new(name: String) -> Self {
        AttrSelector { select_name: name }
    }
}

impl Selector for AttrSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let AttrSelector { select_name } = self;
        match value {
            Value::Record(attrs, _) => attrs.iter().find_map(|Attr { name, value }: &Attr| {
                if name.as_str() == select_name.as_str() {
                    Some(value)
                } else {
                    None
                }
            }),
            _ => None,
        }
    }
}

pub struct SlotSelector {
    select_key: Value,
}

impl SlotSelector {
    pub fn new(key: Value) -> Self {
        SlotSelector { select_key: key }
    }

    pub fn for_field(name: impl Into<Text>) -> Self {
        SlotSelector {
            select_key: Value::text(name),
        }
    }
}

impl Selector for SlotSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let SlotSelector { select_key } = self;
        match value {
            Value::Record(_, items) => items.iter().find_map(|item: &Item| match item {
                Item::Slot(key, value) if key == select_key => Some(value),
                _ => None,
            }),
            _ => None,
        }
    }
}

pub struct IndexSelector {
    index: usize,
}

impl IndexSelector {
    pub fn new(index: usize) -> Self {
        IndexSelector { index }
    }
}

impl Selector for IndexSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let IndexSelector { index } = self;
        match value {
            Value::Record(_, items) => items.get(*index).map(|item| match item {
                Item::ValueItem(v) => v,
                Item::Slot(_, v) => v,
            }),
            _ => None,
        }
    }
}

pub enum BasicSelector {
    Attr(AttrSelector),
    Slot(SlotSelector),
    Index(IndexSelector),
}

impl From<AttrSelector> for BasicSelector {
    fn from(value: AttrSelector) -> Self {
        BasicSelector::Attr(value)
    }
}

impl From<SlotSelector> for BasicSelector {
    fn from(value: SlotSelector) -> Self {
        BasicSelector::Slot(value)
    }
}

impl From<IndexSelector> for BasicSelector {
    fn from(value: IndexSelector) -> Self {
        BasicSelector::Index(value)
    }
}

impl Selector for BasicSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        match self {
            BasicSelector::Attr(s) => s.select(value),
            BasicSelector::Slot(s) => s.select(value),
            BasicSelector::Index(s) => s.select(value),
        }
    }
}

pub struct ChainSelector(Vec<BasicSelector>);

impl ChainSelector {
    pub fn new(selectors: Vec<BasicSelector>) -> Self {
        ChainSelector(selectors)
    }

    pub fn push(&mut self, selector: impl Into<BasicSelector>) {
        self.0.push(selector.into())
    }
}

impl Selector for ChainSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut v = Some(value);
        let ChainSelector(selectors) = self;
        for s in selectors {
            let selected = if let Some(v) = v {
                s.select(v)
            } else {
                break;
            };
            v = selected;
        }
        v
    }
}

impl Selector for Box<dyn Selector + Send> {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        (**self).select(value)
    }
}

static INIT_REGEX: OnceLock<Regex> = OnceLock::new();
static FIELD_REGEX: OnceLock<Regex> = OnceLock::new();

fn init_regex() -> &'static Regex {
    INIT_REGEX.get_or_init(|| create_init_regex().expect("Invalid regex."))
}

fn field_regex() -> &'static Regex {
    FIELD_REGEX.get_or_init(|| create_field_regex().expect("Invalid regex."))
}

fn create_init_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\$(?:key|value|topic))(?:\\[(\\d+)])?\\z")
}

fn create_field_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\@?(?:\\w+))(?:\\[(\\d+)])?\\z")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectorComponent<'a> {
    is_attr: bool,
    name: &'a str,
    index: Option<usize>,
}

impl<'a> SelectorComponent<'a> {
    pub fn new(is_attr: bool, name: &'a str, index: Option<usize>) -> Self {
        SelectorComponent {
            is_attr,
            name,
            index,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectorDescriptor<'a> {
    Part {
        part: MessagePart,
        index: Option<usize>,
        components: Vec<SelectorComponent<'a>>,
    },
    Topic,
}

impl<'a> SelectorDescriptor<'a> {
    pub fn for_part(part: MessagePart, index: Option<usize>) -> Self {
        SelectorDescriptor::Part {
            part,
            index,
            components: vec![],
        }
    }

    pub fn push(&mut self, component: SelectorComponent<'a>) {
        if let Self::Part { components, .. } = self {
            components.push(component);
        }
    }
}

impl<'a> SelectorDescriptor<'a> {
    pub fn field(&self) -> MessageField {
        match self {
            SelectorDescriptor::Part { part, .. } => (*part).into(),
            SelectorDescriptor::Topic => MessageField::Topic,
        }
    }

    pub fn suggested_name(&self) -> Option<&'a str> {
        match self {
            SelectorDescriptor::Part {
                part,
                index,
                components,
            } => {
                if let Some(SelectorComponent { name, index, .. }) = components.last() {
                    if index.is_none() {
                        Some(*name)
                    } else {
                        None
                    }
                } else if index.is_none() {
                    Some(match part {
                        MessagePart::Key => "key",
                        MessagePart::Value => "value",
                    })
                } else {
                    None
                }
            }
            SelectorDescriptor::Topic => Some("topic"),
        }
    }

    pub fn selector(&self) -> Option<ChainSelector> {
        match self {
            SelectorDescriptor::Part {
                index, components, ..
            } => {
                let mut links = vec![];
                if let Some(n) = index {
                    links.push(BasicSelector::Index(IndexSelector::new(*n)));
                }
                for SelectorComponent {
                    is_attr,
                    name,
                    index,
                } in components
                {
                    links.push(if *is_attr {
                        BasicSelector::Attr(AttrSelector::new(name.to_string()))
                    } else {
                        BasicSelector::Slot(SlotSelector::for_field(*name))
                    });
                    if let Some(n) = index {
                        links.push(BasicSelector::Index(IndexSelector::new(*n)));
                    }
                }
                Some(ChainSelector::new(links))
            }
            SelectorDescriptor::Topic => None,
        }
    }
}

#[derive(Clone, Copy, Error, Debug, PartialEq, Eq)]
pub enum BadSelector {
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    #[error("Invalid root selector (must be one of '$key' or '$value' with an optional index or '$topic').")]
    InvalidRoot,
    #[error(
        "Invalid component selector (must be an attribute or slot name with an optional index)."
    )]
    InvalidComponent,
    #[error("An index specified was not a valid usize.")]
    IndexOutOfRange,
    #[error("The topic does not have components.")]
    TopicWithComponent,
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}

pub fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    if descriptor.is_empty() {
        return Err(BadSelector::EmptySelector);
    }
    let mut it = descriptor.split('.');
    let (field, index) = match it.next() {
        Some(root) if !root.is_empty() => {
            if let Some(captures) = init_regex().captures(root) {
                let field = match captures.get(1) {
                    Some(kind) if kind.as_str() == "$key" => MessageField::Key,
                    Some(kind) if kind.as_str() == "$value" => MessageField::Value,
                    Some(kind) if kind.as_str() == "$topic" => MessageField::Topic,
                    _ => return Err(BadSelector::InvalidRoot),
                };
                let index = if let Some(index_match) = captures.get(2) {
                    if field != MessageField::Topic {
                        Some(index_match.as_str().parse::<usize>()?)
                    } else {
                        return Err(BadSelector::InvalidRoot);
                    }
                } else {
                    None
                };
                (field, index)
            } else {
                return Err(BadSelector::InvalidRoot);
            }
        }
        _ => return Err(BadSelector::EmptyComponent),
    };

    let mut components = vec![];
    for part in it {
        if part.is_empty() {
            return Err(BadSelector::EmptyComponent);
        }
        if let Some(captures) = field_regex().captures(part) {
            let (is_attr, name) = match captures.get(1) {
                Some(name) if name.as_str().starts_with('@') => (true, &name.as_str()[1..]),
                Some(name) => (false, name.as_str()),
                _ => return Err(BadSelector::InvalidComponent),
            };
            let index = if let Some(index_match) = captures.get(2) {
                Some(index_match.as_str().parse::<usize>()?)
            } else {
                None
            };
            components.push(SelectorComponent {
                is_attr,
                name,
                index,
            });
        } else {
            return Err(BadSelector::InvalidRoot);
        }
    }

    let part = match field {
        MessageField::Key => MessagePart::Key,
        MessageField::Value => MessagePart::Value,
        MessageField::Topic => {
            if components.is_empty() {
                return Ok(SelectorDescriptor::Topic);
            } else {
                return Err(BadSelector::TopicWithComponent);
            }
        }
    };

    Ok(SelectorDescriptor::Part {
        part,
        index,
        components,
    })
}

#[cfg(test)]
mod tests {
    use crate::selector::{BadSelector, SelectorComponent, SelectorDescriptor};
    use crate::MessagePart;

    #[test]
    fn init_regex_creation() {
        super::create_init_regex().expect("Creation failed.");
    }

    #[test]
    fn match_key() {
        if let Some(captures) = super::init_regex().captures("$key") {
            let kind = captures.get(1).expect("Missing capture.");
            assert!(captures.get(2).is_none());
            assert_eq!(kind.as_str(), "$key");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_value() {
        if let Some(captures) = super::init_regex().captures("$value") {
            let kind = captures.get(1).expect("Missing capture.");
            assert!(captures.get(2).is_none());
            assert_eq!(kind.as_str(), "$value");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_topic() {
        if let Some(captures) = super::init_regex().captures("$topic") {
            let kind = captures.get(1).expect("Missing capture.");
            assert!(captures.get(2).is_none());
            assert_eq!(kind.as_str(), "$topic");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_key_indexed() {
        if let Some(captures) = super::init_regex().captures("$key[3]") {
            let kind = captures.get(1).expect("Missing capture.");
            let index = captures.get(2).expect("Missing capture.");
            assert_eq!(kind.as_str(), "$key");
            assert_eq!(index.as_str(), "3");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_value_indexed() {
        if let Some(captures) = super::init_regex().captures("$value[0]") {
            let kind = captures.get(1).expect("Missing capture.");
            let index = captures.get(2).expect("Missing capture.");
            assert_eq!(kind.as_str(), "$value");
            assert_eq!(index.as_str(), "0");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_attr() {
        if let Some(captures) = super::field_regex().captures("@my_attr") {
            let name = captures.get(1).expect("Missing capture.");
            assert!(captures.get(2).is_none());
            assert_eq!(name.as_str(), "@my_attr");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_attr_indexed() {
        if let Some(captures) = super::field_regex().captures("@attr[73]") {
            let name = captures.get(1).expect("Missing capture.");
            let index = captures.get(2).expect("Missing capture.");
            assert_eq!(name.as_str(), "@attr");
            assert_eq!(index.as_str(), "73");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_slot() {
        if let Some(captures) = super::field_regex().captures("slot") {
            let name = captures.get(1).expect("Missing capture.");
            assert!(captures.get(2).is_none());
            assert_eq!(name.as_str(), "slot");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_slot_indexed() {
        if let Some(captures) = super::field_regex().captures("slot5[123]") {
            let name = captures.get(1).expect("Missing capture.");
            let index = captures.get(2).expect("Missing capture.");
            assert_eq!(name.as_str(), "slot5");
            assert_eq!(index.as_str(), "123");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn match_slot_non_latin() {
        if let Some(captures) = super::field_regex().captures("اسم[123]") {
            let name = captures.get(1).expect("Missing capture.");
            let index = captures.get(2).expect("Missing capture.");
            assert_eq!(name.as_str(), "اسم");
            assert_eq!(index.as_str(), "123");
        } else {
            panic!("Did not match.");
        }
    }

    #[test]
    fn parse_simple() {
        let key = super::parse_selector("$key").expect("Parse failed.");
        assert_eq!(key, SelectorDescriptor::for_part(MessagePart::Key, None));

        let value = super::parse_selector("$value").expect("Parse failed.");
        assert_eq!(
            value,
            SelectorDescriptor::for_part(MessagePart::Value, None)
        );

        let indexed = super::parse_selector("$key[2]").expect("Parse failed.");
        assert_eq!(
            indexed,
            SelectorDescriptor::for_part(MessagePart::Key, Some(2))
        );
    }

    #[test]
    fn parse_topic() {
        let topic = super::parse_selector("$topic").expect("Parse failed.");
        assert_eq!(topic, SelectorDescriptor::Topic);

        assert_eq!(
            super::parse_selector("$topic[0]"),
            Err(BadSelector::InvalidRoot)
        );
        assert_eq!(
            super::parse_selector("$topic.slot"),
            Err(BadSelector::TopicWithComponent)
        );
    }

    #[test]
    fn parse_one_component() {
        let first = super::parse_selector("$key.@attr").expect("Parse failed.");
        let mut expected_first = SelectorDescriptor::for_part(MessagePart::Key, None);
        expected_first.push(SelectorComponent::new(true, "attr", None));
        assert_eq!(first, expected_first);

        let second = super::parse_selector("$value.slot").expect("Parse failed.");
        let mut expected_second = SelectorDescriptor::for_part(MessagePart::Value, None);
        expected_second.push(SelectorComponent::new(false, "slot", None));
        assert_eq!(second, expected_second);

        let third = super::parse_selector("$key.@attr[3]").expect("Parse failed.");
        let mut expected_third = SelectorDescriptor::for_part(MessagePart::Key, None);
        expected_third.push(SelectorComponent::new(true, "attr", Some(3)));
        assert_eq!(third, expected_third);

        let fourth = super::parse_selector("$value[6].slot[8]").expect("Parse failed.");
        let mut expected_fourth = SelectorDescriptor::for_part(MessagePart::Value, Some(6));
        expected_fourth.push(SelectorComponent::new(false, "slot", Some(8)));
        assert_eq!(fourth, expected_fourth);
    }

    #[test]
    fn multi_component_selector() {
        let selector = super::parse_selector("$value.red.@green[7].blue").expect("Parse failed.");
        let mut expected = SelectorDescriptor::for_part(MessagePart::Value, None);
        expected.push(SelectorComponent::new(false, "red", None));
        expected.push(SelectorComponent::new(true, "green", Some(7)));
        expected.push(SelectorComponent::new(false, "blue", None));
        assert_eq!(selector, expected);
    }
}
