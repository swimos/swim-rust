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

use super::MessagePart;

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
    part: MessagePart,
    selector: S,
}

impl<S: Selector> RecordSelector<S> {
    
    pub fn select<'a, K, V>(&self, key: &'a mut K, value: &'a mut V) -> Option<&'a Value>
    where
        K: Deferred<'a> + 'a,
        V: Deferred<'a> + 'a {
        let RecordSelector { part, selector } = self;
        match part {
            MessagePart::Key => selector.select(key.get()),
            MessagePart::Value => selector.select(value.get()),
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
            Value::Record(attrs, _) => {
                attrs.iter().find_map(|Attr { name, value }: &Attr| {
                    if name.as_str() == select_name.as_str() {
                        Some(value)
                    } else {
                        None
                    }
                })
            },
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
        SlotSelector { select_key: Value::text(name) }
    }

}

impl Selector for SlotSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let SlotSelector { select_key } = self;
        match value {
            Value::Record(_, items) => {
                items.iter().find_map(|item: &Item| {
                    match item {
                        Item::Slot(key, value) if key == select_key => Some(value),
                        _ => None,
                    }
                })
            },
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
    Regex::new("\\A(\\$(?:key|value))(?:\\[(\\d+)])?\\z")
}

fn create_field_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\@?(?:\\w+))(?:\\[(\\d+)])?\\z")
}

pub struct SelectorComponent<'a> {
    is_attr: bool,
    name: &'a str,
    index: Option<usize>,
}

pub struct SelectorDescriptor<'a> {
    part: MessagePart,
    index: Option<usize>,
    components: Vec<SelectorComponent<'a>>,
}

impl<'a> SelectorDescriptor<'a> {

    pub fn part(&self) -> MessagePart {
        self.part
    }

    pub fn suggested_name(&self) -> Option<&'a str> {
        let SelectorDescriptor { index, components, part } = self;
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

    pub fn selector(&self) -> ChainSelector {
        let SelectorDescriptor { index, components, .. } = self;
        let mut links = vec![];
        if let Some(n) = index {
            links.push(BasicSelector::Index(IndexSelector::new(*n)));
        }
        for SelectorComponent { is_attr, name, index } in components {
            links.push(if *is_attr {
                BasicSelector::Attr(AttrSelector::new(name.to_string()))
            } else {
                BasicSelector::Slot(SlotSelector::for_field(*name))
            });
            if let Some(n) = index {
                links.push(BasicSelector::Index(IndexSelector::new(*n)));
            }
        }
        ChainSelector::new(links)
    }

}

#[derive(Clone, Copy, Error, Debug)]
pub enum BadSelector {
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    #[error("Invalid root selector (must be one of '$key' or '$value' with an optional index).")]
    InvalidRoot,
    #[error("Invalid component selector (must be an attribute or slot name with an optional index).")]
    InvalidComponent,
    #[error("An index specified was not a valid usize.")]
    IndexOutOfRange,
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}

pub fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    if (descriptor.is_empty()) {
        return Err(BadSelector::EmptySelector)
    }
    let mut it = descriptor.split('.');
    let (part, index) = match it.next() {
        Some(root) if !root.is_empty() => {
            if let Some(captures) = init_regex().captures(root) {
                let part = match captures.get(1) {
                    Some(kind) if kind.as_str() == "$key" => MessagePart::Key,
                    Some(kind) if kind.as_str() == "$value" => MessagePart::Value,
                    _ => return Err(BadSelector::InvalidRoot),
                };
                let index = if let Some(index_match) = captures.get(2) {
                    Some(index_match.as_str().parse::<usize>()?)
                } else {
                    None
                };
                (part, index)
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
            components.push(SelectorComponent { is_attr, name, index });
        } else {
            return Err(BadSelector::InvalidRoot);
        }
    }
    
    Ok(SelectorDescriptor { part, index, components })
}

#[cfg(test)]
mod tests {

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

}