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

use swimos_model::{Attr, Item, Text, Value};

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


