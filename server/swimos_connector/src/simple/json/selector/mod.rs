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

mod common;
mod lane;
mod node;
mod payload;

use crate::simple::selector::Selector;
pub use common::{ParseError, Part, SelectorPatternIter};
pub use lane::LaneSelectorPattern;
pub use node::NodeSelectorPattern;
pub use payload::PayloadSelectorPattern;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Selectors {
    inner: Arc<Inner>,
}

impl Selectors {
    pub fn new(
        node: NodeSelectorPattern,
        lane: LaneSelectorPattern,
        payload: PayloadSelectorPattern,
    ) -> Selectors {
        Selectors {
            inner: Arc::new(Inner {
                node,
                lane,
                payload,
            }),
        }
    }

    pub fn node(&self) -> &NodeSelectorPattern {
        &self.inner.as_ref().node
    }

    pub fn lane(&self) -> &LaneSelectorPattern {
        &self.inner.as_ref().lane
    }

    pub fn payload(&self) -> &PayloadSelectorPattern {
        &self.inner.as_ref().payload
    }
}

#[derive(Debug)]
struct Inner {
    node: NodeSelectorPattern,
    lane: LaneSelectorPattern,
    payload: PayloadSelectorPattern,
}

pub struct FieldSelector<'l> {
    name: &'l str,
}

impl<'l> FieldSelector<'l> {
    pub fn new(name: &'l str) -> FieldSelector<'l> {
        FieldSelector { name }
    }
}

impl<'l> Selector for FieldSelector<'l> {
    type Value = Value;

    fn name(&self) -> String {
        self.name.to_string()
    }

    fn select<'a>(&self, value: &'a Self::Value) -> Option<&'a Self::Value> {
        let FieldSelector { name } = self;
        match value {
            Value::Object(obj) => obj.get(*name),
            _ => None,
        }
    }

    fn select_owned<'a>(&self, value: Self::Value) -> Option<Self::Value> {
        let FieldSelector { name } = self;
        match value {
            Value::Object(mut obj) => obj.remove(*name),
            _ => None,
        }
    }
}

pub enum BasicSelector<'l> {
    Field(FieldSelector<'l>),
    // todo: array support
}

impl<'l> BasicSelector<'l> {
    pub fn field(name: &'l str) -> BasicSelector<'l> {
        BasicSelector::Field(FieldSelector::new(name))
    }
}

impl<'l> Selector for BasicSelector<'l> {
    type Value = Value;

    fn name(&self) -> String {
        match self {
            BasicSelector::Field(s) => s.name(),
        }
    }

    fn select<'a>(&self, value: &'a Self::Value) -> Option<&'a Self::Value> {
        match self {
            BasicSelector::Field(s) => s.select(value),
        }
    }

    fn select_owned(&self, value: Self::Value) -> Option<Self::Value> {
        match self {
            BasicSelector::Field(s) => s.select_owned(value),
        }
    }
}
