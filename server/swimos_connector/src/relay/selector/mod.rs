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

pub use common::{ParseError, Part, SelectorPatternIter};
pub use lane::LaneSelector;
pub use node::NodeSelector;
pub use payload::PayloadSelector;

use crate::relay::selector::payload::GenericSendCommandOp;
use crate::selector::Deferred;
use crate::SelectorError;
use std::slice::Iter;
use std::sync::Arc;
use swimos_model::Value;

/// A collection of relays which are used to derive the commands to send to lanes on agents.
#[derive(Debug, Clone, Default)]
pub struct Relays {
    inner: Arc<Inner>,
}

impl Relays {
    pub fn new<I>(chain: I) -> Relays
    where
        I: IntoIterator<Item = Relay>,
    {
        Relays {
            inner: Arc::new(Inner {
                chain: chain.into_iter().collect(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.chain.is_empty()
    }
}

impl From<Relay> for Relays {
    fn from(relays: Relay) -> Relays {
        Relays {
            inner: Arc::new(Inner {
                chain: vec![relays],
            }),
        }
    }
}

impl<'s> IntoIterator for &'s Relays {
    type Item = &'s Relay;
    type IntoIter = Iter<'s, Relay>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.as_ref().chain.as_slice().iter()
    }
}

#[derive(Default, Debug)]
struct Inner {
    chain: Vec<Relay>,
}

/// A relay which is used to build a command to send to a lane on an agent.
#[derive(Debug)]
pub struct Relay {
    node: NodeSelector,
    lane: LaneSelector,
    payload: PayloadSelector,
}

impl Relay {
    /// Builds a new relay.
    ///
    /// # Arguments
    /// * `node` - a selector for deriving a node URI to send a command to.
    /// * `lane` - a selector for deriving a lane URI to send a command to.
    /// * `payload` - a selector for extracting the command.
    pub fn new(node: NodeSelector, lane: LaneSelector, payload: PayloadSelector) -> Relay {
        Relay {
            node,
            lane,
            payload,
        }
    }

    pub fn select_handler<K, V>(
        &self,
        topic: &Value,
        key: &mut K,
        value: &mut V,
    ) -> Result<GenericSendCommandOp, SelectorError>
    where
        K: Deferred,
        V: Deferred,
    {
        let Relay {
            node,
            lane,
            payload,
        } = self;
        let node_uri = node.select(key, value, topic)?;
        let lane_uri = lane.select(key, value, topic)?;
        payload.select(node_uri, lane_uri, key, value, topic)
    }
}
