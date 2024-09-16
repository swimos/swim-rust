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

use std::slice::Iter;
use std::sync::Arc;

/// A collection of selectors which are used to derive the commands to send to lanes on agents.
pub struct RecordSelectors {
    inner: Arc<Inner>,
}

impl<I> From<I> for RecordSelectors
where
    I: IntoIterator<Item = Selectors>,
{
    fn from(chain: I) -> RecordSelectors {
        RecordSelectors {
            inner: Arc::new(Inner {
                chain: chain.into_iter().collect(),
            }),
        }
    }
}

impl From<Selectors> for RecordSelectors {
    fn from(selectors: Selectors) -> RecordSelectors {
        RecordSelectors {
            inner: Arc::new(Inner {
                chain: vec![selectors],
            }),
        }
    }
}

impl<'s> IntoIterator for &'s RecordSelectors {
    type Item = &'s Selectors;
    type IntoIter = Iter<'s, Selectors>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.as_ref().chain.as_slice().iter()
    }
}

struct Inner {
    chain: Vec<Selectors>,
}

/// A selector which is used to build a command to send to a lane on an agent.
pub struct Selectors {
    node: NodeSelector,
    lane: LaneSelector,
    payload: PayloadSelector,
}

impl Selectors {
    /// Builds a new selector.
    ///
    /// # Arguments
    /// * `node` - a selector for deriving a node URI to send a command to.
    /// * `lane` - a selector for deriving a lane URI to send a command to.
    /// * `payload` - a selector for extracting the command.
    pub fn new(node: NodeSelector, lane: LaneSelector, payload: PayloadSelector) -> Selectors {
        Selectors {
            node,
            lane,
            payload,
        }
    }

    pub(crate) fn with<F, O>(&self, f: F) -> O
    where
        F: FnOnce(&NodeSelector, &LaneSelector, &PayloadSelector) -> O,
    {
        let Selectors {
            node,
            lane,
            payload,
        } = self;
        f(node, lane, payload)
    }
}
