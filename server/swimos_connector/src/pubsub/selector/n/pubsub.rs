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

use crate::pubsub::selector::n::{MapLaneSelector, ValueLaneSelector};
use crate::{
    pubsub::selector::n::{BoxedComputed, ChainSelector, Deferred, Selector, ValueSelector},
    DeserializationError,
};
use frunk::Coprod;
use swimos_model::Value;

/// Canonical selector for pub-sub type connectors.
pub type PubSubSelector = Coprod!(TopicSelector, KeySelector, PayloadSelector);
/// Selector type for Value Lanes.
pub type PubSubValueLaneSelector = ValueLaneSelector<PubSubSelector>;
/// Selector type for Map Lanes.
pub type PubSubMapLaneSelector = MapLaneSelector<PubSubSelector, PubSubSelector>;

pub struct TopicSelector;

impl Selector for TopicSelector {
    type Arg = Value;

    fn select<'a>(
        &self,
        from: &'a mut Self::Arg,
    ) -> Result<Option<&'a Value>, DeserializationError> {
        Ok(Some(from))
    }
}

pub struct KeySelector(ChainSelector);

impl KeySelector {
    pub fn new(inner: ChainSelector) -> KeySelector {
        KeySelector(inner)
    }
}

impl Selector for KeySelector {
    type Arg = BoxedComputed;

    fn select<'a>(
        &self,
        from: &'a mut Self::Arg,
    ) -> Result<Option<&'a Value>, DeserializationError> {
        let KeySelector(chain) = self;
        Ok(ValueSelector::select(chain, from.get()?))
    }
}

pub struct PayloadSelector(ChainSelector);

impl PayloadSelector {
    pub fn new(inner: ChainSelector) -> PayloadSelector {
        PayloadSelector(inner)
    }
}

impl Selector for PayloadSelector {
    type Arg = BoxedComputed;

    fn select<'a>(
        &self,
        from: &'a mut Self::Arg,
    ) -> Result<Option<&'a Value>, DeserializationError> {
        let PayloadSelector(chain) = self;
        Ok(ValueSelector::select(chain, from.get()?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pubsub::selector::n::{
            ChainSelector, Computed, KeySelector, PayloadSelector, PubSubSelector,
            PubSubValueLaneSelector, SelectHandler, TopicSelector,
        },
        test_support::{fail, run_handler, TestSpawner},
        ConnectorAgent,
    };
    use bytes::BytesMut;
    use frunk::hlist;
    use std::{fmt::Debug, ops::Deref};
    use swimos_agent::{
        agent_model::{AgentSpec, ItemDescriptor},
        lanes::ValueLane,
    };
    use swimos_api::agent::WarpLaneKind;
    use swimos_model::Value;

    const LANE: &str = "lane";

    struct TestAgent {
        a: ValueLane<String>,
        b: ValueLane<i32>,
        c: ValueLane<f64>,
    }

    impl Default for TestAgent {
        fn default() -> Self {
            TestAgent {
                a: ValueLane::new(0, "".to_string()),
                b: ValueLane::new(1, 1),
                c: ValueLane::new(2, 0f64),
            }
        }
    }

    fn run_selector(selector: PubSubSelector, expected: Value) {
        let selector = PubSubValueLaneSelector::new(LANE.to_string(), selector, true);
        let topic = Value::from("topic");
        let key = Computed::boxed(move || Ok(Value::from(13)));
        let value = Computed::boxed(move || Ok(Value::from(64f64)));

        let mut args = hlist![topic, key, value];
        let handler = selector.select_handler(&mut args).unwrap();
        let mut agent = ConnectorAgent::default();

        agent
            .register_dynamic_item(
                LANE,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Value,
                    flags: Default::default(),
                },
            )
            .expect("Failed to register lane");

        run_handler(
            &TestSpawner::default(),
            &mut BytesMut::default(),
            &agent,
            handler,
            fail,
        );

        match agent.value_lane(LANE) {
            Some(de) => {
                let lane = de.deref();
                lane.read(|state| {
                    assert_eq!(state, &expected);
                })
            }
            None => {
                panic!("Missing lane lane")
            }
        };
    }

    #[test]
    fn selects_topic() {
        run_selector(PubSubSelector::inject(TopicSelector), Value::from("topic"));
    }

    #[test]
    fn selects_key() {
        run_selector(
            PubSubSelector::inject(KeySelector::new(ChainSelector::default())),
            Value::from(13),
        );
    }

    #[test]
    fn selects_payload() {
        run_selector(
            PubSubSelector::inject(PayloadSelector::new(ChainSelector::default())),
            Value::from(64f64),
        );
    }
}
