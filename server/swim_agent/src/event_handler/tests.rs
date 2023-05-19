// Copyright 2015-2023 Swim Inc.
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

use std::fmt::Write;
use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use swim_api::agent::AgentConfig;
use swim_recon::parser::AsyncParseError;
use swim_utilities::routing::route_uri::RouteUri;

use crate::{
    event_handler::{
        ConstHandler, EventHandlerError, GetAgentUri, HandlerActionExt, Sequentially, SideEffects,
    },
    lanes::{value::ValueLaneSet, ValueLane},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::{
    ActionContext, Decode, HandlerAction, HandlerFuture, Modification, SideEffect, Spawner,
    StepResult,
};

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RouteUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

struct DummyAgent;

const DUMMY: DummyAgent = DummyAgent;

#[test]
fn side_effect_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut n = 0;
    let mut handler = SideEffect::from(|| n += 1);
    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(n, 1);
}

#[test]
fn side_effects_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let values = vec![0, 1, 2, 3];

    let target = RefCell::new(vec![]);

    let it = values.clone().into_iter().map(|n| {
        let mut guard = target.borrow_mut();
        guard.push(n);
        2 * n
    });

    let mut handler = SideEffects::from(it);

    for i in values {
        let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
        assert!(matches!(
            result,
            StepResult::Continue {
                modified_item: None
            }
        ));

        let guard = target.borrow();
        let expected: Vec<i32> = (0..(i + 1)).collect();
        assert_eq!(*guard, expected);
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert_eq!(result, vec![0, 2, 4, 6]);
    } else {
        panic!("Expected completion.");
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn constant_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut handler = ConstHandler::from(5);
    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 5
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn get_agent_uri() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut handler = GetAgentUri::default();
    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert_eq!(result, uri);
    } else {
        panic!("Expected completion.");
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let mut handler =
        HandlerActionExt::<DummyAgent>::map(GetAgentUri::default(), move |uri: RouteUri| {
            uri.to_string()
        });

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    match result {
        StepResult::Complete {
            modified_item: None,
            result,
        } => {
            assert_eq!(result, "/node");
        }
        _ => panic!("Unexpected result"),
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn and_then_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut output = None;
    let output_ref = &mut output;
    let mut handler =
        HandlerActionExt::<DummyAgent>::and_then(GetAgentUri::default(), move |uri: RouteUri| {
            SideEffect::from(move || {
                *output_ref = Some(uri.to_string());
            })
        });

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(output, Some(NODE_URI.to_string()));
}

#[test]
fn and_then_contextual_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut output = None;
    let output_ref = &mut output;
    let mut handler = HandlerActionExt::<DummyAgent>::and_then_contextual(
        GetAgentUri::default(),
        move |_: &DummyAgent, uri: RouteUri| {
            SideEffect::from(move || {
                *output_ref = Some(uri.to_string());
            })
        },
    );

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(output, Some(NODE_URI.to_string()));
}

#[test]
fn followed_by_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let output = RefCell::new(None);

    let first = SideEffect::from(|| {
        let mut guard = output.borrow_mut();
        *guard = Some(1);
    });

    let second = SideEffect::from(|| {
        let mut guard = output.borrow_mut();
        *guard = Some(2);
    });

    let mut handler = HandlerActionExt::<DummyAgent>::followed_by(first, second);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));

    {
        let guard = output.borrow();
        assert_eq!(*guard, Some(1));
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    {
        let guard = output.borrow();
        assert_eq!(*guard, Some(2));
    }

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn decoding_handler_success() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut buffer = BytesMut::new();
    write!(buffer, "56").expect("Write failed.");

    let mut handler = Decode::<i32>::new(buffer);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 56
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn decoding_handler_failure() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut buffer = BytesMut::new();
    write!(buffer, "boom").expect("Write failed.");

    let mut handler = Decode::<i32>::new(buffer);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::BadCommand(_))
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

struct FakeLaneWriter(Option<u64>);

impl FakeLaneWriter {
    fn new(id: u64) -> Self {
        FakeLaneWriter(Some(id))
    }
}

impl HandlerAction<DummyAgent> for FakeLaneWriter {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<DummyAgent>,
        _meta: AgentMetadata,
        _context: &DummyAgent,
    ) -> StepResult<Self::Completion> {
        let FakeLaneWriter(id) = self;
        if let Some(n) = id.take() {
            StepResult::Complete {
                modified_item: Some(Modification::of(n)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

#[test]
fn and_then_handler_with_lane_write() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut handler = FakeLaneWriter::new(7).and_then(|_| ConstHandler::from(34));
    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: Some(Modification {
                item_id: 7,
                trigger_handler: true
            })
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 34
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn followed_by_handler_with_lane_write() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let mut handler = FakeLaneWriter::new(7).followed_by(FakeLaneWriter::new(8));
    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: Some(Modification {
                item_id: 7,
                trigger_handler: true
            })
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: Some(Modification {
                item_id: 8,
                trigger_handler: true
            }),
            ..
        }
    ));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &DUMMY);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn event_handler_error_display() {
    let string = format!("{}", EventHandlerError::SteppedAfterComplete);
    assert_eq!(string, "Event handler stepped after completion.");

    let err = AsyncParseError::UnconsumedInput;
    let string = format!("{}", EventHandlerError::BadCommand(err));
    let expected = format!(
        "Invalid incoming message: {}",
        AsyncParseError::UnconsumedInput
    );
    assert_eq!(string, expected);

    let string = format!("{}", EventHandlerError::IncompleteCommand);
    assert_eq!(string, "An incoming message was incomplete.");
}

#[test]
fn sequentially_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let values = RefCell::new(vec![]);

    struct TestAgent {
        lane: ValueLane<i32>,
    }

    let set = ValueLaneSet::new(|agent: &TestAgent| &agent.lane, 5);
    let effect1 = SideEffect::from(|| values.borrow_mut().push(1));
    let effect2 = SideEffect::from(|| values.borrow_mut().push(2));

    let handlers = vec![effect1.boxed(), set.boxed(), effect2.boxed()];

    let mut handler = Sequentially::new(handlers);

    let agent = TestAgent {
        lane: ValueLane::new(0, 0),
    };

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));
    assert_eq!(values.borrow().as_slice(), &[1]);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: Some(Modification {
                item_id: 0,
                trigger_handler: true
            })
        }
    ));
    assert_eq!(values.borrow().as_slice(), &[1]);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));
    assert_eq!(values.borrow().as_slice(), &[1, 2]);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}
