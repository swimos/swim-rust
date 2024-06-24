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
use swimos_api::agent::AgentConfig;
use swimos_recon::parser::AsyncParseError;
use swimos_utilities::routing::RouteUri;

use crate::event_handler::check_step::{check_is_complete, check_is_continue};
use crate::event_handler::{GetParameter, ModificationFlags};

use crate::{
    event_handler::{
        ConstHandler, EventHandlerError, GetAgentUri, HandlerActionExt, Sequentially, SideEffects,
    },
    lanes::{value::ValueLaneSet, ValueLane},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::{join, ActionContext, Decode, HandlerAction, Modification, SideEffect, StepResult};

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

struct DummyAgent;

const DUMMY: DummyAgent = DummyAgent;

#[test]
fn side_effect_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut n = 0;
    let mut handler = SideEffect::from(|| n += 1);
    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(n, 1);
}

#[test]
fn side_effects_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let values = vec![0, 1, 2, 3];

    let target = RefCell::new(vec![]);

    let it = values.clone().into_iter().map(|n| {
        let mut guard = target.borrow_mut();
        guard.push(n);
        2 * n
    });

    let mut handler = SideEffects::from(it);

    for i in values {
        let result = handler.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            &DUMMY,
        );
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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert_eq!(result, vec![0, 2, 4, 6]);
    } else {
        panic!("Expected completion.");
    }

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn constant_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut handler = ConstHandler::from(5);
    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 5
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn get_agent_uri() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut handler = GetAgentUri::default();
    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert_eq!(result, uri);
    } else {
        panic!("Expected completion.");
    }

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn get_parameter() {
    let uri = make_uri();
    let mut route_params = HashMap::new();
    route_params.insert("key".to_string(), "value".to_string());
    let meta = make_meta(&uri, &route_params);

    let mut absent = GetParameter::new("other");
    let result = absent.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert!(result.is_none());
    } else {
        panic!("Expected completion.");
    }

    let result = absent.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    let mut present = GetParameter::new("key");

    let result = present.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    if let StepResult::Complete {
        modified_item: None,
        result,
    } = result
    {
        assert_eq!(result, Some("value".to_string()));
    } else {
        panic!("Expected completion.");
    }

    let result = present.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut handler =
        HandlerActionExt::<DummyAgent>::map(GetAgentUri::default(), move |uri: RouteUri| {
            uri.to_string()
        });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    match result {
        StepResult::Complete {
            modified_item: None,
            result,
        } => {
            assert_eq!(result, "/node");
        }
        _ => panic!("Unexpected result"),
    }

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn and_then_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut output = None;
    let output_ref = &mut output;
    let mut handler =
        HandlerActionExt::<DummyAgent>::and_then(GetAgentUri::default(), move |uri: RouteUri| {
            SideEffect::from(move || {
                *output_ref = Some(uri.to_string());
            })
        });

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(output, Some(NODE_URI.to_string()));
}

#[test]
fn and_then_contextual_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    assert_eq!(output, Some(NODE_URI.to_string()));
}

#[test]
fn followed_by_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
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

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn decoding_handler_success() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut buffer = BytesMut::new();
    write!(buffer, "56").expect("Write failed.");

    let mut handler = Decode::<i32>::new(buffer);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 56
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn decoding_handler_failure() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut buffer = BytesMut::new();
    write!(buffer, "boom").expect("Write failed.");

    let mut handler = Decode::<i32>::new(buffer);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::BadCommand(_))
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
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
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut handler = FakeLaneWriter::new(7).and_then(|_| ConstHandler::from(34));
    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    check_is_continue(result, 7, ModificationFlags::all());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            result: 34
        }
    ));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn followed_by_handler_with_lane_write() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut handler = FakeLaneWriter::new(7).followed_by(FakeLaneWriter::new(8));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    check_is_continue(result, 7, ModificationFlags::all());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
    check_is_complete(result, 8, &(), ModificationFlags::all());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &DUMMY,
    );
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
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let values = RefCell::new(vec![]);

    struct TestAgent {
        lane: ValueLane<i32>,
    }

    let set = ValueLaneSet::new(|agent: &TestAgent| &agent.lane, 5);
    let effect1 = SideEffect::from(|| values.borrow_mut().push(1));
    let effect2 = SideEffect::from(|| values.borrow_mut().push(2));

    let handlers = vec![
        effect1.boxed_local(),
        set.boxed_local(),
        effect2.boxed_local(),
    ];

    let mut handler = Sequentially::new(handlers);

    let agent = TestAgent {
        lane: ValueLane::new(0, 0),
    };

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Continue {
            modified_item: None
        }
    ));
    assert_eq!(values.borrow().as_slice(), &[1]);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );

    check_is_continue(result, 0, ModificationFlags::all());

    assert_eq!(values.borrow().as_slice(), &[1]);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));
    assert_eq!(values.borrow().as_slice(), &[1, 2]);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn join_handler() {
    let first = ConstHandler::from(2);
    let second = ConstHandler::from("Hello".to_string());

    let mut both = join::<(), _, _>(first, second);

    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    loop {
        match both.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            &(),
        ) {
            StepResult::Continue { modified_item } => assert!(modified_item.is_none()),
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                assert!(modified_item.is_none());
                assert_eq!(result, (2, "Hello".to_string()));
                break;
            }
        }
    }

    let result = both.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &(),
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn join_handler_modify() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    struct TestAgent {
        lane1: ValueLane<i32>,
        lane2: ValueLane<i32>,
    }

    let first = ValueLaneSet::new(|agent: &TestAgent| &agent.lane1, 2);
    let second = ValueLaneSet::new(|agent: &TestAgent| &agent.lane2, 3);
    let mut both = join::<TestAgent, _, _>(first, second);

    let mut modifications = vec![];

    let agent = TestAgent {
        lane1: ValueLane::new(0, 0),
        lane2: ValueLane::new(1, 0),
    };

    loop {
        match both.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            &agent,
        ) {
            StepResult::Continue { modified_item } => {
                if let Some(modification) = modified_item {
                    modifications.push(modification);
                }
            }
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                if let Some(modification) = modified_item {
                    modifications.push(modification);
                }
                assert_eq!(result, ((), ()));
                break;
            }
        }
    }

    assert_eq!(
        modifications,
        vec![Modification::of(0), Modification::of(1)]
    );

    let result = both.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    agent.lane1.read(|v| assert_eq!(*v, 2));
    agent.lane2.read(|v| assert_eq!(*v, 3));
}

#[test]
fn join3_handler() {
    let first = ConstHandler::from(2);
    let second = ConstHandler::from("Hello".to_string());
    let third = ConstHandler::from(true);

    let mut both = super::join3::<(), _, _, _>(first, second, third);

    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    loop {
        match both.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            &(),
        ) {
            StepResult::Continue { modified_item } => assert!(modified_item.is_none()),
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                assert!(modified_item.is_none());
                assert_eq!(result, (2, "Hello".to_string(), true));
                break;
            }
        }
    }

    let result = both.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &(),
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn join3_handler_modify() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    struct TestAgent {
        lane1: ValueLane<i32>,
        lane2: ValueLane<i32>,
        lane3: ValueLane<i32>,
    }

    let first = ValueLaneSet::new(|agent: &TestAgent| &agent.lane1, 2);
    let second = ValueLaneSet::new(|agent: &TestAgent| &agent.lane2, 3);
    let third = ValueLaneSet::new(|agent: &TestAgent| &agent.lane3, 4);
    let mut both = super::join3::<TestAgent, _, _, _>(first, second, third);

    let mut modifications = vec![];

    let agent = TestAgent {
        lane1: ValueLane::new(0, 0),
        lane2: ValueLane::new(1, 0),
        lane3: ValueLane::new(2, 0),
    };

    loop {
        match both.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            &agent,
        ) {
            StepResult::Continue { modified_item } => {
                if let Some(modification) = modified_item {
                    modifications.push(modification);
                }
            }
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                if let Some(modification) = modified_item {
                    modifications.push(modification);
                }
                assert_eq!(result, ((), (), ()));
                break;
            }
        }
    }

    assert_eq!(
        modifications,
        vec![
            Modification::of(0),
            Modification::of(1),
            Modification::of(2)
        ]
    );

    let result = both.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));

    agent.lane1.read(|v| assert_eq!(*v, 2));
    agent.lane2.read(|v| assert_eq!(*v, 3));
    agent.lane3.read(|v| assert_eq!(*v, 4));
}
