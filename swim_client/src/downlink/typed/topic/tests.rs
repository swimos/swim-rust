// Copyright 2015-2020 SWIM.AI inc.
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

use crate::downlink::model::map::{MapEvent, ValMap, ViewWithEvent};
use crate::downlink::model::value::SharedValue;
use crate::downlink::typed::event::TypedViewWithEvent;
use crate::downlink::typed::topic::{ApplyForm, ApplyFormsMap, TryTransformTopic};
use crate::downlink::Event;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::stream::{iter, Iter};
use futures::FutureExt;
use im::OrdMap;
use std::num::ParseIntError;
use std::sync::Arc;
use std::vec::IntoIter;
use swim_common::model::Value;
use swim_common::topic::{Topic, TopicError};
use utilities::future::Transform;

#[test]
fn apply_form_value() {
    let good = SharedValue::new(Value::Int32Value(7));
    let bad = SharedValue::new(Value::text("hello"));

    let apply: ApplyForm<i32> = ApplyForm::new();
    let result = apply.transform(Event::Local(good));
    assert_eq!(result, Ok(Event::Local(7)));

    let result = apply.transform(Event::Local(bad));
    assert!(result.is_err());
}

#[test]
fn apply_form_map() {
    let mut good_map = ValMap::new();
    good_map.insert(Value::text("a"), Arc::new(Value::Int32Value(1)));
    good_map.insert(Value::text("b"), Arc::new(Value::Int32Value(2)));

    let good = ViewWithEvent {
        view: good_map.clone(),
        event: MapEvent::Update(Value::text("b")),
    };

    let with_bad_event = ViewWithEvent {
        view: good_map.clone(),
        event: MapEvent::Update(Value::Int32Value(7)),
    };

    let apply: ApplyFormsMap<String, i32> = ApplyFormsMap::new();

    let result = apply.transform(Event::Remote(good));
    assert!(result.is_ok());

    if let Event::Remote(TypedViewWithEvent { view, event }) = result.unwrap() {
        let mut expected_view = OrdMap::new();
        expected_view.insert("a".to_string(), 1);
        expected_view.insert("b".to_string(), 2);

        assert_eq!(view.as_ord_map(), expected_view);
        assert_eq!(event, MapEvent::Update("b".to_string()));

        let result = apply.transform(Event::Local(with_bad_event));
        assert!(result.is_err());
    } else {
        panic!("Expected remote event!")
    }
}

#[derive(Clone, Debug)]
struct ParseStringEvent;

impl Transform<Event<String>> for ParseStringEvent {
    type Out = Result<Event<i32>, ParseIntError>;

    fn transform(&self, input: Event<String>) -> Self::Out {
        input.try_transform(|s| s.parse::<i32>())
    }
}

struct TestTopic(Vec<Event<String>>);

impl Topic<Event<String>> for TestTopic {
    type Receiver = Iter<IntoIter<Event<String>>>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        let TestTopic(strings) = self;
        ready(Ok(iter(strings.clone().into_iter()))).boxed()
    }
}

#[tokio::test]
async fn try_transform_topic() {
    let topic = TestTopic(vec![
        Event::Local("0".to_string()),
        Event::Remote("1".to_string()),
        Event::Local("2".to_string()),
        Event::Local("fail".to_string()),
        Event::Remote("3".to_string()),
    ]);

    let expected = vec![Event::Local(0), Event::Remote(1), Event::Local(2)];

    let mut transformed: TryTransformTopic<String, TestTopic, ParseStringEvent> =
        TryTransformTopic::new(topic, ParseStringEvent);

    let sub1 = transformed.subscribe().await;

    assert!(sub1.is_ok());
    let stream1 = sub1.unwrap();

    let results1 = stream1.collect::<Vec<_>>().await;
    assert_eq!(&results1, &expected);

    let sub2 = transformed.subscribe().await;

    assert!(sub2.is_ok());
    let stream2 = sub2.unwrap();

    let results2 = stream2.collect::<Vec<_>>().await;
    assert_eq!(&results2, &expected);
}
