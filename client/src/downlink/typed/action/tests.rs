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

use common::model::Value;
use crate::downlink::model::value::{Action, SharedValue};
use common::sink::item::ItemSink;
use crate::downlink::DownlinkError;
use futures::future::{ready, Ready};
use crate::downlink::typed::action::ValueActions;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;

struct TestValueDl(SharedValue);

impl TestValueDl {

    fn new(n: i32) -> TestValueDl {
        TestValueDl(SharedValue::new(Value::Int32Value(n)))
    }

    fn actions(n: i32) -> ValueActions<TestValueDl, i32> {
        ValueActions::new(TestValueDl::new(n))
    }

}

impl<'a> ItemSink<'a, Action> for TestValueDl {
    type Error = DownlinkError;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Action) -> Self::SendFuture {
        let TestValueDl(state) = self;
        let result = match value {
            Action::Set(v, maybe_cb) => {
                if matches!(v, Value::Int32Value(_)) {
                    *state = SharedValue::new(v);
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send(());
                    }
                    Ok(())
                } else {
                    Err(DownlinkError::InvalidAction)
                }
            },
            Action::Get(cb) => {
                let _ = cb.send(state.clone());
                Ok(())
            },
            Action::Update(f, maybe_cb) => {
                let old = state.clone();
                let new = f(state.as_ref());
                if matches!(new, Value::Int32Value(_)) {
                    *state = SharedValue::new(new);
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send(old);
                    }
                    Ok(())
                } else {
                    Err(DownlinkError::InvalidAction)
                }
            },
        };
        ready(result)
    }
}

#[tokio::test]
async fn value_get() {
    let mut actions: ValueActions<TestValueDl, i32> = TestValueDl::actions(2);

    let n = actions.get().await;
    assert_that!(n, eq(Ok(2)));
}

#[tokio::test]
async fn value_set() {
    let mut actions: ValueActions<TestValueDl, i32> = TestValueDl::actions(2);

    let result = actions.set(7).await;
    assert_that!(result, eq(Ok(())));

    let n = actions.get().await;
    assert_that!(n, eq(Ok(7)));
}

#[tokio::test]
async fn value_update() {
    let mut actions: ValueActions<TestValueDl, i32> = TestValueDl::actions(2);

    let result = actions.update(|n| n + 2).await;
    assert_that!(result, eq(Ok(2)));

    let n = actions.get().await;
    assert_that!(n, eq(Ok(4)));
}