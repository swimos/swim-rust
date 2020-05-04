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

mod value {

    use crate::downlink::model::value::{Action, SharedValue};
    use crate::downlink::typed::action::ValueActions;
    use crate::downlink::DownlinkError;
    use common::model::Value;
    use common::sink::item::ItemSink;
    use futures::future::{ready, Ready};
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
                }
                Action::Get(cb) => {
                    let _ = cb.send(state.clone());
                    Ok(())
                }
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
                }
            };
            ready(result)
        }
    }

    #[tokio::test]
    async fn value_get() {
        let mut actions = TestValueDl::actions(2);

        let n = actions.get().await;
        assert_that!(n, eq(Ok(2)));
    }

    #[tokio::test]
    async fn value_set() {
        let mut actions = TestValueDl::actions(2);

        let result = actions.set(7).await;
        assert_that!(result, eq(Ok(())));

        let n = actions.get().await;
        assert_that!(n, eq(Ok(7)));
    }

    #[tokio::test]
    async fn value_set_and_forget() {
        let mut actions = TestValueDl::actions(2);

        let result = actions.set_and_forget(7).await;
        assert_that!(result, eq(Ok(())));

        let n = actions.get().await;
        assert_that!(n, eq(Ok(7)));
    }

    #[tokio::test]
    async fn value_update() {
        let mut actions = TestValueDl::actions(2);

        let result = actions.update(|n| n + 2).await;
        assert_that!(result, eq(Ok(2)));

        let n = actions.get().await;
        assert_that!(n, eq(Ok(4)));
    }

    #[tokio::test]
    async fn value_update_and_forget() {
        let mut actions = TestValueDl::actions(2);

        let result = actions.update_and_forget(|n| n + 2).await;
        assert_that!(result, eq(Ok(())));

        let n = actions.get().await;
        assert_that!(n, eq(Ok(4)));
    }
}

mod map {

    use crate::downlink::model::map::{MapAction, ValMap};
    use crate::downlink::typed::action::MapActions;
    use crate::downlink::DownlinkError;
    use common::model::Value;
    use common::sink::item::ItemSink;
    use futures::future::{ready, Ready};
    use hamcrest2::assert_that;
    use hamcrest2::prelude::*;
    use im::OrdMap;
    use std::sync::Arc;

    struct TestMapDl(ValMap);

    impl TestMapDl {
        fn new(map: OrdMap<i32, i32>) -> Self {
            TestMapDl(
                map.iter()
                    .map(|(k, v)| (Value::Int32Value(*k), Value::Int32Value(*v)))
                    .collect(),
            )
        }

        fn actions(map: OrdMap<i32, i32>) -> MapActions<TestMapDl, i32, i32> {
            MapActions::new(TestMapDl::new(map))
        }
    }

    impl<'a> ItemSink<'a, MapAction> for TestMapDl {
        type Error = DownlinkError;
        type SendFuture = Ready<Result<(), Self::Error>>;

        fn send_item(&'a mut self, value: MapAction) -> Self::SendFuture {
            let TestMapDl(state) = self;
            let result = match value {
                MapAction::Insert { key, value, old } => {
                    if matches!((&key, &value), (Value::Int32Value(_), Value::Int32Value(_))) {
                        let old_val = state.get(&key).map(Clone::clone);
                        state.insert(key, Arc::new(value));
                        if let Some(cb) = old {
                            let _ = cb.send(old_val);
                        }
                        Ok(())
                    } else {
                        Err(DownlinkError::InvalidAction)
                    }
                }
                MapAction::Remove { key, old } => {
                    if matches!(&key, Value::Int32Value(_)) {
                        let old_val = state.get(&key).map(Clone::clone);
                        state.remove(&key);
                        if let Some(cb) = old {
                            let _ = cb.send(old_val);
                        }
                        Ok(())
                    } else {
                        Err(DownlinkError::InvalidAction)
                    }
                }
                MapAction::Take { n, before, after } => {
                    let map_before = state.clone();
                    *state = state.take(n);
                    if let Some(cb) = before {
                        let _ = cb.send(map_before);
                    }
                    if let Some(cb) = after {
                        let _ = cb.send(state.clone());
                    }
                    Ok(())
                }
                MapAction::Skip { n, before, after } => {
                    let map_before = state.clone();
                    *state = state.skip(n);
                    if let Some(cb) = before {
                        let _ = cb.send(map_before);
                    }
                    if let Some(cb) = after {
                        let _ = cb.send(state.clone());
                    }
                    Ok(())
                }
                MapAction::Clear { before } => {
                    let map_before = state.clone();
                    state.clear();
                    if let Some(cb) = before {
                        let _ = cb.send(map_before);
                    }
                    Ok(())
                }
                MapAction::Get { request } => {
                    let _ = request.send(state.clone());
                    Ok(())
                }
                MapAction::GetByKey { key, request } => {
                    if matches!(&key, Value::Int32Value(_)) {
                        let _ = request.send(state.get(&key).map(Clone::clone));
                        Ok(())
                    } else {
                        Err(DownlinkError::InvalidAction)
                    }
                }
                MapAction::Update {
                    key,
                    f,
                    before,
                    after,
                } => {
                    if matches!(&key, Value::Int32Value(_)) {
                        let old_val = state.get(&key).map(Clone::clone);
                        let replacement = match &old_val {
                            None => f(&None),
                            Some(v) => f(&Some(v.as_ref())),
                        }
                        .map(Arc::new);
                        match &replacement {
                            Some(v) => state.insert(key, v.clone()),
                            _ => state.remove(&key),
                        };
                        if let Some(cb) = before {
                            let _ = cb.send(old_val);
                        }
                        if let Some(cb) = after {
                            let _ = cb.send(replacement);
                        }
                        Ok(())
                    } else {
                        Err(DownlinkError::InvalidAction)
                    }
                }
            };
            ready(result)
        }
    }

    fn make_map() -> OrdMap<i32, i32> {
        let mut map = OrdMap::new();
        map.insert(1, 2);
        map.insert(2, 4);
        map.insert(3, 6);
        map
    }

    #[tokio::test]
    async fn map_view() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.view().await;
        assert_that!(&result, ok());

        let map = result.unwrap().as_ord_map();

        assert_that!(map, eq(make_map()));
    }

    #[tokio::test]
    async fn map_get() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.get(2).await;
        assert_that!(&result, ok());

        let map = result.unwrap();

        assert_that!(map, eq(Some(4)));
    }

    #[tokio::test]
    async fn map_insert() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.insert(4, 8).await;
        assert_that!(&result, ok());

        let map = result.unwrap();

        assert_that!(map, eq(None));

        let result = actions.get(4).await;
        assert_that!(result, eq(Ok(Some(8))));
    }

    #[tokio::test]
    async fn map_insert_and_forget() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.insert_and_forget(4, 8).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.get(4).await;
        assert_that!(result, eq(Ok(Some(8))));
    }

    #[tokio::test]
    async fn map_remove() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.remove(2).await;
        assert_that!(&result, ok());

        let map = result.unwrap();

        assert_that!(map, eq(Some(4)));

        let result = actions.get(2).await;
        assert_that!(result, eq(Ok(None)));
    }

    #[tokio::test]
    async fn map_remove_and_forget() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.remove_and_forget(2).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.get(2).await;
        assert_that!(result, eq(Ok(None)));
    }

    #[tokio::test]
    async fn map_clear() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.clear().await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        let map = result.unwrap();

        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn map_clear_and_forget() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.clear_and_forget().await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        let map = result.unwrap();

        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn map_remove_all() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.remove_all().await;
        assert_that!(&result, ok());

        let map = result.unwrap().as_ord_map();

        assert_that!(map, eq(make_map()));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap();

        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn map_take() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.take(1).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(1, 2);
        assert_that!(map, eq(expected));
    }

    #[tokio::test]
    async fn map_take_and_forget() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.take_and_forget(1).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(1, 2);
        assert_that!(map, eq(expected));
    }

    #[tokio::test]
    async fn map_take_and_get() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.take_and_get(1).await;

        let mut expected = OrdMap::new();
        expected.insert(1, 2);

        assert_that!(&result, ok());

        let (before, after) = result.unwrap();

        assert_that!(before.as_ord_map(), eq(make_map()));
        assert_that!(after.as_ord_map(), eq(expected.clone()));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        assert_that!(map, eq(expected));
    }

    #[tokio::test]
    async fn map_skip() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.skip(2).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(3, 6);
        assert_that!(map, eq(expected));
    }

    #[tokio::test]
    async fn map_skip_and_forget() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.skip_and_forget(2).await;
        assert_that!(result, eq(Ok(())));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(3, 6);
        assert_that!(map, eq(expected));
    }

    #[tokio::test]
    async fn map_skip_and_get() {
        let mut actions = TestMapDl::actions(make_map());

        let result = actions.skip_and_get(2).await;

        let mut expected = OrdMap::new();
        expected.insert(3, 6);

        assert_that!(&result, ok());

        let (before, after) = result.unwrap();

        assert_that!(before.as_ord_map(), eq(make_map()));
        assert_that!(after.as_ord_map(), eq(expected.clone()));

        let result = actions.view().await;
        assert_that!(&result, ok());
        let map = result.unwrap().as_ord_map();

        assert_that!(map, eq(expected));
    }
}
