// Copyright 2015-2021 Swim Inc.
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

use std::collections::HashMap;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::{
    event_handler::{EventHandler, UnitHandler},
    lifecycle::utility::HandlerContext,
};

pub trait OnUpdate<'a, K, V, Context>: Send {
    type OnUpdateHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_update(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
    ) -> Self::OnUpdateHandler;
}

pub trait OnUpdateShared<'a, K, V, Context, Shared>: Send {
    type OnUpdateHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
    ) -> Self::OnUpdateHandler;
}

impl<'a, K, V, Context> OnUpdate<'a, K, V, Context> for NoHandler {
    type OnUpdateHandler = UnitHandler;

    fn on_update(
        &'a self,
        _map: &HashMap<K, V>,
        _key: K,
        _prev_value: Option<V>,
    ) -> Self::OnUpdateHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnUpdateShared<'a, K, V, Context, Shared> for NoHandler {
    type OnUpdateHandler = UnitHandler;

    fn on_update(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &HashMap<K, V>,
        _key: K,
        _prev_value: Option<V>,
    ) -> Self::OnUpdateHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnUpdate<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(&HashMap<K, V>, K, Option<V>) -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
    ) -> Self::OnUpdateHandler {
        let FnHandler(f) = self;
        f(map, key, prev_value)
    }
}

impl<'a, K, V, Context, Shared, F, H> OnUpdateShared<'a, K, V, Context, Shared> for FnHandler<F>
where
    Shared: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>, &HashMap<K, V>, K, Option<V>) -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
    ) -> Self::OnUpdateHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, map, key, prev_value)
    }
}
