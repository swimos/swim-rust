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

use swim_model::address::Address;

use crate::{agent_lifecycle::utility::HandlerContext, event_handler::EventHandler};

pub trait OnJoinValueSynced<K, V, Context>: Send {
    type OnJoinValueSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a>;
}

pub trait OnJoinValueSyncedShared<K, V, Context, Shared>: Send {
    type OnJoinValueSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a>;
}