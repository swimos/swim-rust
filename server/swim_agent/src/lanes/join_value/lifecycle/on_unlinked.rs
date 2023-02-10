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

use crate::{
    agent_lifecycle::utility::HandlerContext, event_handler::HandlerAction,
    lanes::join_value::LinkClosedResponse,
};

pub trait OnJoinValueUnlinked<K, Context>: Send {
    type OnJoinValueUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse>
        + 'a
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a>;
}

pub trait OnJoinValueUnlinkedShared<K, Context, Shared>: Send {
    type OnJoinValueUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse>
        + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a>;
}
