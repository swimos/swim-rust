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

use crate::{event_handler::{HandlerAction}, agent_lifecycle::utility::HandlerContext, lanes::join_value::{RemoteLane, LinkClosedResponse}};

pub trait OnJoinValueFailed<K, Context>: Send {
    type OnJoinValueFailedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse> + 'a
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: RemoteLane<'_>,
    ) -> Self::OnJoinValueFailedHandler<'a>;
}

pub trait OnJoinValueFailedShared<K, Context, Shared>: Send {
    type OnJoinValueFailedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_failed<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: RemoteLane<'_>,
    ) -> Self::OnJoinValueFailedHandler<'a>;
}