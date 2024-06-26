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

use swimos_api::address::Address;

use crate::{agent_lifecycle::HandlerContext, event_handler::HandlerAction};

pub mod map;
#[cfg(test)]
mod test_util;
pub mod value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownlinkStatus {
    Pending,
    Linked,
}

/// The response type for join lane lifecycle events to handle closed downlinks.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkClosedResponse {
    /// Attempt to reinstate the link.
    Retry,
    /// Abandon the link but leave any associated keys in the lane's map.
    #[default]
    Abandon,
    /// Abandon the link and delete any associated keys in the map.
    Delete,
}

pub trait JoinHandlerFn<'a, Context, Shared, T, Out> {
    type Handler: HandlerAction<Context, Completion = Out> + 'a;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: T,
        remote: Address<&str>,
    ) -> Self::Handler;
}

impl<'a, Context, Shared, T, F, H, Out> JoinHandlerFn<'a, Context, Shared, T, Out> for F
where
    F: Fn(&'a Shared, HandlerContext<Context>, T, Address<&str>) -> H,
    H: HandlerAction<Context, Completion = Out> + 'a,
{
    type Handler = H;

    fn make_handler(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: T,
        remote: Address<&str>,
    ) -> Self::Handler {
        (*self)(shared, handler_context, key, remote)
    }
}

/// Enumeration of the varieties of join lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinLaneKind {
    /// Indicates a [join value lane](super::JoinValueLane).
    Value,
    /// Indicates a [join map lane](super::JoinMapLane).
    Map,
}
