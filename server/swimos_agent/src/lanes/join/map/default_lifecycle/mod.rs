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

use std::collections::HashSet;

use swimos_model::address::Address;

use crate::{
    event_handler::{ConstHandler, UnitHandler},
    lanes::LinkClosedResponse,
};

use super::lifecycle::{
    on_failed::OnJoinMapFailed, on_linked::OnJoinMapLinked, on_synced::OnJoinMapSynced,
    on_unlinked::OnJoinMapUnlinked,
};

/// The default lifecycle for downlinks associated with join map lanes. This is used if no
/// lifecycle is explicitly specified. The event handlers all do nothing.
#[derive(Clone, Copy, Debug)]
pub struct DefaultJoinMapLifecycle;

impl<L, Context> OnJoinMapLinked<L, Context> for DefaultJoinMapLifecycle {
    type OnJoinMapLinkedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, K, Context> OnJoinMapSynced<L, K, Context> for DefaultJoinMapLifecycle {
    type OnJoinMapSyncedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
        _keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, K, Context> OnJoinMapUnlinked<L, K, Context> for DefaultJoinMapLifecycle {
    type OnJoinMapUnlinkedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
        _keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        ConstHandler::from(LinkClosedResponse::default())
    }
}

impl<L, K, Context> OnJoinMapFailed<L, K, Context> for DefaultJoinMapLifecycle {
    type OnJoinMapFailedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
        _keys: HashSet<K>,
    ) -> Self::OnJoinMapFailedHandler<'a> {
        ConstHandler::from(LinkClosedResponse::default())
    }
}
