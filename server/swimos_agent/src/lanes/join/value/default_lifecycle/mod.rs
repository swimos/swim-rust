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

use crate::{
    event_handler::{ConstHandler, UnitHandler},
    lanes::join::LinkClosedResponse,
};

use super::lifecycle::{
    on_failed::OnJoinValueFailed, on_linked::OnJoinValueLinked, on_synced::OnJoinValueSynced,
    on_unlinked::OnJoinValueUnlinked,
};

/// The default lifecycle for downlinks associated with join value lanes. This is used if no
/// lifecycle is explicitly specified. The event handlers all do nothing.
#[derive(Clone, Copy, Debug)]
pub struct DefaultJoinValueLifecycle;

impl<Context, K> OnJoinValueLinked<K, Context> for DefaultJoinValueLifecycle {
    type OnJoinValueLinkedHandler<'a>
        = UnitHandler
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<Context, K, V> OnJoinValueSynced<K, V, Context> for DefaultJoinValueLifecycle {
    type OnJoinValueSyncedHandler<'a>
        = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
        _value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<Context, K> OnJoinValueUnlinked<K, Context> for DefaultJoinValueLifecycle {
    type OnJoinValueUnlinkedHandler<'a>
        = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        ConstHandler::from(LinkClosedResponse::default())
    }
}

impl<Context, K> OnJoinValueFailed<K, Context> for DefaultJoinValueLifecycle {
    type OnJoinValueFailedHandler<'a>
        = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueFailedHandler<'a> {
        ConstHandler::from(LinkClosedResponse::default())
    }
}
