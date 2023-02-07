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

use self::{
    on_failed::{OnJoinValueFailed, OnJoinValueFailedShared},
    on_linked::{OnJoinValueLinked, OnJoinValueLinkedShared},
    on_synced::{OnJoinValueSynced, OnJoinValueSyncedShared},
    on_unlinked::{OnJoinValueUnlinked, OnJoinValueUnlinkedShared},
};

pub mod on_failed;
pub mod on_linked;
pub mod on_synced;
pub mod on_unlinked;

pub trait JoinValueLaneLifecycle<K, V, Context>:
    OnJoinValueLinked<K, Context>
    + OnJoinValueSynced<K, V, Context>
    + OnJoinValueUnlinked<K, Context>
    + OnJoinValueFailed<K, Context>
{
}

impl<K, V, Context, L> JoinValueLaneLifecycle<K, V, Context> for L where
    L: OnJoinValueLinked<K, Context>
        + OnJoinValueSynced<K, V, Context>
        + OnJoinValueUnlinked<K, Context>
        + OnJoinValueFailed<K, Context>
{
}

pub trait JoinValueLaneLifecycleShared<K, V, Context, Shared>:
    OnJoinValueLinkedShared<K, Context, Shared>
    + OnJoinValueSyncedShared<K, V, Context, Shared>
    + OnJoinValueUnlinkedShared<K, Context, Shared>
    + OnJoinValueFailedShared<K, Context, Shared>
{
}

impl<K, V, Context, Shared, L> JoinValueLaneLifecycleShared<K, V, Context, Shared> for L where
    L: OnJoinValueLinkedShared<K, Context, Shared>
        + OnJoinValueSyncedShared<K, V, Context, Shared>
        + OnJoinValueUnlinkedShared<K, Context, Shared>
        + OnJoinValueFailedShared<K, Context, Shared>
{
}
