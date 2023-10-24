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

use std::any::{Any, TypeId};
use std::hash::Hash;

use swim_form::Form;
use swim_model::address::Address;
use swim_model::Text;

use crate::event_handler::{DowncastError, EventHandler};

//use super::AddDownlinkAction;
use super::{lifecycle::JoinMapLaneLifecycle, JoinMapLane};

/// Uses a [`JoinMapLaneLifecycle`] to create a handler action that will open a new downlink
/// for a join value lane. The purposes of this is to hide the specific types of the lane behind
/// the [`JoinMapInitializer`] trait so it can be stored inside the agent context (which has no
/// knowledge of the types).
pub struct LifecycleInitializer<Context, L, K, V, F> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    lifecycle_factory: F,
}
