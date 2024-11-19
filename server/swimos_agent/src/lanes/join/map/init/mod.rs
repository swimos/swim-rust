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

use std::any::{Any, TypeId};
use std::hash::Hash;

use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::Text;

use crate::agent_model::AgentDescription;
use crate::event_handler::{DowncastError, EventHandler, JoinLaneInitializer};
use crate::lanes::JoinLaneKind;

use super::AddDownlinkAction;
use super::{lifecycle::JoinMapLaneLifecycle, JoinMapLane};

/// Uses a [`JoinMapLaneLifecycle`] to create a handler action that will open a new downlink
/// for a join map lane. The purposes of this is to hide the specific types of the lane behind
/// the [`JoinLaneInitializer`] trait so it can be stored inside the agent context (which has no
/// knowledge of the types).
pub struct LifecycleInitializer<Context, L, K, V, F> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    lifecycle_factory: F,
}

impl<Context, L, K, V, F, LC> LifecycleInitializer<Context, L, K, V, F>
where
    F: Fn() -> LC + Send,
    LC: JoinMapLaneLifecycle<L, K, Context> + Send + 'static,
{
    pub fn new(projection: fn(&Context) -> &JoinMapLane<L, K, V>, lifecycle_factory: F) -> Self {
        LifecycleInitializer {
            projection,
            lifecycle_factory,
        }
    }
}

impl<Context, L, K, V, F, LC> JoinLaneInitializer<Context>
    for LifecycleInitializer<Context, L, K, V, F>
where
    Context: AgentDescription + 'static,
    L: Any + Clone + Eq + Hash + Send + 'static,
    K: Any + Form + Clone + Eq + Hash + Ord + Send + 'static,
    V: Any + Form + Send + Sync + 'static,
    K::Rec: Send,
    V::BodyRec: Send,
    F: Fn() -> LC + Send,
    LC: JoinMapLaneLifecycle<L, K, Context> + Send + 'static,
{
    fn try_create_action(
        &self,
        link_key: Box<dyn Any + Send>,
        key_type: TypeId,
        value_type: TypeId,
        address: Address<Text>,
    ) -> Result<Box<dyn EventHandler<Context> + Send + 'static>, DowncastError> {
        let LifecycleInitializer {
            projection,
            lifecycle_factory,
        } = self;

        match link_key.downcast::<L>() {
            Ok(link_key) => {
                let expected_key = TypeId::of::<K>();
                let expected_value = TypeId::of::<V>();
                if key_type != expected_key {
                    return Err(DowncastError::Key {
                        actual_type: key_type,
                        expected_type: expected_key,
                    });
                }
                if value_type != expected_value {
                    return Err(DowncastError::Value {
                        actual_type: value_type,
                        expected_type: expected_value,
                    });
                }
                let lifecycle = lifecycle_factory();
                let action = AddDownlinkAction::new(*projection, *link_key, address, lifecycle);
                Ok(Box::new(action))
            }
            Err(bad_key) => Err(DowncastError::LinkKey {
                key: bad_key,
                expected_type: std::any::TypeId::of::<K>(),
            }),
        }
    }

    fn kind(&self) -> JoinLaneKind {
        JoinLaneKind::Map
    }
}
