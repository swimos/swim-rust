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

use crate::event_handler::{DowncastError, EventHandler, JoinLaneInitializer};
use crate::lanes::JoinLaneKind;

use super::AddDownlinkAction;
use super::{lifecycle::JoinValueLaneLifecycle, JoinValueLane};

#[cfg(test)]
mod tests;

/// Uses a [`JoinValueLaneLifecycle`] to create a handler action that will open a new downlink
/// for a join value lane. The purposes of this is to hide the specific types of the lane behind
/// the [`JoinValueInitializer`] trait so it can be stored inside the agent context (which has no
/// knowledge of the types).
pub struct LifecycleInitializer<Context, K, V, F> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    lifecycle_factory: F,
}

impl<Context, K, V, F, LC> LifecycleInitializer<Context, K, V, F>
where
    F: Fn() -> LC + Send,
    LC: JoinValueLaneLifecycle<K, V, Context> + Send + 'static,
{
    pub fn new(projection: fn(&Context) -> &JoinValueLane<K, V>, lifecycle_factory: F) -> Self {
        LifecycleInitializer {
            projection,
            lifecycle_factory,
        }
    }
}

impl<Context, K, V, F, LC> JoinLaneInitializer<Context> for LifecycleInitializer<Context, K, V, F>
where
    Context: 'static,
    K: Any + Clone + Eq + Hash + Send + 'static,
    V: Any + Form + Send + Sync + 'static,
    V::Rec: Send,
    F: Fn() -> LC + Send,
    LC: JoinValueLaneLifecycle<K, V, Context> + Send + 'static,
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

        if (*link_key).type_id() != key_type {
            return Err(DowncastError::LinkKey {
                key: link_key,
                expected_type: key_type,
            });
        }

        match link_key.downcast::<K>() {
            Ok(key) => {
                let expected_value = TypeId::of::<V>();
                if value_type == expected_value {
                    let lifecycle = lifecycle_factory();
                    let action = AddDownlinkAction::new(*projection, *key, address, lifecycle);
                    Ok(Box::new(action))
                } else {
                    Err(DowncastError::Value {
                        actual_type: value_type,
                        expected_type: expected_value,
                    })
                }
            }
            Err(bad_key) => Err(DowncastError::LinkKey {
                key: bad_key,
                expected_type: std::any::TypeId::of::<K>(),
            }),
        }
    }

    fn kind(&self) -> JoinLaneKind {
        JoinLaneKind::Value
    }
}
