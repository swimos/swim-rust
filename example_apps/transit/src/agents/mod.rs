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

use std::fmt::Display;

use std::hash::Hash;
use swim::{
    agent::{
        agent_lifecycle::utility::JoinValueContext,
        lanes::join_value::{lifecycle::JoinValueLaneLifecycle, LinkClosedResponse},
    },
    form::Form,
};
use tracing::debug;

pub mod agency;
pub mod country;
pub mod state;
pub mod vehicle;

fn join_value_logging_lifecycle<Agent, K, I, V>(
    context: JoinValueContext<Agent, K, V>,
    to_id: impl Fn(K) -> I + Send + Copy + 'static,
    tag: &'static str,
) -> impl JoinValueLaneLifecycle<K, V, Agent> + 'static
where
    Agent: 'static,
    I: Display + Send + 'static,
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Form + Send + Display + 'static,
    V::Rec: Send,
{
    context
        .builder()
        .on_linked(move |context, key, _remote| {
            let id = to_id(key);
            context.effect(move || {
                debug!(id = %id, "{} lane linked to remote.", tag);
            })
        })
        .on_synced(move |context, key, _remote, value| {
            let id = to_id(key);
            let value = value.cloned();
            context.effect(move || {
                if let Some(value) = value {
                    debug!(id = %id, value = %value, "{} lane synced with remote.", tag);
                }
            })
        })
        .on_unlinked(move |context, key, _remote| {
            let id = to_id(key);
            context.effect(move || {
                debug!(id = %id, "{} lane unlinked from remote.", tag);
                LinkClosedResponse::Delete
            })
        })
        .on_failed(move |context, key, _remote| {
            let id = to_id(key);
            context.effect(move || {
                debug!(id = %id, "{} lane link to remote failed.", tag);
                LinkClosedResponse::Delete
            })
        })
        .done()
}
