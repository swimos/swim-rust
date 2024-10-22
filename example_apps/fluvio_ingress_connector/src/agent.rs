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

use std::collections::HashMap;
use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::{MapLane, ValueLane};
use swimos::agent::{lifecycle, AgentLaneModel};

/// Sensor Agent model.
#[derive(AgentLaneModel)]
pub struct SensorAgent {
    /// The latest temperature reading.
    temperature: ValueLane<i64>,
    /// The latest voltage reading.
    /// Key: timestamp that the key was updated.
    /// Value: voltage.
    voltage: MapLane<i64, f64>,
}

/// Sensor Agent lifecycle.
#[derive(Clone)]
pub struct SensorLifecycle;

#[lifecycle(SensorAgent)]
impl SensorLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<SensorAgent>) -> impl EventHandler<SensorAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(&self, context: HandlerContext<SensorAgent>) -> impl EventHandler<SensorAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_event(temperature)]
    pub fn on_temperature(
        &self,
        context: HandlerContext<SensorAgent>,
        value: &i64,
    ) -> impl EventHandler<SensorAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting temperature to: {}", n);
        })
    }

    #[on_update(voltage)]
    pub fn on_update(
        &self,
        context: HandlerContext<SensorAgent>,
        _map: &HashMap<i64, f64>,
        timestamp: i64,
        _prev: Option<f64>,
        new_value: &f64,
    ) -> impl EventHandler<SensorAgent> + '_ {
        let new_value = *new_value;
        context.effect(move || {
            println!("Setting voltage entry for {} to '{}'", timestamp, new_value);
        })
    }
}
