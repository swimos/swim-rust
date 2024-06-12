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

use std::{cell::Cell, collections::HashMap, time::Duration};

use rand::Rng;
use swimos::{
    agent::{
        agent_lifecycle::utility::HandlerContext,
        event_handler::{EventHandler, HandlerActionExt, Sequentially},
        lanes::{CommandLane, MapLane, ValueLane},
        lifecycle, projections, AgentLaneModel,
    },
    model::Timestamp,
};
use tutorial_app_model::{Counter, HistoryItem, Message};

#[derive(AgentLaneModel)]
#[projections]
pub struct UnitAgent {
    publish: CommandLane<Message>,
    history: MapLane<usize, HistoryItem>,
    #[item(transient)]
    histogram: MapLane<i64, Counter>,
    latest: ValueLane<Option<Message>>,
}

#[derive(Debug)]
pub struct ExampleLifecycle {
    max_history: usize,
    epoch: Cell<usize>,
}

impl ExampleLifecycle {
    pub fn new(max_history: usize) -> Self {
        ExampleLifecycle {
            max_history,
            epoch: Cell::new(0),
        }
    }
}

#[lifecycle(UnitAgent, no_clone)]
impl ExampleLifecycle {
    #[on_command(publish)]
    pub fn publish_message(
        &self,
        context: HandlerContext<UnitAgent>,
        message: &Message,
    ) -> impl EventHandler<UnitAgent> {
        let log_msg = format!("Commanded with: {:?}", message);
        let print = context.effect(move || {
            println!("{}", log_msg);
        });
        let set = context.set_value(UnitAgent::LATEST, Some(*message));
        print.followed_by(set)
    }

    #[on_set(latest)]
    pub fn latest_updated(
        &self,
        context: HandlerContext<UnitAgent>,
        new_message: &Option<Message>,
        previous: Option<Option<Message>>,
    ) -> impl EventHandler<UnitAgent> + '_ {
        (*new_message)
            .map(move |message| {
                let print = context.effect(move || {
                    if let Some(prev) = previous.flatten() {
                        println!("Latest set from {:?} to {:?}.", prev, message);
                    } else {
                        println!("Last set to {:?}.", message);
                    }
                });
                let item = HistoryItem::new(message);
                let e = self.epoch.get();
                self.epoch.set(e + 1);
                let add_to_history = context.update(UnitAgent::HISTORY, e, item);
                let update_hist = update_histogram(context, item);
                print.followed_by(add_to_history).followed_by(update_hist)
            })
            .discard()
    }

    #[on_update(history)]
    fn maintain_history(
        &self,
        context: HandlerContext<UnitAgent>,
        map: &HashMap<usize, HistoryItem>,
        _key: usize,
        _prev: Option<HistoryItem>,
        _new_value: &HistoryItem,
    ) -> impl EventHandler<UnitAgent> {
        truncate_history(map, self.max_history, self.epoch.get(), context)
    }

    #[on_update(histogram)]
    fn maintain_histogram(
        &self,
        context: HandlerContext<UnitAgent>,
        map: &HashMap<i64, Counter>,
        _key: i64,
        _prev: Option<Counter>,
        _new_value: &Counter,
    ) -> impl EventHandler<UnitAgent> {
        remove_old(context, map)
    }
}

fn truncate_history(
    map: &HashMap<usize, HistoryItem>,
    max_history: usize,
    epoch: usize,
    context: HandlerContext<UnitAgent>,
) -> impl EventHandler<UnitAgent> {
    let cut_off = epoch.saturating_sub(max_history);
    let to_remove = map
        .keys()
        .filter(move |k| **k < cut_off)
        .copied()
        .collect::<Vec<_>>();
    let len = map.len();
    let n = to_remove.len();
    let print = context.effect(move || {
        if n > 0 {
            println!(
                "History has {} elements. Truncating to {}.",
                len, max_history
            );
        }
    });
    let truncate = Sequentially::new(
        to_remove
            .into_iter()
            .map(move |k| context.remove(UnitAgent::HISTORY, k)),
    );
    print.followed_by(truncate)
}

fn update_histogram(
    context: HandlerContext<UnitAgent>,
    item: HistoryItem,
) -> impl EventHandler<UnitAgent> {
    let bucket = bucket_of(&item.timestamp);
    context.with_entry(UnitAgent::HISTOGRAM, bucket, |maybe| {
        let mut counter = maybe.unwrap_or_default();
        counter.count += rand::thread_rng().gen_range(0..20);
        Some(counter)
    })
}

const TWO_MINS_MS: i64 = Duration::from_secs(2 * 60).as_millis() as i64;

fn bucket_of(timestamp: &Timestamp) -> i64 {
    (timestamp.millis() / 5000) * 5000
}

fn remove_old(
    context: HandlerContext<UnitAgent>,
    map: &HashMap<i64, Counter>,
) -> impl EventHandler<UnitAgent> + 'static {
    let now = Timestamp::now().millis();
    let removals = map
        .keys()
        .filter(move |key| (now - **key) > TWO_MINS_MS)
        .map(move |k| context.remove(UnitAgent::HISTOGRAM, *k))
        .collect::<Vec<_>>();
    Sequentially::new(removals)
}
