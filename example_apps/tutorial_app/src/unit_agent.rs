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

use std::{collections::HashMap, time::Duration};

use rand::Rng;
use swim::{
    agent::{
        agent_lifecycle::utility::HandlerContext,
        event_handler::{EventHandler, HandlerActionExt, Sequentially},
        lanes::{CommandLane, MapLane, ValueLane},
        lifecycle, projections,
        state::History,
        AgentLaneModel,
    },
    model::time::Timestamp,
};
use tutorial_app_model::{Message, HistoryItem, Counter};

#[derive(AgentLaneModel)]
#[projections]
pub struct UnitAgent {
    publish: CommandLane<Message>,
    history: ValueLane<Vec<HistoryItem>>,
    #[transient]
    histogram: MapLane<i64, Counter>,
    last: ValueLane<Option<Message>>,
}

#[derive(Debug)]
pub struct ExampleLifecycle {
    accumulator: History<UnitAgent, HistoryItem>,
}

impl ExampleLifecycle {
    pub fn new(max_size: usize) -> Self {
        ExampleLifecycle {
            accumulator: History::new(max_size),
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
        let set = context.set_value(UnitAgent::LAST, Some(*message));
        print.followed_by(set)
    }

    #[on_set(last)]
    pub fn last_updated(
        &self,
        context: HandlerContext<UnitAgent>,
        new_message: &Option<Message>,
        previous: Option<Option<Message>>,
    ) -> impl EventHandler<UnitAgent> + '_ {
        (*new_message)
            .map(move |message| {
                let print = context.effect(move || {
                    if let Some(prev) = previous.flatten() {
                        println!("Last set from {:?} to {:?}.", prev, message);
                    } else {
                        println!("Last set to {:?}.", message);
                    }
                });
                let item = HistoryItem::new(message);
                let add_to_history = update_history(&self.accumulator, context, item);
                let update_hist = update_histogram(context, item);
                print.followed_by(add_to_history).followed_by(update_hist)
            })
            .discard()
    }

    #[on_update(histogram)]
    fn maintain_histogram(
        &self,
        context: HandlerContext<UnitAgent>,
        map: &HashMap<i64, Counter>,
        _key: i64,
        _prev: Option<Counter>,
        _new_value: &Counter,
    ) -> impl EventHandler<UnitAgent> + 'static {
        remove_old(context, map)
    }
}

fn update_history(
    history: &History<UnitAgent, HistoryItem>,
    context: HandlerContext<UnitAgent>,
    item: HistoryItem,
) -> impl EventHandler<UnitAgent> + '_ {
    let append = history.push(item.clone());
    let update_history = history.and_then_with(move |hist| {
        context.set_value(UnitAgent::HISTORY, hist.iter().cloned().collect())
    });
    append.followed_by(update_history)
}

fn update_histogram(
    context: HandlerContext<UnitAgent>,
    item: HistoryItem,
) -> impl EventHandler<UnitAgent> {
    let bucket = item.timestamp.nanos() / (5000 * 5000);
    context.with_entry(UnitAgent::HISTOGRAM, bucket, |maybe| {
        let mut counter = maybe.unwrap_or_default();
        counter.count += rand::thread_rng().gen_range(0..20);
        Some(counter)
    })
}

const TWO_MINS_NS: i64 = Duration::from_secs(2 * 60).as_nanos() as i64;

fn remove_old(
    context: HandlerContext<UnitAgent>,
    map: &HashMap<i64, Counter>,
) -> impl EventHandler<UnitAgent> + 'static {
    let now = Timestamp::now().nanos();
    let removals = map
        .keys()
        .filter(move |key| (now - **key) > TWO_MINS_NS)
        .map(move |k| context.remove(UnitAgent::HISTOGRAM, *k))
        .collect::<Vec<_>>();
    Sequentially::new(removals)
}
