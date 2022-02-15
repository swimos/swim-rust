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

use super::{for_value_downlink, ValueDownlinkLifecycle};

#[test]
fn make_lifecycles() {
    let _basic = basic_lifecycle();
    let _with_handler = with_handler_lifecycle();
    let _with_handler2 = with_handler_lifecycle2();
    let _basic_stateful = stateful_lifecycle();
    let _blocking = with_blocking_handler_lifecycle();
}

fn basic_lifecycle() -> impl for<'a> ValueDownlinkLifecycle<'a, i32> {
    for_value_downlink::<i32>()
}

async fn handler(from: Option<&i32>, to: &i32) {
    if let Some(before) = from {
        println!("{} => {}", before, to);
    } else {
        println!("_ => {}", to);
    }
}

fn with_handler_lifecycle() -> impl for<'a> ValueDownlinkLifecycle<'a, i32> {
    for_value_downlink::<i32>().on_set(handler)
}

use crate::on_synced_handler;

fn with_handler_lifecycle2() -> impl for<'a> ValueDownlinkLifecycle<'a, i32> {
    for_value_downlink::<i32>().on_synced(on_synced_handler!(i32, |value| {
        println!("{}", value);
    }))
}

async fn handler_with_state(state: &mut String, from: Option<&i32>, to: &i32) {
    if let Some(before) = from {
        println!("{}: {} => {}", state, before, to);
    } else {
        println!("_ => {}", to);
    }
    *state = "Done".to_string();
}

fn stateful_lifecycle() -> impl for<'a> ValueDownlinkLifecycle<'a, i32> {
    for_value_downlink::<i32>()
        .with("Stuff".to_string())
        .on_set(handler_with_state)
}

fn with_blocking_handler_lifecycle() -> impl for<'a> ValueDownlinkLifecycle<'a, i32> {
    let mut m = 0;
    let mut n = 0;
    for_value_downlink::<i32>()
        .on_linked_blocking(move || {
            n += 1;
            println!("Linked {} times.", n);
        })
        .on_synced_blocking(move |v| {
            m += 1;
            println!("Synced {} times. Value = {}", m, v);
        })
}
