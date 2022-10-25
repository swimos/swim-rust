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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};

#[cfg(test)]
mod tests;

#[derive(Default, Debug)]
struct UplinkCounters {
    link_count: AtomicU64,
    event_count: AtomicU64,
    command_count: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct UplinkSnapshot {
    pub link_count: u64,
    pub event_count: u64,
    pub command_count: u64,
}

#[derive(Default, Debug, Clone)]
pub struct UplinkReporter {
    counters: Arc<UplinkCounters>,
}

#[derive(Default, Debug, Clone)]
pub struct UplinkReportReader {
    counters: Weak<UplinkCounters>,
}

fn saturating_incr(n: &AtomicU64) {
    let _ = n.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(1))
    });
}

fn snapshot_value(n: &AtomicU64) -> u64 {
    loop {
        let count = n.load(Ordering::Relaxed);
        if n.compare_exchange_weak(count, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break count;
        }
    }
}

impl UplinkReporter {
    pub fn count_event(&self) {
        saturating_incr(&self.counters.event_count)
    }

    pub fn count_command(&self) {
        saturating_incr(&self.counters.command_count)
    }

    pub fn incr_uplinks(&self) {
        self.counters
            .link_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("Uplink count overflow!"); // The number of uplinks should never become this large.
    }

    pub fn decr_uplinks(&self) {
        self.counters
            .link_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .expect("Uplink count underflow!"); // The number of uplinks cannot be less than 0.
    }

    pub fn reader(&self) -> UplinkReportReader {
        UplinkReportReader {
            counters: Arc::downgrade(&self.counters),
        }
    }
}

impl UplinkReportReader {
    pub fn snapshot(&self) -> Option<UplinkSnapshot> {
        self.counters.upgrade().map(|counters| {
            let link_count = counters.link_count.load(Ordering::Relaxed);
            let event_count = snapshot_value(&counters.event_count);
            let command_count = snapshot_value(&counters.command_count);
            UplinkSnapshot {
                link_count,
                event_count,
                command_count,
            }
        })
    }
}
