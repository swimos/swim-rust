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

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use swim_api::meta::uplink::WarpUplinkPulse;

#[cfg(test)]
mod tests;

/// A number of metrics that can be reported to the reproting system. This can be done
/// at the level of an individual lane or in aggregate for the complete agent.
///
/// #Metrics
/// * The number of active uplinks for the lane/agent.
/// * The number of events that were generated by the lane/agent since the last snapshot was taken.
/// * The number of commands received by the lane/agent since the last snapshot was taken.
#[derive(Default, Debug)]
struct UplinkCounters {
    link_count: AtomicU64,
    event_count: AtomicU64,
    command_count: AtomicU64,
}

/// A snapshot taken from an [`UplinkCounters`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct UplinkSnapshot {
    pub link_count: u64,
    pub event_count: u64,
    pub command_count: u64,
}

impl UplinkSnapshot {
    /// Create an uplink pulse from the snapshot. This appends a rate (in items/s) for the event
    /// and command count, based on the duration that has elapsed since the previous snapshot.
    pub fn make_pulse(&self, diff: Duration) -> WarpUplinkPulse {
        let UplinkSnapshot {
            link_count,
            event_count,
            command_count,
        } = *self;
        let micros = diff.as_micros();

        if micros > 0 {
            let event_rate =
                u64::try_from(((event_count as u128).saturating_mul(1000000)) / micros)
                    .unwrap_or(u64::MAX);
            let command_rate =
                u64::try_from(((command_count as u128).saturating_mul(1000000)) / micros)
                    .unwrap_or(u64::MAX);
            WarpUplinkPulse {
                link_count,
                event_rate,
                event_count,
                command_rate,
                command_count,
            }
        } else {
            WarpUplinkPulse {
                link_count,
                event_rate: u64::MAX,
                event_count,
                command_rate: u64::MAX,
                command_count,
            }
        }
    }
}

/// Allows an agent to report metrics back to the metrics reporting sytem.  The consumer
/// can take a snapshot which will consume the value of some of the counters.
#[derive(Default, Debug, Clone)]
pub struct UplinkReporter {
    counters: Arc<UplinkCounters>,
}

/// A cosumer attached to an [`UplinkReporter`]. When the corresponding reporter is dropped, this
/// will become invalidated and all future snapshot calls will return nothing.
#[derive(Debug, Clone)]
pub struct UplinkReportReader {
    counters: Weak<UplinkCounters>,
}

fn saturating_add(n: &AtomicU64, m: u64) {
    let _ = n.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(m))
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
    /// Increment the count of events by the given amount (this will saturate).
    pub fn count_events(&self, n: u64) {
        saturating_add(&self.counters.event_count, n)
    }

    /// Increment the count of commands by the given amount (this will saturate).
    pub fn count_commands(&self, n: u64) {
        saturating_add(&self.counters.command_count, n)
    }

    /// Set the number of active uplinks.
    pub fn set_uplinks(&self, n: u64) {
        self.counters.link_count.store(n, Ordering::Relaxed);
    }

    /// Create a reader attached to this reporter.
    pub fn reader(&self) -> UplinkReportReader {
        UplinkReportReader {
            counters: Arc::downgrade(&self.counters),
        }
    }
}

impl UplinkReportReader {
    /// This will return true if and only if the coorresponding reporter has not yet been dropped.
    pub fn is_active(&self) -> bool {
        self.counters.upgrade().is_some()
    }

    /// Create a snapshot of the current state. This will read the value of the number of uplinks
    /// and consume the counts of events and commands (setting the new values back to 0). If
    /// the reporter to which this reader is attached has been dropped, this will return nothing.
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
