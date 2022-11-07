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

use std::time::Duration;

use swim_api::meta::uplink::WarpUplinkPulse;

use super::{UplinkReporter, UplinkSnapshot};

#[test]
fn empty_snapshot() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 0,
            event_count: 0,
            command_count: 0
        })
    );
}

#[test]
fn increment_events() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_events(1);

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 0,
            event_count: 1,
            command_count: 0
        })
    );
}

#[test]
fn increment_events_multiple() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_events(3);

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 0,
            event_count: 3,
            command_count: 0
        })
    );
}

#[test]
fn increment_commands() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_commands(1);

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 0,
            event_count: 0,
            command_count: 1
        })
    );
}

#[test]
fn increment_commands_multiple() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_commands(4);

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 0,
            event_count: 0,
            command_count: 4
        })
    );
}

#[test]
fn set_uplinks() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.set_uplinks(4);

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 4,
            event_count: 0,
            command_count: 0
        })
    );
}

#[test]
fn snapshot_resets_command_event_counts() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_events(2);
    reporter.count_commands(1);
    reporter.set_uplinks(3);

    let snapshot = reader.snapshot();
    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 3,
            event_count: 2,
            command_count: 1
        })
    );

    let snapshot = reader.snapshot();
    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 3,
            event_count: 0,
            command_count: 0
        })
    );
}

#[test]
fn simple_pulse() {
    let snapshot = UplinkSnapshot {
        link_count: 7,
        event_count: 12,
        command_count: 6483293,
    };

    let WarpUplinkPulse {
        link_count,
        event_rate,
        event_count,
        command_rate,
        command_count,
    } = snapshot.make_pulse(Duration::from_secs(1));

    assert_eq!(link_count, 7);
    assert_eq!(event_count, 12);
    assert_eq!(event_rate, 12);
    assert_eq!(command_count, 6483293);
    assert_eq!(command_rate, 6483293);
}

#[test]
fn long_pulse() {
    let snapshot = UplinkSnapshot {
        link_count: 7,
        event_count: 120,
        command_count: 6483297,
    };

    let WarpUplinkPulse {
        link_count,
        event_rate,
        event_count,
        command_rate,
        command_count,
    } = snapshot.make_pulse(Duration::from_secs(10));

    assert_eq!(link_count, 7);
    assert_eq!(event_count, 120);
    assert_eq!(event_rate, 12);
    assert_eq!(command_count, 6483297);
    assert_eq!(command_rate, 648329);
}

#[test]
fn saturating_pulse() {
    let n = u64::MAX / 10;
    let snapshot = UplinkSnapshot {
        link_count: 7,
        event_count: n,
        command_count: n,
    };

    let WarpUplinkPulse {
        link_count,
        event_rate,
        event_count,
        command_rate,
        command_count,
    } = snapshot.make_pulse(Duration::from_micros(2));

    assert_eq!(link_count, 7);
    assert_eq!(event_count, n);
    assert_eq!(event_rate, u64::MAX);
    assert_eq!(command_count, n);
    assert_eq!(command_rate, u64::MAX);
}

#[test]
fn zero_duration_saturates_rates() {
    let snapshot = UplinkSnapshot {
        link_count: 1,
        event_count: 12,
        command_count: 67,
    };

    let WarpUplinkPulse {
        link_count,
        event_rate,
        event_count,
        command_rate,
        command_count,
    } = snapshot.make_pulse(Duration::ZERO);

    assert_eq!(link_count, 1);
    assert_eq!(event_count, 12);
    assert_eq!(event_rate, u64::MAX);
    assert_eq!(command_count, 67);
    assert_eq!(command_rate, u64::MAX);
}

#[test]
fn sub_microsecond_duration_saturates_rates() {
    let snapshot = UplinkSnapshot {
        link_count: 1,
        event_count: 12,
        command_count: 67,
    };

    let WarpUplinkPulse {
        link_count,
        event_rate,
        event_count,
        command_rate,
        command_count,
    } = snapshot.make_pulse(Duration::from_nanos(123));

    assert_eq!(link_count, 1);
    assert_eq!(event_count, 12);
    assert_eq!(event_rate, u64::MAX);
    assert_eq!(command_count, 67);
    assert_eq!(command_rate, u64::MAX);
}
