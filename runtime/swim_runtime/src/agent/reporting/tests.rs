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

    reporter.count_event();

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

    reporter.count_event();
    reporter.count_event();
    reporter.count_event();

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

    reporter.count_command();

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

    reporter.count_command();
    reporter.count_command();
    reporter.count_command();
    reporter.count_command();

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
fn increment_uplinks() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.incr_uplinks();

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 1,
            event_count: 0,
            command_count: 0
        })
    );
}

#[test]
fn increment_uplinks_multiple() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.incr_uplinks();
    reporter.incr_uplinks();
    reporter.incr_uplinks();

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
fn deccrement_uplinks() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.incr_uplinks();
    reporter.incr_uplinks();
    reporter.incr_uplinks();
    reporter.decr_uplinks();

    let snapshot = reader.snapshot();

    assert_eq!(
        snapshot,
        Some(UplinkSnapshot {
            link_count: 2,
            event_count: 0,
            command_count: 0
        })
    );
}

#[test]
fn snapshot_resets_command_event_counts() {
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();

    reporter.count_event();
    reporter.count_event();
    reporter.count_command();
    reporter.incr_uplinks();
    reporter.incr_uplinks();
    reporter.incr_uplinks();

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
