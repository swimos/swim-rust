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

use swimos_api::lane::LaneKind;
use swimos_model::Text;
use swimos_runtime::agent::reporting::{UplinkReporter, UplinkSnapshot};

use crate::model::LaneView;

use super::{AgentIntrospectionUpdater, AgentSnapshot};

#[test]
fn snapshot_from_handle() {
    let reporter = UplinkReporter::default();
    let updater = AgentIntrospectionUpdater::new(reporter.reader());

    let lane_reporter = UplinkReporter::default();
    updater.add_lane(Text::new("lane"), LaneKind::Value, lane_reporter.reader());

    reporter.set_uplinks(2);
    reporter.count_events(46);
    reporter.count_commands(89);
    lane_reporter.set_uplinks(1);
    lane_reporter.count_events(22);
    lane_reporter.count_commands(879);

    let mut handle = updater.make_handle();
    let AgentSnapshot {
        lanes,
        aggregate_reporter,
    } = handle.new_snapshot().expect("Expected a snapshot.");

    assert_eq!(lanes.len(), 1);
    assert!(lanes.contains_key("lane"));
    let agg_snapshot = aggregate_reporter.snapshot().expect("Expected snapshot.");
    assert_eq!(
        agg_snapshot,
        UplinkSnapshot {
            link_count: 2,
            event_count: 46,
            command_count: 89
        }
    );

    let LaneView {
        kind,
        report_reader,
    } = &lanes["lane"];
    assert_eq!(kind, &LaneKind::Value);

    let lane_snapshot = report_reader.snapshot().expect("Expected snapshot.");
    assert_eq!(
        lane_snapshot,
        UplinkSnapshot {
            link_count: 1,
            event_count: 22,
            command_count: 879
        }
    );
}

#[test]
fn drop_reporter() {
    let reporter = UplinkReporter::default();
    let updater = AgentIntrospectionUpdater::new(reporter.reader());

    let mut handle = updater.make_handle();

    drop(reporter);

    assert!(handle.new_snapshot().is_none());
}
