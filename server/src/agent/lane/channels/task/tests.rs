// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::update::UpdateError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::UplinkError;
use crate::routing::RoutingAddr;
use common::warp::path::RelativePath;
use swim_form::FormDeserializeErr;

#[test]
fn lane_io_err_display_update() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_update_err(
        route,
        UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted),
    );

    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelops was invalid: Malformatted"
        ]
    );
}

#[test]
fn lane_io_err_display_uplink() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_uplink_errors(
        route,
        vec![UplinkErrorReport {
            error: UplinkError::SenderDropped,
            addr: RoutingAddr::remote(1),
        }],
    );
    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- uplink_errors =",
            "* Uplink to Remote Endpoint (1) failed: Uplink send channel was dropped."
        ]
    );
}

#[test]
fn lane_io_err_display_both() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::new(
        route,
        UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted),
        vec![UplinkErrorReport {
            error: UplinkError::SenderDropped,
            addr: RoutingAddr::remote(1),
        }],
    );
    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelops was invalid: Malformatted",
            "- uplink_errors =",
            "* Uplink to Remote Endpoint (1) failed: Uplink send channel was dropped."
        ]
    );
}
