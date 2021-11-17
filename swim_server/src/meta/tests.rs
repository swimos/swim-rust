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

use crate::meta::log::LogLevel;
use crate::meta::uri::{parse, MetaParseErr};
use crate::meta::{get_route, LaneAddressedKind, MetaNodeAddressed};
use std::convert::TryFrom;
use std::str::FromStr;
use swim_utilities::routing::uri::RelativeUri;

#[test]
fn test_parse_paths() {
    let meta_types = vec!["edge", "mesh", "part", "host", "node"];
    let expected = RelativeUri::new(http::Uri::try_from("/unit/foo/").unwrap()).unwrap();

    for meta in meta_types {
        let input = format!("/swim:meta:{}/unit%2Ffoo/lane/bar/uplink", meta);
        let path = RelativeUri::new(http::Uri::try_from(input).unwrap()).unwrap();

        assert_eq!(get_route(path), expected)
    }

    assert_eq!(get_route(expected.clone()), expected)
}

fn parse_meta(node: &str, lane: &str) -> Result<MetaNodeAddressed, MetaParseErr> {
    let node = RelativeUri::from_str(node).expect("Failed to parse node URI");
    parse(node, lane)
}

#[test]
fn test_parse_node_addressed() {
    assert_eq!(
        parse_meta("/swim:meta:node/unit%2Ffoo", "pulse"),
        Ok(MetaNodeAddressed::NodeProfile)
    );

    assert_eq!(
        parse_meta("/swim:meta:node/unit%2Ffoo/lane/bar", "uplink"),
        Ok(MetaNodeAddressed::UplinkProfile {
            lane_uri: "bar".into()
        })
    );

    assert_eq!(
        parse_meta("/swim:meta:node/unit%2Ffoo/lane/bar", "pulse"),
        Ok(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Pulse
        })
    );

    assert_eq!(
        parse_meta("/swim:meta:node/unit%2Ffoo/lane/bar", "traceLog"),
        Ok(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Log(LogLevel::Trace)
        })
    );

    assert_eq!(
        parse_meta("/swim:meta:node/unit%2Ffoo/", "lanes"),
        Ok(MetaNodeAddressed::Lanes)
    );

    let log_levels = vec![
        LogLevel::Trace,
        LogLevel::Debug,
        LogLevel::Info,
        LogLevel::Warn,
        LogLevel::Error,
        LogLevel::Fail,
    ];

    for level in log_levels {
        assert_eq!(
            parse_meta("/swim:meta:node/unit%2Ffoo", level.uri_ref()),
            Ok(MetaNodeAddressed::NodeLog(level))
        );
    }
}

#[test]
fn node_addressed_display() {
    let node_profile = MetaNodeAddressed::NodeProfile;
    assert_eq!(format!("{}", node_profile), format!("NodePulse"));

    let uplink_profile = MetaNodeAddressed::UplinkProfile {
        lane_uri: "/bar".into(),
    };
    assert_eq!(
        format!("{}", uplink_profile),
        format!("UplinkProfile(lane_uri: \"/bar\")")
    );

    let lane_addressed_pulse = MetaNodeAddressed::LaneAddressed {
        lane_uri: "/bar".into(),
        kind: LaneAddressedKind::Pulse,
    };
    assert_eq!(
        format!("{}", lane_addressed_pulse),
        format!("Lane(lane_uri: \"/bar\", kind: Pulse)")
    );
    let lane_addressed_log = MetaNodeAddressed::LaneAddressed {
        lane_uri: "/bar".into(),
        kind: LaneAddressedKind::Log(LogLevel::Trace),
    };
    assert_eq!(
        format!("{}", lane_addressed_log),
        format!("Lane(lane_uri: \"/bar\", kind: Log(level: Trace))")
    );

    let lanes = MetaNodeAddressed::Lanes;
    assert_eq!(format!("{}", lanes), format!("Lanes"));

    let log = MetaNodeAddressed::NodeLog(LogLevel::Trace);
    assert_eq!(format!("{}", log), format!("Log(level: Trace)"));
}
