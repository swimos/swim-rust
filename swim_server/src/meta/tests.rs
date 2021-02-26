// Copyright 2015-2021 SWIM.AI inc.
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
use crate::meta::uri::{iter, parse, MetaParseErr};
use crate::meta::{get_route, LaneAddressedKind, MetaNodeAddressed};
use std::convert::TryFrom;
use utilities::uri::RelativeUri;

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

#[test]
fn test_uri_iter() {
    let test = |uri| {
        let mut iter = iter(uri);
        assert_eq!(iter.next(), Some("swim:meta:node"));
        assert_eq!(iter.next(), Some("unit%2Ffoo"));
        assert_eq!(iter.next(), Some("lane"));
        assert_eq!(iter.next(), Some("bar"));
        assert_eq!(iter.next(), None);
    };

    test("swim:meta:node/unit%2Ffoo/lane/bar");
    test("/swim:meta:node/unit%2Ffoo/lane/bar");
}

#[test]
fn test_parse_node() {
    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo", "pulse"),
        Ok(MetaNodeAddressed::NodeProfile)
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/lane/bar", "uplink"),
        Ok(MetaNodeAddressed::UplinkProfile {
            lane_uri: "bar".into()
        })
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/lane/bar", "pulse"),
        Ok(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Pulse
        })
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/lane/bar", "traceLog"),
        Ok(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Log(LogLevel::Trace)
        })
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/", "lanes"),
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
            parse("swim:meta:node/unit%2Ffoo", level.uri_ref()),
            Ok(MetaNodeAddressed::NodeLog(level))
        );
    }
}

#[test]
fn test_parse_empty() {
    assert_eq!(parse("", ""), Err(MetaParseErr::EmptyUri))
}
