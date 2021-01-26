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

use crate::agent::meta::log::{DEBUG_URI, ERROR_URI, FAIL_URI, INFO_URI, TRACE_URI, WARN_URI};
use crate::agent::meta::{LogLevel, MetaNodeAddressed};
use swim_common::warp::path::RelativePath;

#[test]
fn log_level_uri() {
    assert_eq!(LogLevel::Trace.uri_ref(), TRACE_URI);
    assert_eq!(LogLevel::Debug.uri_ref(), DEBUG_URI);
    assert_eq!(LogLevel::Info.uri_ref(), INFO_URI);
    assert_eq!(LogLevel::Warn.uri_ref(), WARN_URI);
    assert_eq!(LogLevel::Error.uri_ref(), ERROR_URI);
    assert_eq!(LogLevel::Fail.uri_ref(), FAIL_URI);
}

#[test]
fn node_decoded_paths() {
    let node = MetaNodeAddressed::NodeProfile {
        node_uri: "unit/foo".into(),
    };
    assert_eq!(
        node.decoded_relative_path(),
        RelativePath::new("/unit/foo", "/pulse")
    );

    let node = MetaNodeAddressed::UplinkProfile {
        node_uri: "unit/foo".into(),
        lane_uri: "bar".into(),
    };
    assert_eq!(
        node.decoded_relative_path(),
        RelativePath::new("/unit/foo", "/lane/bar/uplink")
    );

    let node = MetaNodeAddressed::Lanes {
        node_uri: "unit/foo".into(),
    };
    assert_eq!(
        node.decoded_relative_path(),
        RelativePath::new("/unit/foo", "/lanes")
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
        let node = MetaNodeAddressed::Log {
            node_uri: "unit/foo".into(),
            level,
        };

        assert_eq!(
            node.decoded_relative_path(),
            RelativePath::new("/unit/foo", &format!("/{}", level.uri_ref()))
        );
    }
}
