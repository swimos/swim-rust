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

use super::{META_EDGE, META_HOST, META_MESH, META_NODE, META_PART};
use crate::agent::meta::MetaKind;
use percent_encoding::percent_decode_str;
use swim_common::warp::path::RelativePath;

fn iter(uri: &str) -> impl Iterator<Item = &str> {
    let lower_bound = if uri.starts_with('/') { 1 } else { 0 };

    let upper_bound = if uri.ends_with('/') {
        uri.len() - 1
    } else {
        uri.len()
    };

    uri[lower_bound..upper_bound].split('/').into_iter()
}

#[derive(Debug, PartialEq)]
pub struct MetaParseErr(String);

fn parse(node_uri: &str) -> Result<MetaKind, MetaParseErr> {
    if node_uri.is_empty() {
        return Err(MetaParseErr("Empty URI".to_string()));
    }

    let _original_uri = node_uri.clone();
    let mut iter = iter(node_uri);

    match iter.next() {
        Some(META_EDGE) => Ok(MetaKind::Edge),
        Some(META_MESH) => {
            unimplemented!()
        }
        Some(META_PART) => {
            unimplemented!()
        }
        Some(META_HOST) => {
            unimplemented!()
        }
        Some(META_NODE) => {
            let node_uri = iter
                .next()
                .map(|c| percent_decode_str(c).map(|c| c as char).collect::<String>());

            let lane = iter.next();
            let lane_uri = iter.next();

            match (node_uri, lane, lane_uri) {
                (Some(node_uri), Some("lane"), Some(lane_uri)) => Ok(MetaKind::Lane {
                    node_uri: node_uri.into(),
                    lane_uri: lane_uri.into(),
                }),
                (Some(node_uri), None, None) => Ok(MetaKind::Node(node_uri.into())),
                _ => Err(MetaParseErr("Malformatted URI".to_string())),
            }
        }
        Some(s) => Err(MetaParseErr(format!("Unknown URI: {}", s))),
        None => Err(MetaParseErr("Empty URI provided".to_string())),
    }
}

#[test]
fn test_iter() {
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
fn test_parse() {
    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/"),
        Ok(MetaKind::Node("unit/foo".into()))
    );
    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/lane/bar"),
        Ok(MetaKind::Lane {
            node_uri: "unit/foo".into(),
            lane_uri: "bar".into()
        })
    );
}
