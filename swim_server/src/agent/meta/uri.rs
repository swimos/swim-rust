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
use crate::agent::meta::log::InvalidLogUri;
use crate::agent::meta::{LogLevel, MetaAddressed, MetaNodeAddressed};
use percent_encoding::percent_decode_str;
use pin_utils::core_reexport::fmt::Formatter;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::Display;
use swim_common::model::text::Text;
use utilities::errors::Recoverable;

const LANE_PART: &str = "lane";
const LANES_PART: &str = "lanes";
const PULSE_PART: &str = "pulse";
const UPLINK_PART: &str = "uplink";

fn iter(uri: &str) -> impl Iterator<Item = &str> {
    let lower_bound = if uri.starts_with('/') { 1 } else { 0 };

    let upper_bound = if uri.ends_with('/') {
        uri.len() - 1
    } else {
        uri.len()
    };

    uri[lower_bound..upper_bound].split('/')
}

#[derive(Debug, PartialEq)]
pub enum MetaParseErr {
    EmptyUri,
    UnknownTarget,
    InvalidLogUri(InvalidLogUri),
}

impl Display for MetaParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaParseErr::EmptyUri => write!(f, "Empty URI provided"),
            MetaParseErr::UnknownTarget => {
                write!(f, "The request URI does not match any resources")
            }
            MetaParseErr::InvalidLogUri(e) => {
                write!(f, "{}", e.0)
            }
        }
    }
}

impl Error for MetaParseErr {}

impl Recoverable for MetaParseErr {
    fn is_fatal(&self) -> bool {
        !matches!(self, MetaParseErr::EmptyUri)
    }
}

fn decode_to_text(s: &str) -> Text {
    percent_decode_str(s)
        .map(|c| c as char)
        .collect::<String>()
        .into()
}

pub fn parse(node_uri: &str, lane_uri: &str) -> Result<MetaAddressed, MetaParseErr> {
    if node_uri.is_empty() {
        return Err(MetaParseErr::EmptyUri);
    }

    let mut node_iter = iter(node_uri);
    let lane_iter = iter(lane_uri);

    match node_iter.next() {
        Some(META_EDGE) => Ok(MetaAddressed::Edge),
        Some(META_MESH) => Ok(MetaAddressed::Mesh),
        Some(META_PART) => Ok(MetaAddressed::Part),
        Some(META_HOST) => Ok(MetaAddressed::Host),
        Some(META_NODE) => parse_node(node_iter, lane_iter).map(MetaAddressed::Node),
        Some(_) => Err(MetaParseErr::UnknownTarget),
        None => {
            // unreachable as the split operation will return an empty string after finding no
            // matches in the input str.
            unreachable!()
        }
    }
}

fn parse_node<'c, L: Iterator<Item = &'c str>, R: Iterator<Item = &'c str>>(
    mut node_iter: L,
    mut lane_iter: R,
) -> Result<MetaNodeAddressed, MetaParseErr> {
    let encoded_node_uri = node_iter.next();
    let lane = node_iter.next();
    let lane_uri = node_iter.next();

    match (encoded_node_uri, lane, lane_uri) {
        // Uplink request
        (Some(node_uri), Some(LANE_PART), Some(lane_uri)) => match lane_iter.next() {
            Some(UPLINK_PART) => Ok(MetaNodeAddressed::UplinkProfile {
                node_uri: decode_to_text(node_uri),
                lane_uri: decode_to_text(lane_uri),
            }),
            _ => Err(MetaParseErr::UnknownTarget),
        },
        // Node requests
        (Some(node_uri), None, None) => match lane_iter.next() {
            // Node pulse
            Some(PULSE_PART) => Ok(MetaNodeAddressed::NodeProfile {
                node_uri: decode_to_text(node_uri),
            }),
            Some(LANES_PART) => Ok(MetaNodeAddressed::Lanes {
                node_uri: decode_to_text(node_uri),
            }),
            Some(log_uri) => {
                let level = LogLevel::try_from(log_uri).map_err(MetaParseErr::InvalidLogUri)?;

                Ok(MetaNodeAddressed::Log {
                    node_uri: decode_to_text(node_uri),
                    level,
                })
            }
            None => Err(MetaParseErr::UnknownTarget),
        },
        _ => Err(MetaParseErr::UnknownTarget),
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
fn test_parse_node() {
    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo", "pulse"),
        Ok(MetaAddressed::Node(MetaNodeAddressed::NodeProfile {
            node_uri: "unit/foo".into(),
        }))
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/lane/bar", "uplink"),
        Ok(MetaAddressed::Node(MetaNodeAddressed::UplinkProfile {
            node_uri: "unit/foo".into(),
            lane_uri: "bar".into()
        }))
    );

    assert_eq!(
        parse("swim:meta:node/unit%2Ffoo/", "lanes"),
        Ok(MetaAddressed::Node(MetaNodeAddressed::Lanes {
            node_uri: "unit/foo".into(),
        }))
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
            Ok(MetaAddressed::Node(MetaNodeAddressed::Log {
                node_uri: "unit/foo".into(),
                level
            }))
        );
    }
}

#[test]
fn test_parse_edge() {
    assert_eq!(parse("swim:meta:edge", ""), Ok(MetaAddressed::Edge))
}

#[test]
fn test_parse_mesh() {
    assert_eq!(parse("swim:meta:mesh", ""), Ok(MetaAddressed::Mesh))
}

#[test]
fn test_parse_part() {
    assert_eq!(parse("swim:meta:part", ""), Ok(MetaAddressed::Part))
}

#[test]
fn test_parse_host() {
    assert_eq!(parse("swim:meta:host", ""), Ok(MetaAddressed::Host))
}

#[test]
fn test_parse_unknown() {
    assert_eq!(
        parse("swim:meta:unknown", ""),
        Err(MetaParseErr::UnknownTarget)
    )
}

#[test]
fn test_parse_empty() {
    assert_eq!(parse("", ""), Err(MetaParseErr::EmptyUri))
}
