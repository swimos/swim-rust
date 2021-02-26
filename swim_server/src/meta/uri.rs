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

use super::META_NODE;
use crate::meta::log::InvalidLogUri;
use crate::meta::MetaNodeAddressed;
use crate::meta::{LaneAddressedKind, LogLevel};
use percent_encoding::percent_decode_str;
use pin_utils::core_reexport::fmt::Formatter;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::Display;
use swim_common::model::text::Text;
use utilities::errors::Recoverable;

use crate::meta::{LANES_URI, LANE_URI, PULSE_URI, UPLINK_URI};

pub(in crate::meta) fn iter(uri: &str) -> impl Iterator<Item = &str> {
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
    UnknownNodeTarget,
    NotMetaAddressed,
    InvalidLogUri(InvalidLogUri),
}

impl Display for MetaParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaParseErr::EmptyUri => write!(f, "Empty node or lane URI provided"),
            MetaParseErr::UnknownNodeTarget => {
                write!(f, "The request URI does not match any resources")
            }
            MetaParseErr::NotMetaAddressed => write!(f, "The node URI is not node addressed"),
            MetaParseErr::InvalidLogUri(e) => {
                write!(f, "{}", e.0)
            }
        }
    }
}

impl Error for MetaParseErr {}

impl Recoverable for MetaParseErr {
    fn is_fatal(&self) -> bool {
        matches!(
            self,
            MetaParseErr::NotMetaAddressed | MetaParseErr::EmptyUri
        )
    }
}

fn decode_to_text(s: &str) -> Text {
    percent_decode_str(s)
        .map(|c| c as char)
        .collect::<String>()
        .into()
}

/// Parses `node_uri` and `lane_uri` into `MetaNodeAddressed`.
pub fn parse(node_uri: &str, lane_uri: &str) -> Result<MetaNodeAddressed, MetaParseErr> {
    if node_uri.is_empty() {
        return Err(MetaParseErr::EmptyUri);
    }

    let mut node_iter = iter(node_uri);
    let lane_iter = iter(lane_uri);

    match node_iter.next() {
        Some(META_NODE) => parse_node(node_iter, lane_iter),
        Some(_) => Err(MetaParseErr::NotMetaAddressed),
        None => {
            // unreachable as the split operation will return an empty string after finding no
            // matches in the input str.
            unreachable!()
        }
    }
}

fn parse_node<'c, L, R>(
    mut node_iter: L,
    mut lane_iter: R,
) -> Result<MetaNodeAddressed, MetaParseErr>
where
    L: Iterator<Item = &'c str>,
    R: Iterator<Item = &'c str>,
{
    let encoded_node_uri = node_iter.next();
    let lane = node_iter.next();
    let lane_uri = node_iter.next();

    match (encoded_node_uri, lane, lane_uri) {
        // Lane requests
        (Some(_), Some(LANE_URI), Some(lane_uri)) => match lane_iter.next() {
            // Uplink profile
            Some(UPLINK_URI) => Ok(MetaNodeAddressed::UplinkProfile {
                lane_uri: decode_to_text(lane_uri),
            }),
            // Lane pulse
            Some(target_lane) if target_lane == PULSE_URI => Ok(MetaNodeAddressed::LaneAddressed {
                lane_uri: lane_uri.into(),
                kind: LaneAddressedKind::Pulse,
            }),
            // Log lanes
            Some(target_lane) => match LogLevel::try_from(target_lane) {
                Ok(level) => Ok(MetaNodeAddressed::LaneAddressed {
                    lane_uri: lane_uri.into(),
                    kind: LaneAddressedKind::Log(level),
                }),
                _ => Err(MetaParseErr::UnknownNodeTarget),
            },
            _ => Err(MetaParseErr::UnknownNodeTarget),
        },
        // Node requests
        (Some(_), None, None) => match lane_iter.next() {
            // Pulse
            Some(PULSE_URI) => Ok(MetaNodeAddressed::NodeProfile),
            // Lanes
            Some(LANES_URI) => Ok(MetaNodeAddressed::Lanes),
            // Log lanes
            Some(log_uri) => {
                let level = LogLevel::try_from(log_uri).map_err(MetaParseErr::InvalidLogUri)?;

                Ok(MetaNodeAddressed::NodeLog(level))
            }
            None => Err(MetaParseErr::UnknownNodeTarget),
        },
        _ => Err(MetaParseErr::UnknownNodeTarget),
    }
}
