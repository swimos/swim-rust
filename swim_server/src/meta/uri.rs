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
use crate::meta::log::InvalidUri;
use crate::meta::MetaNodeAddressed;
use crate::meta::{LaneAddressedKind, LogLevel};
use percent_encoding::percent_decode_str;
use pin_utils::core_reexport::fmt::Formatter;
use std::convert::TryFrom;
use std::fmt::Display;
use swim_common::model::text::Text;
use thiserror::Error;
use utilities::errors::Recoverable;

use crate::meta::{LANES_URI, LANE_URI, PULSE_URI, UPLINK_URI};
use utilities::uri::{BadRelativeUri, RelativeUri};

/// Errors that are produced when parsing an invalid node or lane URI.
#[derive(Debug, PartialEq, Error)]
pub enum MetaParseErr {
    /// Either the provided node or lane URI failed to parse or the target does not exist.
    InvalidUri(InvalidUri),
    /// The URI was addressed in the format `swim:meta:node` but the target doesn't exist. Such as:
    /// `/swim:meta:node/lane/host`
    UnknownNodeTarget,
    /// The node URI's first segment is not `swim:meta:node`.
    NotMetaAddressed,
}

impl From<BadRelativeUri> for MetaParseErr {
    fn from(e: BadRelativeUri) -> Self {
        match e {
            BadRelativeUri::Invalid(m) => MetaParseErr::InvalidUri(InvalidUri(m.to_string())),
            BadRelativeUri::Absolute(u) => MetaParseErr::InvalidUri(InvalidUri(u.to_string())),
        }
    }
}

impl Display for MetaParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaParseErr::InvalidUri(msg) => msg.0.fmt(f),
            MetaParseErr::UnknownNodeTarget => {
                write!(f, "The request URI does not match any resources")
            }
            MetaParseErr::NotMetaAddressed => write!(f, "The node URI is not node addressed"),
        }
    }
}

impl Recoverable for MetaParseErr {
    fn is_fatal(&self) -> bool {
        matches!(
            self,
            MetaParseErr::NotMetaAddressed | MetaParseErr::InvalidUri(_)
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
pub fn parse(
    node_uri: RelativeUri,
    target_lane: impl AsRef<str>,
) -> Result<MetaNodeAddressed, MetaParseErr> {
    let mut node_iter = node_uri.path_iter();

    match node_iter.next() {
        Some(META_NODE) => parse_node(node_iter, target_lane.as_ref()),
        Some(_) => Err(MetaParseErr::NotMetaAddressed),
        None => {
            // unreachable as the split operation will return an empty string after finding no
            // matches in the input str.
            unreachable!()
        }
    }
}

fn parse_node<'c, L>(mut node_iter: L, target_lane: &str) -> Result<MetaNodeAddressed, MetaParseErr>
where
    L: Iterator<Item = &'c str>,
{
    let encoded_node_uri = node_iter.next();
    let lane = node_iter.next();
    let lane_uri = node_iter.next();

    match (encoded_node_uri, lane, lane_uri) {
        // Lane requests
        (Some(_), Some(LANE_URI), Some(lane_uri)) => match target_lane {
            // Uplink profile
            UPLINK_URI => Ok(MetaNodeAddressed::UplinkProfile {
                lane_uri: decode_to_text(lane_uri),
            }),
            // Lane pulse
            PULSE_URI => Ok(MetaNodeAddressed::LaneAddressed {
                lane_uri: lane_uri.into(),
                kind: LaneAddressedKind::Pulse,
            }),
            // Log lanes
            target_lane => match LogLevel::try_from(target_lane) {
                Ok(level) => Ok(MetaNodeAddressed::LaneAddressed {
                    lane_uri: lane_uri.into(),
                    kind: LaneAddressedKind::Log(level),
                }),
                _ => Err(MetaParseErr::UnknownNodeTarget),
            },
        },
        // Node requests
        (Some(_), None, None) => match target_lane {
            // Pulse
            PULSE_URI => Ok(MetaNodeAddressed::NodeProfile),
            // Lanes
            LANES_URI => Ok(MetaNodeAddressed::Lanes),
            // Log lanes
            log_uri => {
                let level = LogLevel::try_from(log_uri).map_err(MetaParseErr::InvalidUri)?;

                Ok(MetaNodeAddressed::NodeLog(level))
            }
        },
        _ => Err(MetaParseErr::UnknownNodeTarget),
    }
}
