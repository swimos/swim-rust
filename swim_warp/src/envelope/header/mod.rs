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

use std::{borrow::Cow, fmt::Display, num::ParseFloatError};

use super::EnvelopeKind;
use smallvec::{smallvec, SmallVec};
use swim_recon::parser::{parse_text, try_extract_header, HeaderPeeler, MessageExtractError, Span};
use swim_utilities::format::comma_sep;
use thiserror::Error;

#[cfg(test)]
mod tests;

/// Interpreted form of a warp envelope, taken from a recon encoded string. Wherever possible, string
/// references point back into the original string (the exception being string literals that need
/// to be unescaped).
#[derive(Debug)]
pub enum RawEnvelope<'a> {
    Auth(Span<'a>),
    DeAuth(Span<'a>),
    Link {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        rate: Option<f32>,
        prio: Option<f32>,
        body: Span<'a>,
    },
    Sync {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        rate: Option<f32>,
        prio: Option<f32>,
        body: Span<'a>,
    },
    Command {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        body: Span<'a>,
    },
    Unlink {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        body: Span<'a>,
    },
    Linked {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        rate: Option<f32>,
        prio: Option<f32>,
        body: Span<'a>,
    },
    Synced {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        body: Span<'a>,
    },
    Event {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        body: Span<'a>,
    },
    Unlinked {
        node_uri: Cow<'a, str>,
        lane_uri: Cow<'a, str>,
        body: Span<'a>,
    },
}

/// A list of missing slots.
#[derive(Debug, Clone)]
pub struct Missing(SmallVec<[&'static str; 2]>);

impl Missing {
    fn single(name: &'static str) -> Self {
        Missing(smallvec!(name))
    }

    fn two(first: &'static str, second: &'static str) -> Self {
        Missing(smallvec!(first, second))
    }
}

impl Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", comma_sep(&self.0))
    }
}

/// Possible errors that can ocurr when attempting to interpret the header of a warp envelope.
#[derive(Debug, Error, Clone)]
pub enum HeaderExtractionError {
    #[error("Invalid tag name: '{0}'")]
    InvalidTag(String),
    #[error("Unexpected value item in header: '{0}'")]
    UnexpectedHeaderValue(String),
    #[error("Unexpected slot in header: '{name} = {value}'")]
    UnexpectedHeaderSlot { name: String, value: String },
    #[error("'{0}' cannot be interpreted as a recon string.")]
    InvalidString(String),
    #[error("Expecting a floating point number.")]
    InvalidFloat(#[from] ParseFloatError),
    #[error("The input did not contain an envelope header.")]
    Incomplete,
    #[error("Header had missing slots: {0}")]
    MissingSlots(Missing),
}

/// Try to interpret an array of bytes as a warp envelope, without allocating.
pub fn peel_envelope_header(input: &[u8]) -> Result<RawEnvelope<'_>, MessageExtractError> {
    try_extract_header(input, EnvelopeHeaderPeeler::default())
}

#[derive(Debug, Clone, Copy, Default)]
struct EnvelopeHeaderPeeler<'a> {
    kind: Option<EnvelopeKind>,
    node_uri: Option<&'a str>,
    lane_uri: Option<&'a str>,
    rate: Option<f32>,
    prio: Option<f32>,
}

fn with_path<'a, F>(
    node_uri: Option<&'a str>,
    lane_uri: Option<&'a str>,
    body: Span<'a>,
    constructor: F,
) -> Result<RawEnvelope<'a>, HeaderExtractionError>
where
    F: FnOnce(Cow<'a, str>, Cow<'a, str>, Span<'a>) -> RawEnvelope<'a>,
{
    match (node_uri, lane_uri) {
        (Some(node_uri), Some(lane_uri)) => {
            let node_uri_text = if let Ok(node_uri_text) = parse_text(Span::new(node_uri)) {
                node_uri_text
            } else {
                return Err(HeaderExtractionError::InvalidString(node_uri.to_string()));
            };
            let lane_uri_text = if let Ok(lane_uri_text) = parse_text(Span::new(lane_uri)) {
                lane_uri_text
            } else {
                return Err(HeaderExtractionError::InvalidString(node_uri.to_string()));
            };
            Ok(constructor(node_uri_text, lane_uri_text, body))
        }
        (_, Some(_)) => Err(HeaderExtractionError::MissingSlots(Missing::single(
            NODE_URI_SLOT,
        ))),
        (Some(_), _) => Err(HeaderExtractionError::MissingSlots(Missing::single(
            LANE_URI_SLOT,
        ))),
        _ => Err(HeaderExtractionError::MissingSlots(Missing::two(
            NODE_URI_SLOT,
            LANE_URI_SLOT,
        ))),
    }
}

fn with_rate_prio<'a, F>(
    node_uri: Option<&'a str>,
    lane_uri: Option<&'a str>,
    rate: Option<f32>,
    prio: Option<f32>,
    body: Span<'a>,
    constructor: F,
) -> Result<RawEnvelope<'a>, HeaderExtractionError>
where
    F: FnOnce(Cow<'a, str>, Cow<'a, str>, Option<f32>, Option<f32>, Span<'a>) -> RawEnvelope<'a>,
{
    let attach_rate_prio =
        move |node_uri, lane_uri, body| constructor(node_uri, lane_uri, rate, prio, body);
    with_path(node_uri, lane_uri, body, attach_rate_prio)
}

const AUTH_TAG: &str = "auth";
const DEAUTH_TAG: &str = "deauth";
const LINK_TAG: &str = "link";
const SYNC_TAG: &str = "sync";
const COMMAND_TAG: &str = "command";
const UNLINK_TAG: &str = "unlink";
const LINKED_TAG: &str = "linked";
const SYNCED_TAG: &str = "synced";
const EVENT_TAG: &str = "event";
const UNLINKED_TAG: &str = "unlinked";

const LANE_URI_SLOT: &str = "lane";
const NODE_URI_SLOT: &str = "node";
const RATE_SLOT: &str = "rate";
const PRIO_SLOT: &str = "prio";

impl<'a> HeaderPeeler<'a> for EnvelopeHeaderPeeler<'a> {
    type Output = RawEnvelope<'a>;

    type Error = HeaderExtractionError;

    fn tag(mut self, name: &str) -> Result<Self, Self::Error> {
        self.kind = Some(match name {
            AUTH_TAG => EnvelopeKind::Auth,
            DEAUTH_TAG => EnvelopeKind::DeAuth,
            LINK_TAG => EnvelopeKind::Link,
            SYNC_TAG => EnvelopeKind::Sync,
            COMMAND_TAG => EnvelopeKind::Command,
            UNLINK_TAG => EnvelopeKind::Unlink,
            LINKED_TAG => EnvelopeKind::Linked,
            SYNCED_TAG => EnvelopeKind::Synced,
            EVENT_TAG => EnvelopeKind::Event,
            UNLINKED_TAG => EnvelopeKind::Unlinked,
            ow => {
                return Err(HeaderExtractionError::InvalidTag(ow.to_string()));
            }
        });
        Ok(self)
    }

    fn feed_header_slot(mut self, name: &str, value: Span<'a>) -> Result<Self, Self::Error> {
        match name {
            NODE_URI_SLOT => {
                self.node_uri = Some(*value);
            }
            LANE_URI_SLOT => {
                self.lane_uri = Some(*value);
            }
            RATE_SLOT => {
                self.rate = Some(value.parse()?);
            }
            PRIO_SLOT => {
                self.prio = Some(value.parse()?);
            }
            _ => {
                return Err(HeaderExtractionError::UnexpectedHeaderSlot {
                    name: name.to_string(),
                    value: value.to_string(),
                });
            }
        }
        Ok(self)
    }

    fn feed_header_value(self, value: Span<'a>) -> Result<Self, Self::Error> {
        Err(HeaderExtractionError::UnexpectedHeaderValue(
            value.to_string(),
        ))
    }

    fn feed_header_extant(self) -> Result<Self, Self::Error> {
        Err(HeaderExtractionError::UnexpectedHeaderValue("".to_string()))
    }

    fn done(self, body: Span<'a>) -> Result<Self::Output, Self::Error> {
        let EnvelopeHeaderPeeler {
            kind,
            node_uri,
            lane_uri,
            rate,
            prio,
        } = self;

        if let Some(kind) = kind {
            match kind {
                EnvelopeKind::Auth => Ok(RawEnvelope::Auth(body)),
                EnvelopeKind::DeAuth => Ok(RawEnvelope::DeAuth(body)),
                EnvelopeKind::Link => with_rate_prio(
                    node_uri,
                    lane_uri,
                    rate,
                    prio,
                    body,
                    |node_uri, lane_uri, rate, prio, body| RawEnvelope::Link {
                        node_uri,
                        lane_uri,
                        rate,
                        prio,
                        body,
                    },
                ),
                EnvelopeKind::Sync => with_rate_prio(
                    node_uri,
                    lane_uri,
                    rate,
                    prio,
                    body,
                    |node_uri, lane_uri, rate, prio, body| RawEnvelope::Sync {
                        node_uri,
                        lane_uri,
                        rate,
                        prio,
                        body,
                    },
                ),
                EnvelopeKind::Unlink => {
                    with_path(node_uri, lane_uri, body, |node_uri, lane_uri, body| {
                        RawEnvelope::Unlink {
                            node_uri,
                            lane_uri,
                            body,
                        }
                    })
                }
                EnvelopeKind::Command => {
                    with_path(node_uri, lane_uri, body, |node_uri, lane_uri, body| {
                        RawEnvelope::Command {
                            node_uri,
                            lane_uri,
                            body,
                        }
                    })
                }
                EnvelopeKind::Linked => with_rate_prio(
                    node_uri,
                    lane_uri,
                    rate,
                    prio,
                    body,
                    |node_uri, lane_uri, rate, prio, body| RawEnvelope::Linked {
                        node_uri,
                        lane_uri,
                        rate,
                        prio,
                        body,
                    },
                ),
                EnvelopeKind::Synced => {
                    with_path(node_uri, lane_uri, body, |node_uri, lane_uri, body| {
                        RawEnvelope::Synced {
                            node_uri,
                            lane_uri,
                            body,
                        }
                    })
                }
                EnvelopeKind::Event => {
                    with_path(node_uri, lane_uri, body, |node_uri, lane_uri, body| {
                        RawEnvelope::Event {
                            node_uri,
                            lane_uri,
                            body,
                        }
                    })
                }
                EnvelopeKind::Unlinked => {
                    with_path(node_uri, lane_uri, body, |node_uri, lane_uri, body| {
                        RawEnvelope::Unlinked {
                            node_uri,
                            lane_uri,
                            body,
                        }
                    })
                }
            }
        } else {
            Err(HeaderExtractionError::Incomplete)
        }
    }
}
