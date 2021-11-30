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

use swim_model::Text;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum RawEnvelopeHeader {
    Auth,
    DeAuth,
    Link {
        node: Text,
        lane: Text,
        rate: Option<f64>,
        prio: Option<f64>,
    },
    Sync {
        node: Text,
        lane: Text,
        rate: Option<f64>,
        prio: Option<f64>,
    },
    Unlink {
        node: Text,
        lane: Text,
    },
    Command {
        node: Text,
        lane: Text,
    },
    Linked {
        node: Text,
        lane: Text,
        rate: Option<f64>,
        prio: Option<f64>,
    },
    Synced {
        node: Text,
        lane: Text,
    },
    Unlinked {
        node: Text,
        lane: Text,
    },
    Event {
        node: Text,
        lane: Text,
    },
}

#[derive(Debug, PartialEq, Error)]
pub enum HeaderParseErr {
    #[error("Mismatched tag, found: `{0}`")]
    MismatchedTag(String),
    #[error("The input string was malformatted: `{0}`")]
    Malformatted(String),
    #[error("Unexpected item. Expected a `{0}`")]
    UnexpectedItem(String),
}

impl RawEnvelopeHeader {
    pub fn parse_from(repr: &str) -> Result<(RawEnvelopeHeader, &str), HeaderParseErr> {
        parser::parse_from(repr)
    }
}

mod parser {
    use crate::envelope::{HeaderParseErr, RawEnvelopeHeader};
    use either::Either;
    use swim_model::Text;
    use swim_recon::parser::{parse_envelope_header, HeaderParseIterator, HeaderReadEvent};

    pub fn parse_from(repr: &str) -> Result<(RawEnvelopeHeader, &str), HeaderParseErr> {
        let mut parser = parse_envelope_header(repr);

        let tag = match parser.next() {
            Some(Ok(HeaderReadEvent::Tag(tag))) => tag,
            Some(Ok(_)) => return Err(HeaderParseErr::UnexpectedItem("tag".into())),
            Some(Err(e)) => return Err(HeaderParseErr::Malformatted(e.to_string())),
            None => return Err(HeaderParseErr::Malformatted("Missing tag".into())),
        };

        match tag.as_ref() {
            "auth" => {
                let offset = complete(&mut parser, repr)?;
                Ok((RawEnvelopeHeader::Auth, offset))
            }
            "deauth" => {
                let offset = complete(&mut parser, repr)?;
                Ok((RawEnvelopeHeader::DeAuth, offset))
            }
            tag => {
                let node = read_string_slot(&mut parser, "node")?;
                let lane = read_string_slot(&mut parser, "lane")?;

                match tag {
                    "unlink" => {
                        let offset = complete(&mut parser, repr)?;
                        Ok((RawEnvelopeHeader::Unlink { node, lane }, offset))
                    }
                    "synced" => {
                        let offset = complete(&mut parser, repr)?;
                        Ok((RawEnvelopeHeader::Synced { node, lane }, offset))
                    }
                    "unlinked" => {
                        let offset = complete(&mut parser, repr)?;
                        Ok((RawEnvelopeHeader::Unlinked { node, lane }, offset))
                    }
                    "event" => {
                        let offset = complete(&mut parser, repr)?;
                        Ok((RawEnvelopeHeader::Event { node, lane }, offset))
                    }
                    tag => {
                        let (prio, rate) = read_rates(&mut parser)?;

                        match tag {
                            "link" => {
                                let offset = complete(&mut parser, repr)?;
                                Ok((
                                    RawEnvelopeHeader::Link {
                                        node,
                                        lane,
                                        prio,
                                        rate,
                                    },
                                    offset,
                                ))
                            }
                            "linked" => {
                                let offset = complete(&mut parser, repr)?;
                                Ok((
                                    RawEnvelopeHeader::Linked {
                                        node,
                                        lane,
                                        prio,
                                        rate,
                                    },
                                    offset,
                                ))
                            }
                            tag => Err(HeaderParseErr::Malformatted(format!(
                                "Unknown tag: `{}`",
                                tag
                            ))),
                        }
                    }
                }
            }
        }
    }

    fn read_string_slot(
        parser: &mut HeaderParseIterator,
        expected: &'static str,
    ) -> Result<Text, HeaderParseErr> {
        match parser.next() {
            Some(Ok(HeaderReadEvent::Slot(name, Either::Left(value)))) if name == expected => {
                Ok(value.into())
            }
            Some(Ok(_)) => return Err(HeaderParseErr::UnexpectedItem("string".into())),
            Some(Err(e)) => return Err(HeaderParseErr::Malformatted(e.to_string())),
            None => {
                return Err(HeaderParseErr::Malformatted(format!(
                    "Missing slot: `{}`",
                    expected
                )))
            }
        }
    }

    fn read_rates(
        parser: &mut HeaderParseIterator,
    ) -> Result<(Option<f64>, Option<f64>), HeaderParseErr> {
        let mut prio = None;
        let mut rate = None;

        let with_opt = |var: &mut Option<f64>, key, value| match var {
            Some(_) => {
                return Err(HeaderParseErr::Malformatted(format!(
                    "Duplicate key: `{}`",
                    key
                )))
            }
            None => {
                *var = Some(value);
                Ok(())
            }
        };

        loop {
            match parser.next() {
                Some(Ok(HeaderReadEvent::Slot(name, Either::Right(value)))) => {
                    if name == "prio" {
                        with_opt(&mut prio, "prio", value)?;
                    } else if name == "rate" {
                        with_opt(&mut rate, "rate", value)?;
                    } else {
                        return Err(HeaderParseErr::Malformatted(format!(
                            "Unknown key: `{}`",
                            name
                        )));
                    }
                }
                Some(Ok(_)) => return Err(HeaderParseErr::UnexpectedItem("prio or rate".into())),
                Some(Err(e)) => return Err(HeaderParseErr::Malformatted(e.to_string())),
                None => return Ok((prio, rate)),
            }
        }
    }

    fn complete<'a>(
        parser: &mut HeaderParseIterator,
        repr: &'a str,
    ) -> Result<&'a str, HeaderParseErr> {
        match parser.next() {
            None => Ok(&repr[parser.offset()..]),
            e => Err(HeaderParseErr::Malformatted("Unconsumed input".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::envelope::{HeaderParseErr, RawEnvelopeHeader};

    #[test]
    fn auth() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@auth"),
            Ok((RawEnvelopeHeader::Auth, ""))
        );
    }

    #[test]
    fn deauth() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@deauth"),
            Ok((RawEnvelopeHeader::DeAuth, ""))
        );
    }

    #[test]
    fn unlink() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@unlink(node:node, lane:lane)"),
            Ok((
                RawEnvelopeHeader::Unlink {
                    lane: "lane".into(),
                    node: "node".into()
                },
                ""
            ))
        );
    }

    #[test]
    fn synced() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@synced(node:node, lane:lane)"),
            Ok((
                RawEnvelopeHeader::Synced {
                    lane: "lane".into(),
                    node: "node".into()
                },
                ""
            ))
        );
    }

    #[test]
    fn unlinked() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@unlinked(node:node, lane:lane)"),
            Ok((
                RawEnvelopeHeader::Unlinked {
                    lane: "lane".into(),
                    node: "node".into()
                },
                ""
            ))
        );
    }

    #[test]
    fn event() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@event(node:node, lane:lane)"),
            Ok((
                RawEnvelopeHeader::Event {
                    lane: "lane".into(),
                    node: "node".into()
                },
                ""
            ))
        );
    }

    #[test]
    fn link() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@link(node:node, lane:lane, prio:0.1, rate:1.0)"),
            Ok((
                RawEnvelopeHeader::Link {
                    lane: "lane".into(),
                    node: "node".into(),
                    prio: Some(0.1),
                    rate: Some(1.0)
                },
                ""
            ))
        );
    }

    #[test]
    fn linked() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@linked(node:node, lane:lane, prio:0.1, rate:1.0)"),
            Ok((
                RawEnvelopeHeader::Linked {
                    lane: "lane".into(),
                    node: "node".into(),
                    prio: Some(0.1),
                    rate: Some(1.0)
                },
                ""
            ))
        );
    }

    #[test]
    fn unknown_tag() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@waffle(node:node, lane:lane)"),
            Err(HeaderParseErr::Malformatted("Unknown tag: `waffle`".into()))
        );
    }

    #[test]
    fn extra_part() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@link(node:node, lane:lane, foo:bar)"),
            Err(HeaderParseErr::UnexpectedItem("prio or rate".into()))
        );
    }

    #[test]
    fn duplicate_link_key() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@link(node:node, lane:lane, rate:0.1, rate:0.1)"),
            Err(HeaderParseErr::Malformatted("Duplicate key: `rate`".into()))
        );
    }

    #[test]
    fn event_body() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@event(node:node, lane:lane)@update(key: to) me"),
            Ok((
                RawEnvelopeHeader::Event {
                    lane: "lane".into(),
                    node: "node".into()
                },
                "@update(key: to) me"
            ))
        );
    }

    #[test]
    fn link_body() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@link(node:node, lane:lane)@update(key: to) me"),
            Ok((
                RawEnvelopeHeader::Link {
                    lane: "lane".into(),
                    node: "node".into(),
                    rate: None,
                    prio: None
                },
                "@update(key: to) me"
            ))
        );
    }

    #[test]
    fn incomplete() {
        assert_eq!(
            RawEnvelopeHeader::parse_from("@unlink(node:node, lane:lane, extra:field"),
            Err(HeaderParseErr::Malformatted("Unconsumed input".into()))
        );
    }
}
