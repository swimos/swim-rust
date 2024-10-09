// Copyright 2015-2024 Swim Inc.
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

use crate::selector::{
    AttrSelector, BasicSelector, ChainSelector, IndexSelector, KeySelector, PayloadSelector,
    PubSubSelector, SlotSelector,
};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::char,
    character::complete::one_of,
    combinator::{map, opt},
    combinator::{map_res, recognize},
    error::{Error, ErrorKind},
    multi::many1,
    multi::separated_list0,
    sequence::delimited,
    sequence::preceded,
    IResult, Needed, Parser, Slice,
};
use nom_locate::LocatedSpan;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

pub type Span<'a> = LocatedSpan<&'a str>;

/// Runs `parser` against `input` and checks that the entire input was consumed. If it was not, then
/// this function will return `Err(ParseError)`.
pub fn parse_consume<'i, P, O>(input: Span<'i>, mut parser: P) -> Result<O, ParseError>
where
    P: Parser<Span<'i>, O, Error<Span<'i>>>,
{
    let (input, output) = parser.parse(input).map_err(|err| match err {
        nom::Err::Incomplete(Needed::Size(cnt)) => ParseError(cnt.get()),
        nom::Err::Incomplete(Needed::Unknown) => ParseError(1),
        nom::Err::Error(e) | nom::Err::Failure(e) => ParseError(e.input.location_offset()),
    })?;

    if !input.is_empty() {
        return Err(ParseError(input.location_offset()));
    }

    Ok(output)
}

/// A selector pattern iterator which will yield the parts that the pattern contains.
#[derive(Debug)]
pub struct SelectorPatternIter<'p, I> {
    pattern: &'p str,
    head: Option<Part<'p>>,
    tail: I,
    state: PatternIterState,
}

impl<'p, I> SelectorPatternIter<'p, I> {
    /// Builds a new selector pattern iterator.
    ///
    /// # Arguments
    /// * `pattern` - the pattern that the parts were derived from.
    /// * `head` - an optional head part to yield before any items are consumed from `tail`.
    /// * `tail` - the tail segments.
    pub fn new(pattern: &'p str, head: Option<Part<'p>>, tail: I) -> SelectorPatternIter<'p, I> {
        SelectorPatternIter {
            pattern,
            head,
            tail,
            state: PatternIterState::Start,
        }
    }
}

#[derive(Debug)]
enum PatternIterState {
    Start,
    Path,
    Done,
}

impl<'p, I> Iterator for SelectorPatternIter<'p, I>
where
    I: Iterator<Item = &'p Segment>,
{
    type Item = Part<'p>;

    fn next(&mut self) -> Option<Self::Item> {
        let SelectorPatternIter {
            pattern,
            head,
            tail,
            state,
        } = self;

        loop {
            match state {
                PatternIterState::Start => match head.take() {
                    Some(head) => break Some(head),
                    None => *state = PatternIterState::Path,
                },
                PatternIterState::Path => {
                    let item = match tail.next() {
                        Some(segment) => Some(segment_to_part(pattern, segment)),
                        None => {
                            *state = PatternIterState::Done;
                            None
                        }
                    };
                    break item;
                }
                PatternIterState::Done => break None,
            }
        }
    }
}

/// Derives a [`Part`] from a [`Segment`].
pub fn segment_to_part<'p>(pattern: &'p str, segment: &Segment) -> Part<'p> {
    match segment {
        Segment::Static(StaticBound { start, end }) => Part::Static(&pattern[*start..*end]),
        Segment::Selector(Selector::Topic) => {
            todo!()
            // Part::Selector()
        }
        Segment::Selector(Selector::Property { kind, parts }) => {
            let parts = parts
                .iter()
                .flat_map(|selector| {
                    let SelectorBound {
                        start,
                        end,
                        attr,
                        index,
                    } = selector;

                    let mut links = vec![];
                    if let Some(n) = index {
                        links.push(BasicSelector::Index(IndexSelector::new(*n)));
                    }

                    let name = &pattern[*start..*end];

                    links.push(if *attr {
                        BasicSelector::Attr(AttrSelector::new(name.to_string()))
                    } else {
                        BasicSelector::Slot(SlotSelector::for_field(name))
                    });
                    if let Some(n) = index {
                        links.push(BasicSelector::Index(IndexSelector::new(*n)));
                    }

                    links
                })
                .collect::<Vec<_>>();
            match kind {
                SelectorKind::Key => Part::Selector(PubSubSelector::inject(KeySelector::new(
                    ChainSelector::from(parts),
                ))),
                SelectorKind::Value => Part::Selector(PubSubSelector::inject(
                    PayloadSelector::new(ChainSelector::from(parts)),
                )),
            }
        }
    }
}

/// A structured representation of a segment in a selector pattern.
#[derive(PartialEq, Debug)]
pub enum Part<'p> {
    /// The part is a static string slice. For example, given a selector pattern of `/node_uri/$value.name`
    /// a static part will be derived which contains `/node_uri/`.
    Static(&'p str),
    /// The derived part is a Value selector.
    Selector(PubSubSelector),
}

/// Selector types in a selector pattern.
#[derive(Debug, PartialEq, Eq)]
pub enum Selector {
    /// A topic selector.
    Topic,
    /// A property selector. Either a Key or Value selector.
    Property {
        /// The type of selector.
        kind: SelectorKind,
        /// A list of subcomponents to select.
        parts: Vec<SelectorBound>,
    },
}

/// A segment in a selector pattern.
#[derive(Debug, PartialEq, Eq)]
pub enum Segment {
    /// The segment is a string slice. For example, given a selector pattern of `/node_uri/$value.name`
    /// a part will be derived which contains `/node_uri/`.
    Static(StaticBound),
    /// The segment is a selector.
    Selector(Selector),
}

/// Derived selector subcomponent properties from a pattern.
#[derive(Debug, PartialEq, Eq)]
pub struct SelectorBound {
    /// Where this bound starts.
    pub start: usize,
    /// Where this bound ends.
    pub end: usize,
    /// Whether this subcomponent is an attribute.
    pub attr: bool,
    /// The index this subcomponent accesses in a Record's items.
    pub index: Option<usize>,
}

impl Segment {
    pub fn fmt_with_input(&self, f: &mut fmt::Formatter<'_>, input: &str) -> fmt::Result {
        match self {
            Segment::Static(StaticBound { start, end }) => {
                // check if the previous character in the path was an escape character. while this
                // information was known when the path was parsed, it's not necessary to add the
                // state to the segment as it would only be used for the Display implementation and
                // not in any critical paths.
                let fmt = &input[*start..*end];
                // Safe as an escape character is not valid as a starting character.
                if *start != 0 && input.as_bytes()[start - 1] == b'$' {
                    write!(f, "${fmt}")
                } else {
                    f.write_str(fmt)
                }
            }
            Segment::Selector(selector) => match selector {
                Selector::Topic => {
                    write!(f, "$topic")
                }
                Selector::Property { kind, parts } => {
                    match kind {
                        SelectorKind::Key => write!(f, "$key")?,
                        SelectorKind::Value => write!(f, "$value")?,
                    };

                    for bound in parts {
                        let SelectorBound {
                            start,
                            end,
                            attr,
                            index,
                        } = bound;

                        write!(f, ".")?;
                        if *attr {
                            write!(f, "@")?;
                        }
                        write!(f, "{}", &input[*start..*end])?;

                        if let Some(idx) = index {
                            write!(f, "[{}]", *idx)?;
                        }
                    }
                    Ok(())
                }
            },
        }
    }
}

/// The type of selector.
#[derive(Debug, PartialEq, Eq)]
pub enum SelectorKind {
    Key,
    Value,
}

/// A static part in a selector pattern.
#[derive(Debug, PartialEq, Eq)]
pub struct StaticBound {
    /// The starting index of the static part.
    pub start: usize,
    /// The ending index of the static part.
    pub end: usize,
}

/// Error produced when a parser failed to parse a selector pattern.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("Invalid character at index {0}")]
pub struct ParseError(pub usize);

impl Display for SelectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectorKind::Key => write!(f, "$key"),
            SelectorKind::Value => write!(f, "$value"),
        }
    }
}

pub fn failure<I, O, E: nom::error::ParseError<I>>(input: I) -> IResult<I, O, E> {
    Err(nom::Err::Failure(E::from_error_kind(
        input,
        ErrorKind::Fail,
    )))
}

/// Parses a selector and any subcomponents from `input`.
///
/// Example input: `$key.field.a`.
pub fn parse_payload_selector(input: Span) -> IResult<Span, Selector> {
    alt((
        map(parse_selector("$key"), |parts| Selector::Property {
            kind: SelectorKind::Key,
            parts,
        }),
        map(parse_selector("$value"), |parts| Selector::Property {
            kind: SelectorKind::Value,
            parts,
        }),
    ))(input)
}

/// Returns a parser which will parse a selector named `selector`.
pub fn parse_selector(
    selector: &str,
) -> impl FnMut(Span) -> IResult<Span, Vec<SelectorBound>> + '_ {
    move |input| {
        let (input, _) = tag(selector)(input)?;
        let (input, segments) = opt(preceded(
            char('.'),
            separated_list0(char('.'), non_empty_segment),
        ))(input)?;
        Ok((input, segments.unwrap_or_default()))
    }
}

/// Parses a subcomponent in a selector. A subcomponent may be an attribute and index selector.
///
/// Example inputs: `@attr[1]`, `field`, `@attr`, `field[1]`.
fn non_empty_segment(input: Span) -> IResult<Span, SelectorBound> {
    let (input, attr) = map(opt(char('@')), |o| o.is_some())(input)?;
    let (input, segment) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;

    if segment.is_empty() {
        Err(nom::Err::Failure(Error::new(input, ErrorKind::TakeWhile1)))
    } else {
        let start = segment.location_offset();
        let end = start + segment.fragment().len();

        let (input, index) = opt(delimited(
            char('['),
            map_res(recognize(many1(one_of("0123456789"))), {
                |span: Span| usize::from_str(span.as_ref())
            }),
            char(']'),
        ))(input)?;

        Ok((
            input,
            SelectorBound {
                start,
                end,
                attr,
                index,
            },
        ))
    }
}

/// Parses a static expression.
///
/// # Arguments
/// * `cont` - a parser which is used to check the current character and will cause this parser to
///   terminate early if it returns `false`.
pub fn parse_static_expression<F>(cont: F) -> impl FnMut(Span) -> IResult<Span, Span>
where
    F: FnMut(Span) -> IResult<Span, bool> + Copy,
{
    move |start_span| {
        let mut current_span = start_span;
        loop {
            let (input, cont) = preceded(
                take_while(|c: char| c.is_alphanumeric() || "-._".contains(c)),
                cont,
            )(current_span)?;

            if cont {
                current_span = input;
            } else {
                let end = input.location_offset() - start_span.location_offset();
                let input = start_span.slice(end..);
                let path = start_span.slice(..end);

                if path.len() == 0 {
                    return Err(nom::Err::Error(Error::new(input, ErrorKind::Eof)));
                }

                break Ok((input, path));
            }
        }
    }
}
