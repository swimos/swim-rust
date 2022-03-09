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

use super::tokens::streaming::*;
use super::tokens::*;
use super::{FinalAttrStage, ParseEvents, Span};
use either::Either;
pub use hash::HashParser;
use nom::branch::alt;
use nom::character::complete as char_comp;
use nom::character::streaming as char_str;
use nom::combinator::{eof, map, opt, peek, recognize};
use nom::error::ErrorKind;
use nom::sequence::{pair, preceded};
use nom::{Finish, IResult, Parser};
use std::borrow::Cow;
use swim_form::structural::read::event::ReadEvent;

mod hash;

/// Change the state of the parser after producing an event.
#[derive(Debug)]
enum StateChange {
    /// Pop the frame for an attribute body and move into the 'after attribute' state.
    PopAfterAttr,
    /// Pop the frame for a record item and move into the 'after item' state.
    PopAfterItem,
    /// Change the state of the current frame to the specified value.
    ChangeState(ParseState),
    /// Push a new attribute body frame onto the stack.
    PushAttr,
    /// Start a new record frame, starting with an attribute.
    PushAttrNewRec {
        /// Whether the attribute has a body.
        has_body: bool,
    },
    /// Push a new frame for the items of a record body.
    PushBody,
}

impl StateChange {
    fn apply(self, state_stack: &mut Vec<ParseState>) {
        match self {
            StateChange::PopAfterAttr => {
                state_stack.pop();
                if let Some(top) = state_stack.last_mut() {
                    *top = ParseState::AfterAttr;
                }
            }
            StateChange::PopAfterItem => {
                state_stack.pop();
                if let Some(top) = state_stack.last_mut() {
                    top.after_item();
                }
            }
            StateChange::ChangeState(new_state) => {
                if let Some(top) = state_stack.last_mut() {
                    *top = new_state;
                }
            }
            StateChange::PushAttrNewRec { has_body } => {
                if has_body {
                    state_stack.push(ParseState::Init);
                    state_stack.push(ParseState::AttrBodyStartOrNl);
                } else {
                    state_stack.push(ParseState::AfterAttr);
                }
            }
            StateChange::PushAttr => {
                state_stack.push(ParseState::AttrBodyStartOrNl);
            }
            StateChange::PushBody => {
                state_stack.push(ParseState::RecordBodyStartOrNl);
            }
        }
    }
}

trait ReadEventExt<'a>: Sized {
    fn single(self) -> ParseEvents<'a>;

    fn followed_by(self, other: ReadEvent<'a>) -> ParseEvents<'a>;

    fn singleton_body(self) -> ParseEvents<'a>;
}

impl<'a> ReadEventExt<'a> for ReadEvent<'a> {
    fn single(self) -> ParseEvents<'a> {
        ParseEvents::SingleEvent(self)
    }

    fn followed_by(self, other: ReadEvent<'a>) -> ParseEvents<'a> {
        ParseEvents::TwoEvents(other, self)
    }

    fn singleton_body(self) -> ParseEvents<'a> {
        ParseEvents::ThreeEvents(ReadEvent::EndRecord, self, ReadEvent::StartBody)
    }
}

/// Possible states, within a single stack frame, for the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseState {
    Init,
    AttrBodyStartOrNl,
    AttrBodyAfterValue,
    AttrBodyAfterSlot,
    AttrBodySlot,
    AttrBodyAfterSep,
    AfterAttr,
    RecordBodyStartOrNl,
    RecordBodyAfterValue,
    RecordBodyAfterSlot,
    RecordBodySlot,
    RecordBodyAfterSep,
}

impl ParseState {
    /// Advance the state after an item.
    fn after_item(&mut self) {
        let new_state = match self {
            ParseState::Init => ParseState::AfterAttr,
            ParseState::AttrBodyStartOrNl | ParseState::AttrBodyAfterSep => {
                ParseState::AttrBodyAfterValue
            }
            ParseState::AttrBodySlot => ParseState::AttrBodyAfterSlot,
            ParseState::RecordBodyStartOrNl | ParseState::RecordBodyAfterSep => {
                ParseState::RecordBodyAfterValue
            }
            ParseState::RecordBodySlot => ParseState::RecordBodyAfterSlot,
            ow => panic!("Invalid state transition. {:?}", ow),
        };
        *self = new_state;
    }
}

fn attr_name(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    alt((string_literal, map(identifier, Cow::Borrowed)))(input)
}

fn attr_name_final(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    map(complete::identifier, Cow::Borrowed)(input)
}

impl<'a> ParseEvents<'a> {
    /// Take the next event for the iterator.
    fn take_event(&mut self) -> Option<EventOrEnd<'a>> {
        match std::mem::take(self) {
            ParseEvents::SingleEvent(ev) => Some(EventOrEnd::Event(ev, false)),
            ParseEvents::TwoEvents(second, first) => {
                *self = ParseEvents::SingleEvent(second);
                Some(EventOrEnd::Event(first, true))
            }
            ParseEvents::ThreeEvents(third, second, first) => {
                *self = ParseEvents::TwoEvents(third, second);
                Some(EventOrEnd::Event(first, true))
            }
            ParseEvents::TerminateWithAttr(stage) => match stage {
                FinalAttrStage::Start(name) => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::EndAttr);
                    Some(EventOrEnd::Event(ReadEvent::StartAttribute(name), true))
                }
                FinalAttrStage::EndAttr => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::StartBody);
                    Some(EventOrEnd::Event(ReadEvent::EndAttribute, true))
                }
                FinalAttrStage::StartBody => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::EndBody);
                    Some(EventOrEnd::Event(ReadEvent::StartBody, true))
                }
                FinalAttrStage::EndBody => {
                    *self = ParseEvents::End;
                    Some(EventOrEnd::Event(ReadEvent::EndRecord, true))
                }
            },

            ParseEvents::End => Some(EventOrEnd::End),
            _ => None,
        }
    }

    #[cfg(feature = "async_parser")]
    pub fn is_empty(&self) -> bool {
        matches!(self, ParseEvents::NoEvent | ParseEvents::End)
    }
}

impl<'a> Default for ParseEvents<'a> {
    fn default() -> Self {
        ParseEvents::NoEvent
    }
}

#[derive(Debug)]
enum EventOrEnd<'a> {
    Event(ReadEvent<'a>, bool),
    End,
}

/// An iterator which produces a sequence of parse events from a complete string by
/// applying an [`IncrementalReconParser`] repeatedly.
pub struct ParseIterator<'a> {
    input: Span<'a>,
    parser: Option<IncrementalReconParser>,
    pending: Option<ParseEvents<'a>>,
}

impl<'a> ParseIterator<'a> {
    pub fn new(input: Span<'a>, allow_comments: bool) -> Self {
        ParseIterator {
            input,
            parser: Some(IncrementalReconParser::new(allow_comments)),
            pending: None,
        }
    }
}

impl<'a> Iterator for ParseIterator<'a> {
    type Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let ParseIterator {
            input,
            parser,
            pending,
        } = self;
        if let Some(pending_events) = pending {
            match pending_events.take_event() {
                Some(EventOrEnd::Event(ev, remaining)) => {
                    if !remaining {
                        *pending = None;
                    }
                    Some(Ok(ev))
                }
                _ => {
                    *parser = None;
                    None
                }
            }
        } else {
            loop {
                let ex = parser.as_mut()?.parse(*input);
                let (rem, mut events) = match ex {
                    Ok(r) => r,
                    Err(nom::Err::Incomplete(_)) => {
                        if let Some(mut p) = parser
                            .take()
                            .and_then(IncrementalReconParser::into_final_parser)
                        {
                            match p.parse(*input).finish() {
                                Ok(r) => r,
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            }
                        } else {
                            let err = nom::error::Error::new(*input, ErrorKind::Alt);
                            return Some(Err(err));
                        }
                    }
                    Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                        *parser = None;
                        return Some(Err(e));
                    }
                };
                *input = rem;
                match events.take_event() {
                    Some(EventOrEnd::Event(ev, remaining)) => {
                        if remaining {
                            *pending = Some(events);
                        }
                        return Some(Ok(ev));
                    }
                    Some(EventOrEnd::End) => {
                        *parser = None;
                        return None;
                    }
                    _ => {}
                }
            }
        }
    }
}

impl<'a> From<ReadEvent<'a>> for ParseEvents<'a> {
    fn from(event: ReadEvent<'a>) -> Self {
        ParseEvents::SingleEvent(event)
    }
}

/// A stateful, incremental Recon parser. Each call to `parse` will produce zero
/// or more parse events which are guaranteed to be consistent.
#[derive(Debug)]
pub struct IncrementalReconParser {
    state: Vec<ParseState>,
    allow_comments: bool,
}

impl IncrementalReconParser {
    pub fn new(allow_comments: bool) -> Self {
        IncrementalReconParser {
            state: vec![ParseState::Init],
            allow_comments,
        }
    }
}

impl Default for IncrementalReconParser {
    fn default() -> Self {
        IncrementalReconParser {
            state: vec![ParseState::Init],
            allow_comments: false,
        }
    }
}

enum FinalState {
    Init,
    AfterAttr,
}

/// The incremental recon parser will fail if it is possible that providing more input
/// could change the final result (to allow it be used in cases where the whole string
/// may not be available all at once). When the end of the data is reached, it should
/// be converted into the final segment parser which can read the final events.
pub struct FinalSegmentParser {
    state: FinalState,
    allow_comments: bool,
}

impl FinalSegmentParser {
    fn new(state: FinalState, allow_comments: bool) -> Self {
        FinalSegmentParser {
            state,
            allow_comments,
        }
    }
}

impl IncrementalReconParser {
    /// Convert to the final segment parser to handle the end of the input.
    pub fn into_final_parser(mut self) -> Option<FinalSegmentParser> {
        self.final_parser()
    }

    fn final_parser(&mut self) -> Option<FinalSegmentParser> {
        let IncrementalReconParser {
            state,
            allow_comments,
        } = self;
        let top = state.pop();
        if state.is_empty() {
            match top {
                Some(ParseState::Init) => {
                    Some(FinalSegmentParser::new(FinalState::Init, *allow_comments))
                }
                Some(ParseState::AfterAttr) => Some(FinalSegmentParser::new(
                    FinalState::AfterAttr,
                    *allow_comments,
                )),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn final_parser_and_reset(&mut self) -> Option<FinalSegmentParser> {
        let parser = self.final_parser();
        self.state.clear();
        self.state.push(ParseState::Init);
        parser
    }

    pub fn reset(&mut self) {
        self.state.clear();
        self.state.push(ParseState::Init);
    }
}

impl<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> for IncrementalReconParser {
    fn parse(
        &mut self,
        input: Span<'a>,
    ) -> IResult<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> {
        let IncrementalReconParser {
            state,
            allow_comments,
        } = self;
        if let Some(top) = state.last_mut() {
            let (input, _) = char_str::space0(input)?;

            let input = if *allow_comments {
                comments(input)?.0
            } else {
                input
            };

            match top {
                ParseState::Init => {
                    let (input, (events, change)) =
                        preceded(char_str::multispace0, parse_init)(input)?;
                    if let Some(change) = change {
                        change.apply(state);
                    } else {
                        state.clear();
                    }
                    Ok((input, events))
                }
                ParseState::AfterAttr => {
                    let (input, (events, change)) = parse_after_attr(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyStartOrNl => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<RecBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyStartOrNl => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<AttrBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<RecBody>(true),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<AttrBody>(true),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterValue => {
                    let (input, new_state) =
                        preceded(char_str::space0, parse_after_value::<RecBody>)(input)?;
                    let events = if let Some(new_state) = new_state {
                        *top = new_state;
                        if new_state == ParseState::RecordBodySlot {
                            ReadEvent::Slot.single()
                        } else {
                            ParseEvents::NoEvent
                        }
                    } else {
                        StateChange::PopAfterItem.apply(state);
                        ReadEvent::EndRecord.single()
                    };
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterValue => {
                    let (input, new_state) =
                        preceded(char_str::space0, parse_after_value::<AttrBody>)(input)?;
                    let events = if let Some(new_state) = new_state {
                        *top = new_state;
                        if new_state == ParseState::AttrBodySlot {
                            ReadEvent::Slot.single()
                        } else {
                            ParseEvents::NoEvent
                        }
                    } else {
                        StateChange::PopAfterAttr.apply(state);
                        ReadEvent::EndAttribute.single()
                    };
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterSlot => {
                    let (input, new_state) =
                        preceded(char_str::space0, parse_after_slot::<RecBody>)(input)?;
                    let events = if let Some(new_state) = new_state {
                        *top = new_state;
                        ParseEvents::NoEvent
                    } else {
                        StateChange::PopAfterItem.apply(state);
                        ReadEvent::EndRecord.single()
                    };
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterSlot => {
                    let (input, new_state) =
                        preceded(char_str::space0, parse_after_slot::<AttrBody>)(input)?;
                    let events = if let Some(new_state) = new_state {
                        *top = new_state;
                        ParseEvents::NoEvent
                    } else {
                        StateChange::PopAfterAttr.apply(state);
                        ReadEvent::EndAttribute.single()
                    };
                    Ok((input, events))
                }
                ParseState::RecordBodySlot => {
                    let (input, (events, change)) =
                        preceded(char_str::space0, parse_slot_value::<RecBody>)(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodySlot => {
                    let (input, (events, change)) =
                        preceded(char_str::space0, parse_slot_value::<AttrBody>)(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
            }
        } else {
            Ok((input, ParseEvents::End))
        }
    }
}

impl<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> for FinalSegmentParser {
    fn parse(
        &mut self,
        input: Span<'a>,
    ) -> IResult<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> {
        let FinalSegmentParser {
            state,
            allow_comments,
        } = self;
        match state {
            FinalState::Init => {
                let input = if *allow_comments {
                    complete::comments(input)?.0
                } else {
                    input
                };
                let (input, _) = char_comp::multispace0(input)?;
                parse_init_final(input)
            }
            FinalState::AfterAttr => {
                let input = if *allow_comments {
                    complete::comments(input)?.0
                } else {
                    input
                };
                let (input, _) = char_comp::space0(input)?;
                parse_after_attr_final(input)
            }
        }
    }
}

fn parse_init_final(input: Span<'_>) -> IResult<Span<'_>, ParseEvents<'_>> {
    alt((
        map(eof, |_| ReadEvent::Extant.single()),
        map(complete::identifier_or_bool, |v| {
            identifier_event(v).single()
        }),
        map(complete::numeric_literal, |l| ReadEvent::Number(l).single()),
        map(complete::blob, |data| ReadEvent::Blob(data).single()),
        attr_final,
    ))(input)
}

fn parse_after_attr_final(input: Span<'_>) -> IResult<Span<'_>, ParseEvents<'_>> {
    alt((
        map(eof, |_| {
            ReadEvent::StartBody.followed_by(ReadEvent::EndRecord)
        }),
        map(complete::identifier_or_bool, |v| {
            identifier_event(v).singleton_body()
        }),
        map(complete::numeric_literal, |l| {
            ReadEvent::Number(l).singleton_body()
        }),
        map(complete::blob, |data| {
            ReadEvent::Blob(data).singleton_body()
        }),
        attr_final,
    ))(input)
}

fn parse_init(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, Option<StateChange>)> {
    alt((
        map(string_literal, |s| (ReadEvent::TextValue(s).single(), None)),
        map(complete::identifier_or_bool, |v| {
            (identifier_event(v).single(), None)
        }),
        map(complete::numeric_literal, |l| {
            (ReadEvent::Number(l).single(), None)
        }),
        map(complete::blob, |data| {
            (ReadEvent::Blob(data).single(), None)
        }),
        map(secondary_attr, |(e, c)| (e, Some(c))),
        map(char_str::char('{'), |_| {
            (
                ReadEvent::StartBody.single(),
                Some(StateChange::ChangeState(ParseState::RecordBodyStartOrNl)),
            )
        }),
    ))(input)
}

fn attr(input: Span<'_>) -> IResult<Span<'_>, (Cow<'_, str>, bool)> {
    preceded(
        char_str::char('@'),
        pair(attr_name, map(opt(char_str::char('(')), |o| o.is_some())),
    )(input)
}

fn attr_final(input: Span<'_>) -> IResult<Span<'_>, ParseEvents<'_>> {
    map(preceded(char_comp::char('@'), attr_name_final), |name| {
        ParseEvents::TerminateWithAttr(FinalAttrStage::Start(name))
    })(input)
}

fn primary_attr(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    map(attr, |(name, has_body)| {
        if has_body {
            (
                ReadEvent::StartAttribute(name).single(),
                StateChange::PushAttrNewRec { has_body: true },
            )
        } else {
            (
                ReadEvent::StartAttribute(name).followed_by(ReadEvent::EndAttribute),
                StateChange::PushAttrNewRec { has_body: false },
            )
        }
    })(input)
}

fn secondary_attr(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    map(attr, |(name, has_body)| {
        if has_body {
            (
                ReadEvent::StartAttribute(name).single(),
                StateChange::PushAttr,
            )
        } else {
            (
                ReadEvent::StartAttribute(name).followed_by(ReadEvent::EndAttribute),
                StateChange::ChangeState(ParseState::AfterAttr),
            )
        }
    })(input)
}

fn identifier_event(value: Either<&str, bool>) -> ReadEvent<'_> {
    match value {
        Either::Left(s) => ReadEvent::TextValue(Cow::Borrowed(s)),
        Either::Right(p) => ReadEvent::Boolean(p),
    }
}

fn parse_after_attr(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| {
            (
                ReadEvent::TextValue(s).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        map(identifier_or_bool, |v| {
            (
                identifier_event(v).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        map(numeric_literal, |l| {
            (
                ReadEvent::Number(l).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        map(blob, |data| {
            (
                ReadEvent::Blob(data).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        secondary_attr,
        map(char_str::char('{'), |_| {
            (
                ReadEvent::StartBody.single(),
                StateChange::ChangeState(ParseState::RecordBodyStartOrNl),
            )
        }),
        map(
            peek(alt((
                recognize(alt((separator, char_str::one_of(")}")))),
                char_str::line_ending,
            ))),
            |_| {
                (
                    ReadEvent::StartBody.followed_by(ReadEvent::EndRecord),
                    StateChange::PopAfterItem,
                )
            },
        ),
    ))(input)
}

fn value_item<K: ItemsKind>(event: ReadEvent<'_>) -> (ParseEvents<'_>, StateChange) {
    (event.single(), StateChange::ChangeState(K::after_value()))
}

fn slot_item<K: ItemsKind>(event: ReadEvent<'_>) -> (ParseEvents<'_>, StateChange) {
    (event.single(), StateChange::ChangeState(K::after_slot()))
}

/// Parsing of the body of an attribute and of the body of a record is very similar.
/// This trait encodes the differenes between the two to allow the same method to be
/// used for both.
trait ItemsKind {
    fn start_or_nl() -> ParseState;
    fn after_sep() -> ParseState;
    fn start_slot() -> ParseState;
    fn after_value() -> ParseState;
    fn after_slot() -> ParseState;
    fn end_delim() -> char;
    fn end_event<'a>() -> ReadEvent<'a>;
    fn end_state_change() -> StateChange;
}

struct AttrBody;

struct RecBody;

impl ItemsKind for AttrBody {
    fn start_or_nl() -> ParseState {
        ParseState::AttrBodyStartOrNl
    }

    fn after_sep() -> ParseState {
        ParseState::AttrBodyAfterSep
    }

    fn start_slot() -> ParseState {
        ParseState::AttrBodySlot
    }

    fn after_value() -> ParseState {
        ParseState::AttrBodyAfterValue
    }

    fn after_slot() -> ParseState {
        ParseState::AttrBodyAfterSlot
    }

    fn end_delim() -> char {
        ')'
    }

    fn end_event<'a>() -> ReadEvent<'a> {
        ReadEvent::EndAttribute
    }

    fn end_state_change() -> StateChange {
        StateChange::PopAfterAttr
    }
}

impl ItemsKind for RecBody {
    fn start_or_nl() -> ParseState {
        ParseState::RecordBodyStartOrNl
    }

    fn after_sep() -> ParseState {
        ParseState::RecordBodyAfterSep
    }

    fn start_slot() -> ParseState {
        ParseState::RecordBodySlot
    }

    fn end_delim() -> char {
        '}'
    }

    fn end_event<'a>() -> ReadEvent<'a> {
        ReadEvent::EndRecord
    }

    fn after_value() -> ParseState {
        ParseState::RecordBodyAfterValue
    }

    fn after_slot() -> ParseState {
        ParseState::RecordBodyAfterSlot
    }

    fn end_state_change() -> StateChange {
        StateChange::PopAfterItem
    }
}

fn parse_not_after_item<K: ItemsKind>(
    item_required: bool,
) -> impl for<'a> Fn(Span<'a>) -> IResult<Span<'a>, (ParseEvents<'a>, StateChange)> {
    move |input: Span<'_>| {
        alt((
            map(string_literal, |s| value_item::<K>(ReadEvent::TextValue(s))),
            map(identifier_or_bool, |v| value_item::<K>(identifier_event(v))),
            map(numeric_literal, |l| value_item::<K>(ReadEvent::Number(l))),
            map(blob, |data| value_item::<K>(ReadEvent::Blob(data))),
            map(separator, |_| {
                (
                    ReadEvent::Extant.single(),
                    StateChange::ChangeState(K::after_sep()),
                )
            }),
            map(char_str::char(':'), |_| {
                (
                    ReadEvent::Extant.followed_by(ReadEvent::Slot),
                    StateChange::ChangeState(K::start_slot()),
                )
            }),
            map(char_str::char(K::end_delim()), move |_| {
                let event = K::end_event();
                let events = if item_required {
                    ReadEvent::Extant.followed_by(event)
                } else {
                    event.single()
                };
                (events, K::end_state_change())
            }),
            primary_attr,
            map(char_str::char('{'), |_| {
                (ReadEvent::StartBody.single(), StateChange::PushBody)
            }),
        ))(input)
    }
}

fn parse_slot_value<K: ItemsKind>(
    input: Span<'_>,
) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| slot_item::<K>(ReadEvent::TextValue(s))),
        map(identifier_or_bool, |v| slot_item::<K>(identifier_event(v))),
        map(numeric_literal, |l| slot_item::<K>(ReadEvent::Number(l))),
        map(blob, |data| slot_item::<K>(ReadEvent::Blob(data))),
        map(char_str::line_ending, |_| {
            (
                ReadEvent::Extant.single(),
                StateChange::ChangeState(K::start_or_nl()),
            )
        }),
        map(separator, |_| {
            (
                ReadEvent::Extant.single(),
                StateChange::ChangeState(K::after_sep()),
            )
        }),
        map(char_str::char(K::end_delim()), move |_| {
            (
                ReadEvent::Extant.followed_by(K::end_event()),
                K::end_state_change(),
            )
        }),
        primary_attr,
        map(char_str::char('{'), |_| {
            (ReadEvent::StartBody.single(), StateChange::PushBody)
        }),
    ))(input)
}

fn parse_after_value<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(char_str::line_ending, |_| Some(K::start_or_nl())),
        map(separator, |_| Some(K::after_sep())),
        map(char_str::char(':'), |_| Some(K::start_slot())),
        map(char_str::char(K::end_delim()), |_| None),
    ))(input)
}

fn parse_after_slot<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(char_str::line_ending, |_| Some(K::start_or_nl())),
        map(separator, |_| Some(K::after_sep())),
        map(char_str::char(K::end_delim()), |_| None),
    ))(input)
}

impl<'a> Iterator for ParseEvents<'a> {
    type Item = Option<ReadEvent<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::take(self) {
            ParseEvents::ThreeEvents(e1, e2, e3) => {
                *self = ParseEvents::TwoEvents(e1, e2);
                Some(Some(e3))
            }
            ParseEvents::TwoEvents(e1, e2) => {
                *self = ParseEvents::SingleEvent(e1);
                Some(Some(e2))
            }
            ParseEvents::SingleEvent(e1) => {
                *self = ParseEvents::NoEvent;
                Some(Some(e1))
            }
            ParseEvents::NoEvent => None,
            ParseEvents::TerminateWithAttr(stage) => match stage {
                FinalAttrStage::Start(name) => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::EndAttr);
                    Some(Some(ReadEvent::StartAttribute(name)))
                }
                FinalAttrStage::EndAttr => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::StartBody);
                    Some(Some(ReadEvent::EndAttribute))
                }
                FinalAttrStage::StartBody => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::EndBody);
                    Some(Some(ReadEvent::StartBody))
                }
                FinalAttrStage::EndBody => {
                    *self = ParseEvents::End;
                    Some(Some(ReadEvent::EndRecord))
                }
            },
            ParseEvents::End => Some(None),
        }
    }
}
