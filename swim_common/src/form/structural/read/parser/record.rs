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

use super::tokens::streaming::*;
use super::tokens::*;
use super::{NumericLiteral, ParseEvent, Span};
use crate::form::structural::read::parser::error::ParseError;
use crate::form::structural::read::{BodyReader, HeaderReader, ReadError};
use either::Either;
use nom::branch::alt;
use nom::character::complete as char_comp;
use nom::character::streaming as char_str;
use nom::combinator::{map, opt, peek, recognize};
use nom::error::ErrorKind;
use nom::sequence::{pair, preceded};
use nom::{Finish, IResult, Parser};
use std::borrow::Cow;
use std::convert::TryFrom;

enum StateChange {
    PopAfterItem,
    ChangeState(ParseState),
    PushAttr,
    PushAttrNewRec(bool),
    PushBody,
}

impl StateChange {
    fn apply(self, state_stack: &mut Vec<ParseState>) {
        match self {
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
            StateChange::PushAttrNewRec(has_body) => {
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

impl<'a> ParseEvent<'a> {
    fn single(self) -> ParseEvents<'a> {
        ParseEvents::SingleEvent(self)
    }

    fn followed_by(self, other: ParseEvent<'a>) -> ParseEvents<'a> {
        ParseEvents::TwoEvents(other, self)
    }

    fn singleton_body(self) -> ParseEvents<'a> {
        ParseEvents::ThreeEvents(ParseEvent::EndRecord, self, ParseEvent::StartBody)
    }
}

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
            _ => panic!("Invalid state transition."),
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

pub enum FinalAttrStage<'a> {
    Start(Cow<'a, str>),
    EndAttr,
    StartBody,
    EndBody,
}

pub enum ParseEvents<'a> {
    NoEvent,
    SingleEvent(ParseEvent<'a>),
    TwoEvents(ParseEvent<'a>, ParseEvent<'a>),
    ThreeEvents(ParseEvent<'a>, ParseEvent<'a>, ParseEvent<'a>),
    TerminateWithAttr(FinalAttrStage<'a>),
    End,
}

impl<'a> ParseEvents<'a> {
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
                    Some(EventOrEnd::Event(ParseEvent::StartAttribute(name), true))
                }
                FinalAttrStage::EndAttr => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::StartBody);
                    Some(EventOrEnd::Event(ParseEvent::EndAttribute, true))
                }
                FinalAttrStage::StartBody => {
                    *self = ParseEvents::TerminateWithAttr(FinalAttrStage::EndBody);
                    Some(EventOrEnd::Event(ParseEvent::StartBody, true))
                }
                FinalAttrStage::EndBody => {
                    *self = ParseEvents::End;
                    Some(EventOrEnd::Event(ParseEvent::EndRecord, true))
                }
            },

            ParseEvents::End => Some(EventOrEnd::End),
            _ => None,
        }
    }
}

impl<'a> Default for ParseEvents<'a> {
    fn default() -> Self {
        ParseEvents::NoEvent
    }
}

enum EventOrEnd<'a> {
    Event(ParseEvent<'a>, bool),
    End,
}

pub(crate) struct ParseIterator<'a> {
    input: Span<'a>,
    parser: Option<IncrementalReconParser>,
    pending: Option<ParseEvents<'a>>,
}

impl<'a> ParseIterator<'a> {
    pub(crate) fn new(input: Span<'a>) -> Self {
        ParseIterator {
            input,
            parser: Default::default(),
            pending: None,
        }
    }
}

impl<'a> Iterator for ParseIterator<'a> {
    type Item = Result<ParseEvent<'a>, nom::error::Error<Span<'a>>>;

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
                let (rem, mut events) = match parser.as_mut()?.parse(*input) {
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

impl<'a> From<ParseEvent<'a>> for ParseEvents<'a> {
    fn from(event: ParseEvent<'a>) -> Self {
        ParseEvents::SingleEvent(event)
    }
}

pub(crate) fn read_from_header<'a, H: HeaderReader>(
    mut reader: H,
    it: &mut ParseIterator<'a>,
    first_attr: Cow<'a, str>,
) -> Result<H::Body, ParseError> {
    let mut body_reader = reader.read_attribute(first_attr)?;
    body_reader = read_body(body_reader, it, true)?;
    reader = H::restore(body_reader)?;
    loop {
        if let Some(event) = it.next() {
            match event? {
                ParseEvent::StartAttribute(name) => {
                    let mut body_reader = reader.read_attribute(name)?;
                    body_reader = read_body(body_reader, it, true)?;
                    reader = H::restore(body_reader)?;
                }
                ParseEvent::StartBody => {
                    break Ok(read_body(reader.start_body()?, it, false)?);
                }
                _ => break Err(ParseError::InvalidEventStream),
            }
        } else {
            break Ok(reader.start_body()?);
        }
    }
}

pub(crate) fn read_body<B: BodyReader>(
    mut reader: B,
    it: &mut ParseIterator<'_>,
    attr_body: bool,
) -> Result<B, ParseError> {
    loop {
        if let Some(event) = it.next() {
            match event? {
                ParseEvent::Extant => {
                    reader.push_extant()?;
                }
                ParseEvent::TextValue(value) => {
                    reader.push_text(value)?;
                }
                ParseEvent::Number(NumericLiteral::Int(value)) => {
                    if let Ok(n) = i32::try_from(value) {
                        reader.push_i32(n)?;
                    } else {
                        reader.push_i64(value)?;
                    }
                }
                ParseEvent::Number(NumericLiteral::UInt(value)) => {
                    if let Ok(n) = u32::try_from(value) {
                        reader.push_u32(n)?;
                    } else {
                        reader.push_u64(value)?;
                    }
                }
                ParseEvent::Number(NumericLiteral::BigInt(value)) => {
                    reader.push_big_int(value)?;
                }
                ParseEvent::Number(NumericLiteral::BigUint(value)) => {
                    reader.push_big_uint(value)?;
                }
                ParseEvent::Number(NumericLiteral::Float(value)) => {
                    reader.push_f64(value)?;
                }
                ParseEvent::Boolean(value) => {
                    reader.push_bool(value)?;
                }
                ParseEvent::Blob(data) => {
                    reader.push_blob(data)?;
                }
                ParseEvent::StartAttribute(name) => {
                    let delegate_reader = reader.push_record()?;
                    let delegate_body_reader = read_from_header(delegate_reader, it, name)?;
                    reader = B::restore(delegate_body_reader)?;
                }
                ParseEvent::StartBody => {
                    let delegate_body_reader =
                        read_body(reader.push_record()?.start_body()?, it, false)?;
                    reader = B::restore(delegate_body_reader)?;
                }
                ParseEvent::Slot => {
                    reader.start_slot()?;
                }
                ParseEvent::EndAttribute if attr_body => {
                    break Ok(reader);
                }
                ParseEvent::EndRecord if !attr_body => {
                    break Ok(reader);
                }
                _ => {
                    break Err(ParseError::InvalidEventStream);
                }
            };
        } else {
            break Err(ParseError::Structure(ReadError::IncompleteRecord));
        }
    }
}

#[derive(Debug)]
pub struct IncrementalReconParser {
    state: Vec<ParseState>,
}

impl Default for IncrementalReconParser {
    fn default() -> Self {
        IncrementalReconParser {
            state: vec![ParseState::Init],
        }
    }
}

enum FinalState {
    Init,
    AfterAttr,
}

pub struct FinalSegmentParser(FinalState);

impl IncrementalReconParser {
    pub fn into_final_parser(self) -> Option<FinalSegmentParser> {
        let IncrementalReconParser { mut state } = self;
        let top = state.pop();
        if state.is_empty() {
            match top {
                Some(ParseState::Init) => Some(FinalSegmentParser(FinalState::Init)),
                Some(ParseState::AfterAttr) => Some(FinalSegmentParser(FinalState::AfterAttr)),
                _ => None,
            }
        } else {
            None
        }
    }
}

impl<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> for IncrementalReconParser {
    fn parse(
        &mut self,
        input: Span<'a>,
    ) -> IResult<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> {
        let IncrementalReconParser { state } = self;
        let (input, _) = char_str::space0(input)?;
        if let Some(top) = state.last_mut() {
            match top {
                ParseState::Init => {
                    let (input, (events, change)) =
                        map(preceded(char_str::multispace0, opt(parse_init)), |r| {
                            r.unwrap_or_else(|| {
                                (ParseEvent::Extant.single(), StateChange::PopAfterItem)
                            })
                        })(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AfterAttr => {
                    let (input, (events, change)) = map(opt(parse_after_attr), |r| {
                        r.unwrap_or((ParseEvents::NoEvent, StateChange::PopAfterItem))
                    })(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyStartOrNl => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<AttrBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyStartOrNl => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<RecBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<AttrBody>(true),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        char_str::multispace0,
                        parse_not_after_item::<RecBody>(true),
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
                            ParseEvent::Slot.single()
                        } else {
                            ParseEvents::NoEvent
                        }
                    } else {
                        StateChange::PopAfterItem.apply(state);
                        ParseEvent::EndRecord.single()
                    };
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterValue => {
                    let (input, new_state) =
                        preceded(char_str::space0, parse_after_value::<AttrBody>)(input)?;
                    let events = if let Some(new_state) = new_state {
                        *top = new_state;
                        if new_state == ParseState::AttrBodySlot {
                            ParseEvent::Slot.single()
                        } else {
                            ParseEvents::NoEvent
                        }
                    } else {
                        StateChange::PopAfterItem.apply(state);
                        ParseEvent::EndAttribute.single()
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
                        ParseEvent::EndRecord.single()
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
                        StateChange::PopAfterItem.apply(state);
                        ParseEvent::EndAttribute.single()
                    };
                    Ok((input, events))
                }
                ParseState::RecordBodySlot => {
                    let (input, (events, change)) =
                        preceded(char_str::multispace0, parse_slot_value::<RecBody>)(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodySlot => {
                    let (input, (events, change)) =
                        preceded(char_str::multispace0, parse_slot_value::<AttrBody>)(input)?;
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
        let FinalSegmentParser(state) = self;
        match state {
            FinalState::Init => parse_init_final(input),
            FinalState::AfterAttr => parse_after_attr_final(input),
        }
    }
}

fn parse_init_final(input: Span<'_>) -> IResult<Span<'_>, ParseEvents<'_>> {
    alt((
        map(complete::identifier_or_bool, |v| {
            identifier_event(v).single()
        }),
        map(complete::numeric_literal, |l| {
            ParseEvent::Number(l).single()
        }),
        map(complete::blob, |data| ParseEvent::Blob(data).single()),
        attr_final,
    ))(input)
}

fn parse_after_attr_final(input: Span<'_>) -> IResult<Span<'_>, ParseEvents<'_>> {
    alt((
        map(complete::identifier_or_bool, |v| {
            identifier_event(v).singleton_body()
        }),
        map(complete::numeric_literal, |l| {
            ParseEvent::Number(l).singleton_body()
        }),
        map(complete::blob, |data| {
            ParseEvent::Blob(data).singleton_body()
        }),
        attr_final,
    ))(input)
}

fn parse_init(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| {
            (ParseEvent::TextValue(s).single(), StateChange::PopAfterItem)
        }),
        map(complete::identifier_or_bool, |v| {
            (identifier_event(v).single(), StateChange::PopAfterItem)
        }),
        map(complete::numeric_literal, |l| {
            (ParseEvent::Number(l).single(), StateChange::PopAfterItem)
        }),
        map(complete::blob, |data| {
            (ParseEvent::Blob(data).single(), StateChange::PopAfterItem)
        }),
        secondary_attr,
        map(char_str::char('{'), |_| {
            (
                ParseEvent::StartBody.single(),
                StateChange::ChangeState(ParseState::RecordBodyStartOrNl),
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
                ParseEvent::StartAttribute(name).single(),
                StateChange::PushAttrNewRec(true),
            )
        } else {
            (
                ParseEvent::StartAttribute(name).followed_by(ParseEvent::EndAttribute),
                StateChange::PushAttrNewRec(false),
            )
        }
    })(input)
}

fn secondary_attr(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    map(attr, |(name, has_body)| {
        if has_body {
            (
                ParseEvent::StartAttribute(name).single(),
                StateChange::PushAttr,
            )
        } else {
            (
                ParseEvent::StartAttribute(name).followed_by(ParseEvent::EndAttribute),
                StateChange::ChangeState(ParseState::AfterAttr),
            )
        }
    })(input)
}

fn identifier_event(value: Either<&str, bool>) -> ParseEvent<'_> {
    match value {
        Either::Left(s) => ParseEvent::TextValue(Cow::Borrowed(s)),
        Either::Right(p) => ParseEvent::Boolean(p),
    }
}

fn parse_after_attr(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| {
            (
                ParseEvent::TextValue(s).singleton_body(),
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
                ParseEvent::Number(l).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        map(blob, |data| {
            (
                ParseEvent::Blob(data).singleton_body(),
                StateChange::PopAfterItem,
            )
        }),
        secondary_attr,
        map(char_str::char('{'), |_| {
            (
                ParseEvent::StartBody.single(),
                StateChange::ChangeState(ParseState::RecordBodyStartOrNl),
            )
        }),
        map(
            peek(alt((
                recognize(alt((seperator, char_str::one_of(")}")))),
                char_str::line_ending,
            ))),
            |_| {
                (
                    ParseEvent::StartBody.followed_by(ParseEvent::EndRecord),
                    StateChange::PopAfterItem,
                )
            },
        ),
    ))(input)
}

fn value_item<K: ItemsKind>(event: ParseEvent<'_>) -> (ParseEvents<'_>, StateChange) {
    (event.single(), StateChange::ChangeState(K::after_value()))
}

fn slot_item<K: ItemsKind>(event: ParseEvent<'_>) -> (ParseEvents<'_>, StateChange) {
    (event.single(), StateChange::ChangeState(K::after_slot()))
}

trait ItemsKind {
    fn start_or_nl() -> ParseState;
    fn after_sep() -> ParseState;
    fn start_slot() -> ParseState;
    fn after_value() -> ParseState;
    fn after_slot() -> ParseState;
    fn end_delim() -> char;
    fn end_event<'a>() -> ParseEvent<'a>;
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

    fn end_event<'a>() -> ParseEvent<'a> {
        ParseEvent::EndAttribute
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

    fn end_event<'a>() -> ParseEvent<'a> {
        ParseEvent::EndRecord
    }

    fn after_value() -> ParseState {
        ParseState::RecordBodyAfterValue
    }

    fn after_slot() -> ParseState {
        ParseState::RecordBodyAfterSlot
    }
}

fn parse_not_after_item<K: ItemsKind>(
    item_required: bool,
) -> impl for<'a> Fn(Span<'a>) -> IResult<Span<'a>, (ParseEvents<'a>, StateChange)> {
    move |input: Span<'_>| {
        alt((
            map(string_literal, |s| {
                value_item::<K>(ParseEvent::TextValue(s))
            }),
            map(identifier_or_bool, |v| value_item::<K>(identifier_event(v))),
            map(numeric_literal, |l| value_item::<K>(ParseEvent::Number(l))),
            map(blob, |data| value_item::<K>(ParseEvent::Blob(data))),
            map(seperator, |_| {
                (
                    ParseEvent::Extant.single(),
                    StateChange::ChangeState(K::after_sep()),
                )
            }),
            map(char_str::char(':'), |_| {
                (
                    ParseEvent::Extant.followed_by(ParseEvent::Slot),
                    StateChange::ChangeState(K::start_slot()),
                )
            }),
            map(char_str::char(K::end_delim()), move |_| {
                let event = K::end_event();
                let events = if item_required {
                    ParseEvent::Extant.followed_by(event)
                } else {
                    event.single()
                };
                (events, StateChange::PopAfterItem)
            }),
            primary_attr,
            map(char_str::char('{'), |_| {
                (ParseEvent::StartBody.single(), StateChange::PushBody)
            }),
        ))(input)
    }
}

fn parse_slot_value<K: ItemsKind>(
    input: Span<'_>,
) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| slot_item::<K>(ParseEvent::TextValue(s))),
        map(identifier_or_bool, |v| slot_item::<K>(identifier_event(v))),
        map(numeric_literal, |l| slot_item::<K>(ParseEvent::Number(l))),
        map(blob, |data| slot_item::<K>(ParseEvent::Blob(data))),
        map(seperator, |_| {
            (
                ParseEvent::Extant.single(),
                StateChange::ChangeState(K::after_sep()),
            )
        }),
        map(char_str::char(K::end_delim()), move |_| {
            (
                ParseEvent::Extant.followed_by(K::end_event()),
                StateChange::PopAfterItem,
            )
        }),
        primary_attr,
        map(char_str::char('{'), |_| {
            (ParseEvent::StartBody.single(), StateChange::PushBody)
        }),
    ))(input)
}

fn parse_after_value<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(char_str::line_ending, |_| Some(K::start_or_nl())),
        map(seperator, |_| Some(K::after_sep())),
        map(char_str::char(':'), |_| Some(K::start_slot())),
        map(char_str::char(K::end_delim()), |_| None),
    ))(input)
}

fn parse_after_slot<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(char_str::line_ending, |_| Some(K::start_or_nl())),
        map(seperator, |_| Some(K::after_sep())),
        map(char_str::char(K::end_delim()), |_| None),
    ))(input)
}
