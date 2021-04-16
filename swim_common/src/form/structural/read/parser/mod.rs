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

use crate::form::structural::read::parser::error::ParseError;
use crate::form::structural::read::{BodyReader, HeaderReader, ReadError, StructuralReadable};
use crate::model::parser::{is_identifier_char, is_identifier_start, unescape};
use crate::model::text::Text;
use either::Either;
use nom::branch::alt;
use nom::bytes::streaming::tag_no_case;
use nom::character::streaming as character;
use nom::combinator::{map, map_res, opt, recognize};
use nom::multi::{many0_count, many1_count};
use nom::number::streaming as number;
use nom::sequence::{delimited, pair, preceded, tuple};
use nom::{Finish, IResult, Parser};
use nom_locate::LocatedSpan;
use num_bigint::{BigInt, BigUint, ParseBigIntError, Sign};
use num_traits::Num;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::ops::Neg;

pub mod error;

type Span<'a> = LocatedSpan<&'a str>;

pub enum ParseEvent<'a> {
    Extant,
    TextValue(Cow<'a, str>),
    Number(NumericLiteral),
    Boolean(bool),
    Blob(Vec<u8>),
    StartAttribute(Cow<'a, str>),
    EndAttribute,
    StartBody,
    Slot,
    EndRecord,
}

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

fn unwrap_span(span: Span<'_>) -> &str {
    *span
}

fn identifier(input: Span<'_>) -> IResult<Span<'_>, &str> {
    map(
        recognize(pair(
            character::satisfy(is_identifier_start),
            many0_count(character::satisfy(is_identifier_char)),
        )),
        unwrap_span,
    )(input)
}

fn identifier_or_bool(input: Span<'_>) -> IResult<Span<'_>, Either<&str, bool>> {
    map(identifier, |id| match id {
        "true" => Either::Right(true),
        "false" => Either::Right(false),
        _ => Either::Left(id),
    })(input)
}

fn escape(input: Span<'_>) -> IResult<Span<'_>, &str> {
    map(
        recognize(pair(character::char('\\'), character::anychar)),
        unwrap_span,
    )(input)
}

fn string_literal(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    map_res(
        delimited(
            character::char('"'),
            recognize(many0_count(alt((
                recognize(character::satisfy(|c| c != '\\' && c != '\"')),
                recognize(escape),
            )))),
            character::char('"'),
        ),
        resolve_escapes,
    )(input)
}

#[derive(Debug)]
pub struct InvalidEscapes(Text);

impl Display for InvalidEscapes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\" contains invalid escape sequences.", self.0)
    }
}

impl std::error::Error for InvalidEscapes {}

fn resolve_escapes(span: Span<'_>) -> Result<Cow<'_, str>, InvalidEscapes> {
    let input = *span;
    if input.contains('\\') {
        match unescape(input) {
            Ok(text) => Ok(Cow::Owned(text.into())),
            Err(text) => Err(InvalidEscapes(text)),
        }
    } else {
        Ok(Cow::Borrowed(input))
    }
}

fn natural(
    tag: &'static str,
    digits: &'static str,
) -> impl FnMut(Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    move |input: Span<'_>| {
        preceded(
            tag_no_case(tag),
            recognize(many1_count(character::one_of(digits))),
        )(input)
    }
}

pub enum NumericLiteral {
    Int(i64),
    UInt(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float(f64),
}

fn numeric_literal(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
    alt((
        binary,
        hexadecimal,
        decimal,
        map(number::double, NumericLiteral::Float),
    ))(input)
}

fn signed<F>(mut base: F) -> impl FnMut(Span<'_>) -> IResult<Span<'_>, (bool, Span<'_>)>
where
    F: FnMut(Span<'_>) -> IResult<Span<'_>, Span<'_>>,
{
    move |input: Span<'_>| {
        pair(
            map(opt(character::char('-')), |maybe| maybe.is_some()),
            &mut base,
        )(input)
    }
}

fn hexadecimal_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    natural("0x", "0123456789abcdefABCDEF")(input)
}

fn hexadecimal(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
    map_res(signed(hexadecimal_str), |(negative, rep)| {
        try_to_int_literal(negative, *rep, 16)
    })(input)
}

fn binary_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    natural("0b", "01")(input)
}

fn binary(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
    map_res(signed(binary_str), |(negative, rep)| {
        try_to_int_literal(negative, *rep, 2)
    })(input)
}

fn decimal_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(many1_count(character::one_of("0123456789")))(input)
}

fn decimal(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
    map_res(signed(decimal_str), |(negative, rep)| {
        try_to_int_literal(negative, *rep, 10)
    })(input)
}

fn try_to_int_literal(
    negative: bool,
    rep: &str,
    radix: u32,
) -> Result<NumericLiteral, ParseBigIntError> {
    if let Ok(n) = u64::from_str_radix(rep, radix) {
        if negative {
            if let Ok(m) = i64::try_from(n) {
                Ok(NumericLiteral::Int(-m))
            } else {
                Ok(NumericLiteral::BigInt(BigInt::from(n).neg()))
            }
        } else {
            Ok(NumericLiteral::UInt(n))
        }
    } else {
        let n = BigUint::from_str_radix(rep, radix)?;
        if negative {
            Ok(NumericLiteral::BigInt(BigInt::from_biguint(Sign::Minus, n)))
        } else {
            Ok(NumericLiteral::BigUint(n))
        }
    }
}

fn attr_name(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    alt((string_literal, map(identifier, Cow::Borrowed)))(input)
}

pub enum ParseEvents<'a> {
    NoEvent,
    SingleEvent(ParseEvent<'a>),
    TwoEvents(ParseEvent<'a>, ParseEvent<'a>),
    ThreeEvents(ParseEvent<'a>, ParseEvent<'a>, ParseEvent<'a>),
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

struct ParseIterator<'a> {
    input: Span<'a>,
    parser: Option<IncrementalReconParser>,
    pending: Option<ParseEvents<'a>>,
}

impl<'a> ParseIterator<'a> {
    fn new(input: Span<'a>) -> Self {
        ParseIterator {
            input,
            parser: Default::default(),
            pending: None,
        }
    }
}

pub fn parse_iterator(
    input: Span<'_>,
) -> impl Iterator<Item = Result<ParseEvent<'_>, nom::error::Error<Span<'_>>>> + '_ {
    ParseIterator::new(input)
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
            let p = parser.as_mut()?;
            loop {
                match p.parse(*input).finish() {
                    Ok((rem, mut events)) => {
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
                    Err(e) => {
                        *parser = None;
                        return Some(Err(e));
                    }
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

pub fn parse_from_str<T: StructuralReadable>(input: Span<'_>) -> Result<T, ParseError> {
    let mut iterator = ParseIterator::new(input);
    let parsed = if let Some(event) = iterator.next() {
        match event? {
            ParseEvent::Extant => T::read_extant()?,
            ParseEvent::TextValue(value) => T::read_text(value)?,
            ParseEvent::Number(NumericLiteral::Int(value)) => {
                if let Ok(n) = i32::try_from(value) {
                    T::read_i32(n)?
                } else {
                    T::read_i64(value)?
                }
            }
            ParseEvent::Number(NumericLiteral::UInt(value)) => {
                if let Ok(n) = u32::try_from(value) {
                    T::read_u32(n)?
                } else {
                    T::read_u64(value)?
                }
            }
            ParseEvent::Number(NumericLiteral::BigInt(value)) => T::read_big_int(value)?,
            ParseEvent::Number(NumericLiteral::BigUint(value)) => T::read_big_uint(value)?,
            ParseEvent::Number(NumericLiteral::Float(value)) => T::read_f64(value)?,
            ParseEvent::Boolean(value) => T::read_bool(value)?,
            ParseEvent::Blob(data) => T::read_blob(data)?,
            ParseEvent::StartAttribute(name) => {
                let header_reader = T::record_reader()?;
                let body_reader = read_from_header(header_reader, &mut iterator, name)?;
                T::try_terminate(body_reader)?
            }
            ParseEvent::StartBody => {
                let body_reader =
                    read_body(T::record_reader()?.start_body()?, &mut iterator, false)?;
                T::try_terminate(body_reader)?
            }
            _ => {
                return Err(ParseError::InvalidEventStream);
            }
        }
    } else {
        T::read_extant()?
    };
    Ok(parsed)
}

fn read_from_header<'a, H: HeaderReader>(
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

fn read_body<B: BodyReader>(
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

trait ReconEventParser: for<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> {}

impl<P> ReconEventParser for P where
    P: for<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>>
{
}

impl<'a> Parser<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> for IncrementalReconParser {
    fn parse(
        &mut self,
        input: Span<'a>,
    ) -> IResult<Span<'a>, ParseEvents<'a>, nom::error::Error<Span<'a>>> {
        let IncrementalReconParser { state } = self;
        let (input, _) = character::space0(input)?;
        if let Some(top) = state.last_mut() {
            match top {
                ParseState::Init => {
                    let (input, (events, change)) =
                        map(preceded(character::multispace0, opt(parse_init)), |r| {
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
                        character::multispace0,
                        parse_not_after_item::<AttrBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyStartOrNl => {
                    let (input, (events, change)) = preceded(
                        character::multispace0,
                        parse_not_after_item::<RecBody>(false),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        character::multispace0,
                        parse_not_after_item::<AttrBody>(true),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodyAfterSep => {
                    let (input, (events, change)) = preceded(
                        character::multispace0,
                        parse_not_after_item::<RecBody>(true),
                    )(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::RecordBodyAfterValue => {
                    let (input, new_state) =
                        preceded(character::space0, parse_after_value::<RecBody>)(input)?;
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
                        preceded(character::space0, parse_after_value::<AttrBody>)(input)?;
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
                        preceded(character::space0, parse_after_slot::<RecBody>)(input)?;
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
                        preceded(character::space0, parse_after_slot::<AttrBody>)(input)?;
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
                        preceded(character::multispace0, parse_slot_value::<RecBody>)(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
                ParseState::AttrBodySlot => {
                    let (input, (events, change)) =
                        preceded(character::multispace0, parse_slot_value::<AttrBody>)(input)?;
                    change.apply(state);
                    Ok((input, events))
                }
            }
        } else {
            Ok((input, ParseEvents::End))
        }
    }
}

fn parse_init(input: Span<'_>) -> IResult<Span<'_>, (ParseEvents<'_>, StateChange)> {
    alt((
        map(string_literal, |s| {
            (ParseEvent::TextValue(s).single(), StateChange::PopAfterItem)
        }),
        map(identifier_or_bool, |v| {
            (identifier_event(v).single(), StateChange::PopAfterItem)
        }),
        map(numeric_literal, |l| {
            (ParseEvent::Number(l).single(), StateChange::PopAfterItem)
        }),
        map(blob, |data| {
            (ParseEvent::Blob(data).single(), StateChange::PopAfterItem)
        }),
        secondary_attr,
        map(character::char('{'), |_| {
            (
                ParseEvent::StartBody.single(),
                StateChange::ChangeState(ParseState::RecordBodyStartOrNl),
            )
        }),
    ))(input)
}

fn attr(input: Span<'_>) -> IResult<Span<'_>, (Cow<'_, str>, bool)> {
    preceded(
        character::char('@'),
        pair(attr_name, map(opt(character::char('(')), |o| o.is_some())),
    )(input)
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
        map(character::char('{'), |_| {
            (
                ParseEvent::StartBody.single(),
                StateChange::ChangeState(ParseState::RecordBodyStartOrNl),
            )
        }),
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
            map(character::char(':'), |_| {
                (
                    ParseEvent::Extant.followed_by(ParseEvent::Slot),
                    StateChange::ChangeState(K::start_slot()),
                )
            }),
            map(character::char(K::end_delim()), move |_| {
                let event = K::end_event();
                let events = if item_required {
                    ParseEvent::Extant.followed_by(event)
                } else {
                    event.single()
                };
                (events, StateChange::PopAfterItem)
            }),
            primary_attr,
            map(character::char('{'), |_| {
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
        map(character::char(K::end_delim()), move |_| {
            (
                ParseEvent::Extant.followed_by(K::end_event()),
                StateChange::PopAfterItem,
            )
        }),
        primary_attr,
        map(character::char('{'), |_| {
            (ParseEvent::StartBody.single(), StateChange::PushBody)
        }),
    ))(input)
}

fn seperator(input: Span<'_>) -> IResult<Span<'_>, char> {
    character::one_of(",;")(input)
}

fn parse_after_value<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(character::newline, |_| Some(K::start_or_nl())),
        map(seperator, |_| Some(K::after_sep())),
        map(character::char(':'), |_| Some(K::start_slot())),
        map(character::char(K::end_delim()), |_| None),
    ))(input)
}

fn parse_after_slot<K: ItemsKind>(input: Span<'_>) -> IResult<Span<'_>, Option<ParseState>> {
    alt((
        map(character::newline, |_| Some(K::start_or_nl())),
        map(seperator, |_| Some(K::after_sep())),
        map(character::char(K::end_delim()), |_| None),
    ))(input)
}

fn base64_digit(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '+' || c == '/'
}

fn base64_digit_or_padding(c: char) -> bool {
    base64_digit(c) || c == '='
}

fn base64_block(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    let digit = character::satisfy(base64_digit);
    let mut block = recognize(tuple((&digit, &digit, &digit, &digit)));
    block(input)
}

fn base64_final_block(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    let digit = character::satisfy(base64_digit);
    let padding = character::satisfy(base64_digit_or_padding);
    let mut block = recognize(tuple((&digit, &digit, &padding, &padding)));
    block(input)
}

fn base64(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    recognize(pair(many0_count(base64_block), base64_final_block))(input)
}

fn base64_literal(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    preceded(character::char('%'), base64)(input)
}

fn blob(input: Span<'_>) -> IResult<Span<'_>, Vec<u8>> {
    map_res(base64_literal, |span| base64::decode(*span))(input)
}
