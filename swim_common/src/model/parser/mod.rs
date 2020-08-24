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

use std::borrow::Borrow;
use std::io;
use std::str::FromStr;
use std::str::{from_utf8, from_utf8_unchecked, Utf8Error};

use bytes::{Buf, BytesMut};
use num_bigint::{BigInt, BigUint};

use token_buffer::*;

use crate::model::{Attr, Item, Value};
use core::iter;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use utilities::iteratee::{look_ahead, unfold_with_flush, Iteratee};

#[cfg(test)]
mod tests;

mod token_buffer;

/// Determine if a character is valid at the start of an identifier.
///
/// #Examples
///
/// ```
/// use swim_common::model::parser::is_identifier_start;
///
/// assert!(is_identifier_start('a'));
/// assert!(is_identifier_start('ℵ'));
/// assert!(is_identifier_start('_'));
/// assert!(!is_identifier_start('2'));
/// assert!(!is_identifier_start('-'));
/// assert!(!is_identifier_start('@'));
///
/// ```
pub fn is_identifier_start(c: char) -> bool {
    c >= 'A' && c <= 'Z'
        || c == '_'
        || c >= 'a' && c <= 'z'
        || c == '\u{b7}'
        || c >= '\u{c0}' && c <= '\u{d6}'
        || c >= '\u{d8}' && c <= '\u{f6}'
        || c >= '\u{f8}' && c <= '\u{37d}'
        || c >= '\u{37f}' && c <= '\u{1fff}'
        || c >= '\u{200c}' && c <= '\u{200d}'
        || c >= '\u{203f}' && c <= '\u{2040}'
        || c >= '\u{2070}' && c <= '\u{218f}'
        || c >= '\u{2c00}' && c <= '\u{2fef}'
        || c >= '\u{3001}' && c <= '\u{d7ff}'
        || c >= '\u{f900}' && c <= '\u{fdcf}'
        || c >= '\u{fdf0}' && c <= '\u{fffd}'
        || c >= '\u{10000}' && c <= '\u{effff}'
}

/// Determine if a character is valid after the start of an identifier.
///
/// #Examples
///
/// ```
///
/// use swim_common::model::parser::is_identifier_char;
/// assert!(is_identifier_char('a'));
/// assert!(is_identifier_char('ℵ'));
/// assert!(is_identifier_char('_'));
/// assert!(is_identifier_char('2'));
/// assert!(is_identifier_char('-'));
/// assert!(!is_identifier_char('@'));
///
/// ```
pub fn is_identifier_char(c: char) -> bool {
    is_identifier_start(c) || c == '-' || c >= '0' && c <= '9'
}

/// Determine if a string is a valid Recon identifier.
///
/// #Examples
///
/// ```
/// use swim_common::model::parser::is_identifier;
///
/// assert!(is_identifier("name"));
/// assert!(is_identifier("name2"));
/// assert!(is_identifier("_name"));
/// assert!(is_identifier("two_parts"));
/// assert!(!is_identifier("2morrow"));
/// assert!(!is_identifier("@tag"));
///
/// ```
///
pub fn is_identifier(name: &str) -> bool {
    if name == "true" || name == "false" {
        false
    } else {
        let mut name_chars = name.chars();
        match name_chars.next() {
            Some(c) if is_identifier_start(c) => name_chars.all(is_identifier_char),
            _ => false,
        }
    }
}

/// Errors that can occur when splitting a stream of characters into ['ReconToken']s.
#[derive(Eq, Clone, Copy, Hash, Debug, PartialEq, Ord, PartialOrd)]
pub enum TokenError {
    NoClosingQuote,
    InvalidInteger,
    InvalidFloat,
    BadStartChar,
}

/// Errors that can occur when parsing a ['Value'] from a stream of ['ReconToken']s.
#[derive(Eq, Clone, Copy, Hash, Debug, PartialEq, Ord, PartialOrd)]
pub enum RecordError {
    InvalidValue,
    NonValueToken,
    BadValueStart,
    BadRecordStart,
    InvalidStringLiteral,
    InvalidAttributeName,
    InvalidAttributeValue,
    InvalidItemStart,
    InvalidAfterItem,
    InvalidSlotValue,
    BadStack,
    BadStackOnClose,
    EmptyStackOnClose,
}

/// Parse failure indicating that an invalid token was encountered.
#[derive(Eq, Clone, Hash, Debug, PartialEq, Ord, PartialOrd)]
pub struct BadToken(Location, TokenError);

/// Parse failure indicated that an invalid sequence of tokens was encountered.
#[derive(Eq, Clone, Hash, Debug, PartialEq, Ord, PartialOrd)]
pub struct BadRecord(Location, RecordError);

/// Failure type for Recon parse operations.
#[derive(Eq, Clone, Hash, Debug, PartialEq)]
pub enum ParseFailure {
    /// An invalid token was encountered.
    TokenizationFailure(BadToken),

    /// An inappropriate token was found in the token stream.
    InvalidToken(BadRecord),

    /// More tokens were expected when the end of the stream was reached.
    IncompleteRecord,

    /// Tokens remained in the input when the parse operation completed.
    UnconsumedInput,
}

impl Display for ParseFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseFailure::TokenizationFailure(BadToken(location, _)) => {
                write!(f, "Bad token at: {}:{}", location.line, location.column)
            }
            ParseFailure::InvalidToken(BadRecord(location, err)) => write!(
                f,
                "Token at {}:{} is not valid in this context: {:?}",
                location.line, location.column, *err
            ),
            ParseFailure::IncompleteRecord => {
                write!(f, "The input ended before the record was complete.")
            }
            ParseFailure::UnconsumedInput => {
                write!(f, "Some content from the input was not consumed.")
            }
        }
    }
}

impl Error for ParseFailure {}

impl From<BadToken> for ParseFailure {
    fn from(err: BadToken) -> Self {
        ParseFailure::TokenizationFailure(err)
    }
}

impl From<BadRecord> for ParseFailure {
    fn from(err: BadRecord) -> Self {
        ParseFailure::InvalidToken(err)
    }
}

type TokenErrOrTerminator<'a> = Option<Result<LocatedReconToken<&'a str>, BadToken>>;

fn tokenize_and_terminate(repr: &str) -> impl Iterator<Item = TokenErrOrTerminator> {
    tokenize_str(repr).map(Some).chain(iter::once(None))
}

/// Parse a stream of ['Value']s from a stream of characters. More values will be read until either
/// an error is encountered or the end of the stream is reached.
pub fn parse_all(repr: &str) -> impl Iterator<Item = Result<Value, ParseFailure>> + '_ {
    let tokens = tokenize_and_terminate(repr);

    tokens
        .scan(Some(vec![]), |maybe_state, maybe_token| {
            if let Some(state) = maybe_state {
                let current = match maybe_token {
                    Some(Ok(token)) => match consume_token(state, token) {
                        Some(ParseTermination::Failed(err)) => {
                            *maybe_state = None;
                            Some(Err(err))
                        }
                        Some(ParseTermination::EarlyTermination(value)) => Some(Ok(value)),
                        _ => None,
                    },
                    Some(Err(err)) => Some(Err(ParseFailure::TokenizationFailure(err))),
                    _ => handle_tok_stream_end(state),
                };
                Some(current)
            } else {
                None
            }
        })
        .flatten()
}

/// Parses a Recon document (a sequence of [`Item`]s without an enclosing body) from a string.
pub fn parse_document(repr: &str) -> Result<Vec<Item>, ParseFailure> {
    let mut tokens = tokenize_str(repr);

    let init_state = vec![Frame::new_record(StartAt::RecordBody)];

    let final_state = tokens.try_fold(init_state, |mut state, maybe_token| {
        let token = maybe_token.map_err(ParseFailure::TokenizationFailure)?;
        let location = token.1;
        match consume_token(&mut state, token) {
            Some(ParseTermination::Failed(err)) => Err(err),
            Some(ParseTermination::EarlyTermination(_)) => Err(ParseFailure::InvalidToken(
                BadRecord(location, RecordError::BadStackOnClose),
            )),
            _ => Ok(state),
        }
    });

    final_state.and_then(handle_document_end)
}

///Creates a final ['Value'] from the contents of the stack, where possible.
fn handle_tok_stream_end(state: &mut Vec<Frame>) -> Option<Result<Value, ParseFailure>> {
    let depth = state.len();
    match state.pop() {
        Some(Frame {
            attrs,
            items,
            parse_state,
        }) if depth == 1 => Some(end_record(attrs, items, parse_state)),
        _ => None,
    }
}

fn end_record(
    mut attrs: Vec<Attr>,
    items: Vec<Item>,
    parse_state: ValueParseState,
) -> Result<Value, ParseFailure> {
    match parse_state {
        ValueParseState::ReadingAttribute(name) => {
            attrs.push(Attr {
                name,
                value: Value::Extant,
            });
            Ok(Value::Record(attrs, items))
        }
        ValueParseState::AfterAttribute => Ok(Value::Record(attrs, items)),
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

///Creates a final vector fo [`Item`]s from the contents of the stack, where possible.
fn handle_document_end(mut state: Vec<Frame>) -> Result<Vec<Item>, ParseFailure> {
    if state.len() > 2 {
        return Err(ParseFailure::IncompleteRecord);
    }

    match state.pop() {
        Some(Frame {
            attrs,
            items,
            parse_state,
            ..
        }) => match state.pop() {
            Some(bottom) => {
                let value = end_record(attrs, items, parse_state)?;
                let Frame {
                    mut items,
                    mut parse_state,
                    ..
                } = bottom;
                match parse_state {
                    ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) => {
                        parse_state = ValueParseState::Single(p, value);
                    }
                    ValueParseState::ReadingSlot(p, key) => {
                        items.push(Item::Slot(key, value));
                        parse_state = ValueParseState::AfterSlot(p);
                    }
                    _ => {
                        return Err(ParseFailure::IncompleteRecord);
                    }
                }
                end_document(items, parse_state)
            }
            _ => end_document(items, parse_state),
        },
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

fn end_document(
    mut items: Vec<Item>,
    parse_state: ValueParseState,
) -> Result<Vec<Item>, ParseFailure> {
    match parse_state {
        ValueParseState::RecordStart(false) | ValueParseState::AfterSlot(false) => Ok(items),
        ValueParseState::InsideBody(false, _) => Ok(items),
        ValueParseState::Single(false, value) => {
            items.push(Item::ValueItem(value));
            Ok(items)
        }
        ValueParseState::ReadingSlot(false, key) => {
            items.push(Item::Slot(key, Value::Extant));
            Ok(items)
        }
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

/// Iteratee converting Recon tokens into Recon values.
fn from_tokens_iteratee(
) -> impl Iteratee<LocatedReconToken<String>, Item = Result<Value, ParseFailure>> {
    unfold_with_flush(
        vec![],
        |state: &mut Vec<Frame>, loc_token: LocatedReconToken<String>| {
            consume_token(state, loc_token).map(|res| match res {
                ParseTermination::EarlyTermination(value) => Ok(value),
                ParseTermination::Failed(failure) => Err(failure),
            })
        },
        |mut state: Vec<Frame>| handle_tok_stream_end(&mut state),
    )
}

/// Iteratee converting Recon tokens into a Recon document.
fn from_tokens_document_iteratee(
) -> impl Iteratee<LocatedReconToken<String>, Item = Result<Vec<Item>, ParseFailure>> {
    let init_state = vec![Frame::new_record(StartAt::RecordBody)];
    unfold_with_flush(
        init_state,
        |state: &mut Vec<Frame>, loc_token: LocatedReconToken<String>| {
            let location = loc_token.1;
            consume_token(state, loc_token).map(|res| match res {
                ParseTermination::EarlyTermination(_) => Err(ParseFailure::InvalidToken(
                    BadRecord(location, RecordError::BadStackOnClose),
                )),
                ParseTermination::Failed(failure) => Err(failure),
            })
        },
        |state: Vec<Frame>| Some(handle_document_end(state)),
    )
}

/// Iteratee that parses a stream of UTF characters into Recon ['Value']s.
pub fn parse_iteratee() -> impl Iteratee<(usize, char), Item = Result<Value, ParseFailure>> {
    tokenize_iteratee()
        .and_then_fallible(from_tokens_iteratee())
        .fuse_on_error()
}

/// Iteratee that parses a stream of UTF characters into a recon document.
pub fn parse_document_iteratee(
) -> impl Iteratee<(usize, char), Item = Result<Vec<Item>, ParseFailure>> {
    tokenize_iteratee()
        .and_then_fallible(from_tokens_document_iteratee())
        .fuse()
}

/// Parse exactly one ['Value'] from the input, returning an error if the string does not contain
/// the representation of exactly one.
pub fn parse_single(repr: &str) -> Result<Value, ParseFailure> {
    let mut value_iter = parse_all(repr);
    match value_iter.next() {
        Some(res @ Ok(_)) => match value_iter.next() {
            Some(v) => {
                println!("{:?}", v);
                Err(ParseFailure::UnconsumedInput)
            }
            _ => res,
        },
        Some(err @ Err(_)) => err,
        _ => Err(ParseFailure::IncompleteRecord),
    }
}

/// Unescape a string using Java conventions. Returns the input as a failure if the string
/// contains an invalid escape.
///
/// TODO Handle escaped UTF-16 surrogate pairs.
pub fn unescape(literal: &str) -> Result<String, String> {
    let mut failed = false;
    let unescaped_string = literal
        .chars()
        .scan(EscapeState::None, |state, c| {
            Some(match state {
                EscapeState::None => {
                    if c == '\\' {
                        *state = EscapeState::Escape;
                        None
                    } else {
                        Some(c)
                    }
                }
                EscapeState::Escape if is_escape(c) => {
                    *state = EscapeState::None;
                    Some(match c {
                        '\\' => '\\',
                        '\"' => '\"',
                        'b' => '\u{08}',
                        'f' => '\u{0c}',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        ow => ow,
                    })
                }
                EscapeState::Escape if c == 'u' => {
                    *state = EscapeState::UnicodeEscape0;
                    None
                }
                EscapeState::UnicodeEscape0 if c == 'u' => None,
                EscapeState::UnicodeEscape0 if c.is_digit(16) => {
                    *state = EscapeState::UnicodeEscape1(c.to_digit(16).unwrap());
                    None
                }
                EscapeState::UnicodeEscape1(d1) if c.is_digit(16) => {
                    *state = EscapeState::UnicodeEscape2(*d1, c.to_digit(16).unwrap());
                    None
                }
                EscapeState::UnicodeEscape2(d1, d2) if c.is_digit(16) => {
                    *state = EscapeState::UnicodeEscape3(*d1, *d2, c.to_digit(16).unwrap());
                    None
                }
                EscapeState::UnicodeEscape3(d1, d2, d3) if c.is_digit(16) => {
                    let uc: char = char::try_from(
                        (*d1 << 12) | (*d2 << 8) | (*d3 << 4) | c.to_digit(16).unwrap(),
                    )
                    .unwrap();
                    *state = EscapeState::None;
                    Some(uc)
                }
                EscapeState::Failed => None,
                _ => {
                    *state = EscapeState::Failed;
                    failed = true;
                    None
                }
            })
        })
        .flatten()
        .collect();
    if failed {
        Err(literal.to_owned())
    } else {
        Ok(unescaped_string)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ParseTermination {
    Failed(ParseFailure),
    EarlyTermination(Value),
}

trait TokenStr: PartialEq + Borrow<str> + Into<String> + Debug {}

impl<'a> TokenStr for &'a str {}

impl<'a> TokenStr for String {}

#[derive(PartialEq, Debug)]
enum ReconToken<S> {
    AttrMarker,
    AttrBodyStart,
    AttrBodyEnd,
    RecordBodyStart,
    RecordBodyEnd,
    SlotDivider,
    EntrySep,
    NewLine,
    Identifier(S),
    StringLiteral(S),
    Int32Literal(i32),
    Int64Literal(i64),
    UInt32Literal(u32),
    UInt64Literal(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float64Literal(f64),
    BoolLiteral(bool),
}

impl ReconToken<&str> {
    #[cfg(test)]
    fn owned_token(&self) -> ReconToken<String> {
        match *self {
            ReconToken::AttrMarker => ReconToken::AttrMarker,
            ReconToken::AttrBodyStart => ReconToken::AttrBodyStart,
            ReconToken::AttrBodyEnd => ReconToken::AttrBodyEnd,
            ReconToken::RecordBodyStart => ReconToken::RecordBodyStart,
            ReconToken::RecordBodyEnd => ReconToken::RecordBodyEnd,
            ReconToken::SlotDivider => ReconToken::SlotDivider,
            ReconToken::EntrySep => ReconToken::EntrySep,
            ReconToken::NewLine => ReconToken::NewLine,
            ReconToken::Identifier(id) => ReconToken::Identifier(id.to_owned()),
            ReconToken::StringLiteral(s) => ReconToken::StringLiteral(s.to_owned()),
            ReconToken::Int32Literal(n) => ReconToken::Int32Literal(n),
            ReconToken::Int64Literal(n) => ReconToken::Int64Literal(n),
            ReconToken::UInt32Literal(n) => ReconToken::UInt32Literal(n),
            ReconToken::UInt64Literal(n) => ReconToken::UInt64Literal(n),
            ReconToken::BigInt(ref n) => ReconToken::BigInt(n.clone()),
            ReconToken::BigUint(ref n) => ReconToken::BigUint(n.clone()),
            ReconToken::Float64Literal(x) => ReconToken::Float64Literal(x),
            ReconToken::BoolLiteral(p) => ReconToken::BoolLiteral(p),
        }
    }
}

impl<S: TokenStr> ReconToken<S> {
    /// True if, and only if, the token constitutes a ['Value'] in itself.
    fn is_value(&self) -> bool {
        match self {
            ReconToken::Identifier(_)
            | ReconToken::StringLiteral(_)
            | ReconToken::Int32Literal(_)
            | ReconToken::Int64Literal(_)
            | ReconToken::UInt32Literal(_)
            | ReconToken::UInt64Literal(_)
            | ReconToken::Float64Literal(_)
            | ReconToken::BigInt(_)
            | ReconToken::BigUint(_)
            | ReconToken::BoolLiteral(_) => true,
            _ => false,
        }
    }

    /// Try to get a value from a single token.
    fn unwrap_value(self) -> Option<Result<Value, String>> {
        match self {
            ReconToken::Identifier(name) => Some(Ok(Value::Text(name.into()))),
            ReconToken::StringLiteral(name) => Some(unescape(name.borrow()).map(Value::Text)),
            ReconToken::Int32Literal(n) => Some(Ok(Value::Int32Value(n))),
            ReconToken::Int64Literal(n) => Some(Ok(Value::Int64Value(n))),
            ReconToken::UInt32Literal(n) => Some(Ok(Value::UInt32Value(n))),
            ReconToken::UInt64Literal(n) => Some(Ok(Value::UInt64Value(n))),
            ReconToken::Float64Literal(x) => Some(Ok(Value::Float64Value(x))),
            ReconToken::BoolLiteral(p) => Some(Ok(Value::BooleanValue(p))),
            ReconToken::BigInt(bi) => Some(Ok(Value::BigInt(bi))),
            ReconToken::BigUint(bi) => Some(Ok(Value::BigUint(bi))),
            _ => None,
        }
    }
}

/// A token along with its offset within the stream (in bytes).
#[derive(PartialEq, Debug)]
struct LocatedReconToken<S>(ReconToken<S>, Location);

fn loc<S>(token: ReconToken<S>, location: Location) -> LocatedReconToken<S> {
    LocatedReconToken(token, location)
}

/// States for the tokenization automaton.
#[derive(PartialEq, Eq, Debug)]
enum TokenParseState {
    None(Location),
    ReadingIdentifier(Location),
    ReadingStringLiteral(Location, bool),
    ReadingInteger(Location),
    ReadingMantissa(Location),
    StartExponent(Location),
    ReadingExponent(Location),
    ReadingNewLine(Location),
    Failed(Location, TokenError),
}

impl TokenParseState {
    fn location(&mut self) -> &mut Location {
        match self {
            TokenParseState::None(loc) => loc,
            TokenParseState::ReadingIdentifier(loc) => loc,
            TokenParseState::ReadingStringLiteral(loc, _) => loc,
            TokenParseState::ReadingInteger(loc) => loc,
            TokenParseState::ReadingMantissa(loc) => loc,
            TokenParseState::StartExponent(loc) => loc,
            TokenParseState::ReadingExponent(loc) => loc,
            TokenParseState::ReadingNewLine(loc) => loc,
            TokenParseState::Failed(loc, _) => loc,
        }
    }
}

#[derive(Clone, Copy, Hash, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct Location {
    /// Number of characters from the start. (Absolute offset)
    offset: usize,
    /// Line number.
    line: usize,
    /// Number of characters from the start of the current line. (Relative offset)
    column: usize,
}

impl Location {
    fn new() -> Self {
        Location {
            offset: 0,
            line: 1,
            column: 1,
        }
    }
}

impl Location {
    /// Sets the absolute offset to a new value and recalculates the relative offset.
    fn update_offset(&mut self, new_offset: usize) {
        self.column = self.column + new_offset - self.offset;
        self.offset = new_offset;
    }

    /// Increments the line number and resets the relative offset.
    fn new_line(&mut self) {
        self.column = 0;
        self.line += 1;
    }
}

/// State transition function for the tokenization automaton.
fn tokenize_update<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    state: &mut TokenParseState,
    index: usize,
    current: char,
    next: Option<(usize, char)>,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    source.update(next);
    match state {
        TokenParseState::None(_) => token_start(source, state, index, current, next),
        TokenParseState::ReadingIdentifier(location) => match next {
            Some((_, c)) if is_identifier_char(c) => None,
            _ => {
                let location = *location;
                let tok = extract_identifier(source, next.map(|p| p.0), location);
                *state = TokenParseState::None(location);
                tok
            }
        },
        TokenParseState::ReadingStringLiteral(location, escape) => {
            if current == '\"' {
                if *escape {
                    *escape = false;
                    None
                } else {
                    let location = *location;
                    let start = location.offset;
                    let token = ReconToken::StringLiteral(source.take(start, index));
                    *state = TokenParseState::None(location);
                    Some(Result::Ok(loc(token, location)))
                }
            } else {
                match next {
                    Some(_) => {
                        if current == '\\' && !*escape {
                            *escape = true;
                            None
                        } else {
                            *escape = false;
                            None
                        }
                    }
                    _ => {
                        let location = *location;
                        *state = TokenParseState::Failed(location, TokenError::NoClosingQuote);
                        Some(Result::Err(BadToken(location, TokenError::NoClosingQuote)))
                    }
                }
            }
        }
        TokenParseState::ReadingInteger(location) => match next {
            Some((_, c)) if is_numeric_char(c) => {
                if current == '.' {
                    *state = TokenParseState::ReadingMantissa(*location)
                } else if current == 'e' || current == 'E' {
                    *state = TokenParseState::StartExponent(*location)
                }
                None
            }
            _ => {
                let location = *location;
                let start = location.offset;
                Some(parse_int_token(
                    state,
                    location,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::ReadingMantissa(location) => match next {
            Some((_, c)) if is_mantissa_char(c) => {
                if current == 'e' || current == 'E' {
                    *state = TokenParseState::StartExponent(*location)
                }
                None
            }
            _ => {
                let location = *location;
                let start = location.offset;
                Some(parse_float_token(
                    state,
                    location,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::StartExponent(location) => match next {
            Some((_, c)) if c.is_digit(10) => {
                if current == '-' || current.is_digit(10) {
                    *state = TokenParseState::ReadingExponent(*location);
                    None
                } else {
                    let location = *location;
                    *state = TokenParseState::Failed(location, TokenError::InvalidFloat);
                    Some(Result::Err(BadToken(location, TokenError::InvalidFloat)))
                }
            }
            Some((next_off, _)) => {
                if current.is_digit(10) {
                    let location = *location;
                    let start = location.offset;
                    Some(parse_float_token(
                        state,
                        location,
                        source.take_ref(start, next_off),
                    ))
                } else {
                    Some(Err(BadToken(*location, TokenError::InvalidFloat)))
                }
            }
            _ => {
                let location = *location;
                let start = location.offset;
                Some(parse_float_token(
                    state,
                    location,
                    source.take_all_ref(start),
                ))
            }
        },
        TokenParseState::ReadingExponent(location) => match next {
            Some((_, c)) if c.is_digit(10) => {
                if current.is_digit(10) {
                    *state = TokenParseState::ReadingExponent(*location);
                    None
                } else {
                    let location = *location;
                    *state = TokenParseState::Failed(location, TokenError::InvalidFloat);
                    Some(Result::Err(BadToken(location, TokenError::InvalidFloat)))
                }
            }
            _ => {
                let location = *location;
                let start = location.offset;
                Some(parse_float_token(
                    state,
                    location,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::ReadingNewLine(location) => match next {
            Some((_, w)) if w.is_whitespace() => None,
            Some(_) => {
                let location = *location;
                *state = TokenParseState::None(location);
                Some(Ok(loc(ReconToken::NewLine, location)))
            }
            _ => {
                let location = *location;
                *state = TokenParseState::None(location);
                None
            }
        },
        TokenParseState::Failed(location, err) => Some(Err(BadToken(*location, *err))),
    }
}

/// Cut an identifier out of the source string.
fn extract_identifier<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    next_index: Option<usize>,
    location: Location,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    let content = source.take_opt(location.offset, next_index);
    let token = if content.borrow() == "true" {
        ReconToken::BoolLiteral(true)
    } else if content.borrow() == "false" {
        ReconToken::BoolLiteral(false)
    } else {
        ReconToken::Identifier(content)
    };
    Some(Result::Ok(loc(token, location)))
}

fn is_numeric_char(c: char) -> bool {
    c == '.' || is_mantissa_char(c)
}

fn is_mantissa_char(c: char) -> bool {
    c == '-' || c == 'e' || c == 'E' || c.is_digit(10)
}

fn parse_int_token<T: TokenStr>(
    state: &mut TokenParseState,
    location: Location,
    rep: &str,
) -> Result<LocatedReconToken<T>, BadToken> {
    match rep.parse::<u64>() {
        Ok(n) => {
            *state = TokenParseState::None(location);
            Ok(loc(
                match u32::try_from(n) {
                    Ok(m) => ReconToken::UInt32Literal(m),
                    Err(_) => ReconToken::UInt64Literal(n),
                },
                location,
            ))
        }
        Err(_) => match rep.parse::<i64>() {
            Ok(m) => {
                *state = TokenParseState::None(location);
                Ok(loc(
                    match i32::try_from(m) {
                        Ok(m) => ReconToken::Int32Literal(m),
                        Err(_) => ReconToken::Int64Literal(m),
                    },
                    location,
                ))
            }
            Err(_) => match BigUint::from_str(rep) {
                Ok(n) => {
                    *state = TokenParseState::None(location);
                    Ok(loc(ReconToken::BigUint(n), location))
                }
                Err(_) => match BigInt::from_str(rep) {
                    Ok(n) => {
                        *state = TokenParseState::None(location);
                        Ok(loc(ReconToken::BigInt(n), location))
                    }
                    Err(_) => {
                        *state = TokenParseState::Failed(location, TokenError::InvalidInteger);
                        Err(BadToken(location, TokenError::InvalidInteger))
                    }
                },
            },
        },
    }
}

fn parse_float_token<T: TokenStr>(
    state: &mut TokenParseState,
    location: Location,
    rep: &str,
) -> Result<LocatedReconToken<T>, BadToken> {
    match rep.parse::<f64>() {
        Ok(x) => {
            *state = TokenParseState::None(location);
            Ok(loc(ReconToken::Float64Literal(x), location))
        }
        Err(_) => {
            *state = TokenParseState::Failed(location, TokenError::InvalidFloat);
            Err(BadToken(location, TokenError::InvalidFloat))
        }
    }
}

/// Select the initial state for the tokenization automaton, returning a token immediately
/// where appropriate.
fn token_start<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    state: &mut TokenParseState,
    index: usize,
    current: char,
    next: Option<(usize, char)>,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    let location = state.location();
    location.update_offset(index);

    match current {
        '@' => Some(Result::Ok(loc(ReconToken::AttrMarker, *location))),
        '(' => Some(Result::Ok(loc(ReconToken::AttrBodyStart, *location))),
        ')' => Some(Result::Ok(loc(ReconToken::AttrBodyEnd, *location))),
        '{' => Some(Result::Ok(loc(ReconToken::RecordBodyStart, *location))),
        '}' => Some(Result::Ok(loc(ReconToken::RecordBodyEnd, *location))),
        ':' => Some(Result::Ok(loc(ReconToken::SlotDivider, *location))),
        ',' | ';' => Some(Result::Ok(loc(ReconToken::EntrySep, *location))),
        '\r' | '\n' => match next {
            Some((next_index, c)) if c == '\n' => {
                location.update_offset(next_index);
                location.new_line();
                *state = TokenParseState::ReadingNewLine(*location);
                None
            }
            Some((_, c)) if c.is_whitespace() => {
                location.new_line();
                *state = TokenParseState::ReadingNewLine(*location);
                None
            }
            _ => {
                location.new_line();
                Some(Result::Ok(loc(ReconToken::NewLine, *location)))
            }
        },
        w if w.is_whitespace() => None,
        '\"' => match next {
            Some((next_index, _)) => {
                location.update_offset(next_index);
                source.mark(false);
                *state = TokenParseState::ReadingStringLiteral(*location, false);
                None
            }
            _ => {
                let location = *location;
                *state = TokenParseState::Failed(location, TokenError::NoClosingQuote);
                Some(Err(BadToken(location, TokenError::NoClosingQuote)))
            }
        },
        c if is_identifier_start(c) => match next {
            Some((_, c)) if is_identifier_char(c) => {
                source.mark(true);
                *state = TokenParseState::ReadingIdentifier(*location);
                None
            }
            Some((next_off, _)) => Some(Ok(loc(
                ReconToken::Identifier(source.take(index, next_off)),
                *location,
            ))),
            _ => Some(Ok(loc(
                ReconToken::Identifier(source.take_all(index)),
                *location,
            ))),
        },
        '-' => match next {
            Some((_, c)) if c == '.' || c.is_digit(10) => {
                source.mark(true);
                *state = TokenParseState::ReadingInteger(*location);
                None
            }
            _ => {
                let location = *location;
                *state = TokenParseState::Failed(location, TokenError::InvalidInteger);
                Some(Result::Err(BadToken(location, TokenError::InvalidInteger)))
            }
        },
        c if c.is_digit(10) => match next {
            Some((_, c)) if is_numeric_char(c) => {
                source.mark(true);
                *state = TokenParseState::ReadingInteger(*location);
                None
            }
            _ => Some(Result::Ok(loc(
                ReconToken::UInt32Literal(c.to_digit(10).unwrap() as u32),
                *location,
            ))),
        },
        '.' => match next {
            Some((_, c)) if c.is_digit(10) => {
                source.mark(true);
                *state = TokenParseState::ReadingMantissa(*location);
                None
            }
            _ => {
                let location = *location;
                *state = TokenParseState::Failed(location, TokenError::InvalidFloat);
                Some(Result::Err(BadToken(location, TokenError::InvalidFloat)))
            }
        },
        _ => {
            let location = *location;
            *state = TokenParseState::Failed(location, TokenError::BadStartChar);
            Some(Result::Err(BadToken(location, TokenError::BadStartChar)))
        }
    }
}

/// Attempt to construct the final token at the end of the stream.
fn final_token<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    state: &mut TokenParseState,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    match state {
        TokenParseState::ReadingIdentifier(location) => extract_identifier(source, None, *location),
        TokenParseState::ReadingInteger(location) => {
            let location = *location;
            let start = location.offset;
            Some(parse_int_token(state, location, source.take_all_ref(start)))
        }
        TokenParseState::ReadingMantissa(location) => {
            let location = *location;
            let start = location.offset;
            Some(parse_float_token(
                state,
                location,
                source.take_all_ref(start),
            ))
        }
        TokenParseState::StartExponent(location) => {
            Some(Err(BadToken(*location, TokenError::InvalidFloat)))
        }
        TokenParseState::ReadingExponent(location) => {
            let location = *location;
            let start = location.offset;
            Some(parse_float_token(
                state,
                location,
                source.take_all_ref(start),
            ))
        }
        TokenParseState::ReadingNewLine(location) => {
            Some(Ok(LocatedReconToken(ReconToken::NewLine, *location)))
        }
        TokenParseState::Failed(location, err) => Some(Err(BadToken(*location, *err))),
        _ => None,
    }
}

/// Tokenize a string held entirely in memory.
fn tokenize_str<'a>(
    repr: &'a str,
) -> impl Iterator<Item = Result<LocatedReconToken<&'a str>, BadToken>> + 'a {
    let following = repr
        .char_indices()
        .skip(1)
        .map(Some)
        .chain(iter::once(None));

    let mut token_buffer = InMemoryInput::new(repr);

    repr.char_indices()
        .zip(following)
        .scan(
            TokenParseState::None(Location::new()),
            move |parse_state, ((i, current), next)| {
                let current_token =
                    tokenize_update(&mut token_buffer, parse_state, i, current, next);
                match (&current_token, next) {
                    (Some(_), _) => Some(current_token),
                    (None, None) => Some(final_token(&mut token_buffer, parse_state)),
                    _ => Some(None),
                }
            },
        )
        .flatten()
}

/// Create an iteratee that tokenizes unicode characters.
fn tokenize_iteratee(
) -> impl Iteratee<(usize, char), Item = Result<LocatedReconToken<String>, BadToken>> {
    let char_look_ahead = look_ahead::<(usize, char)>();
    let tokenize = unfold_with_flush(
        (
            false,
            TokenAccumulator::new(),
            TokenParseState::None(Location::new()),
        ),
        |state, item: ((usize, char), Option<(usize, char)>)| {
            let (is_init, token_buffer, parse_state) = state;
            let ((i, current), next) = item;
            if !*is_init {
                token_buffer.update(Some(item.0));
                *is_init = true;
            }
            tokenize_update(token_buffer, parse_state, i, current, next)
        },
        |state| {
            let (_, mut token_buffer, mut parse_state) = state;
            final_token(&mut token_buffer, &mut parse_state)
        },
    );
    char_look_ahead.and_then(tokenize)
}

/// States for the parser automaton.
#[derive(Clone, Debug, PartialEq)]
enum ValueParseState {
    AttributeStart,
    ReadingAttribute(String),
    AfterAttribute,
    RecordStart(bool),
    InsideBody(bool, bool),
    Single(bool, Value),
    ReadingSlot(bool, Value),
    AfterSlot(bool),
}

fn make_singleton(attrs: Vec<Attr>, value: Value) -> Value {
    Value::Record(attrs, vec![Item::ValueItem(value)])
}

/// States for the automaton to unescape a Java escaped string.
enum EscapeState {
    None,
    Escape,
    UnicodeEscape0,
    UnicodeEscape1(u32),
    UnicodeEscape2(u32, u32),
    UnicodeEscape3(u32, u32, u32),
    Failed,
}

fn is_escape(c: char) -> bool {
    c == '\\' || c == '\"' || c == 'b' || c == 'f' || c == 'n' || c == 'r' || c == 't'
}

/// Add a value to the frame on top of the parser stack. If the stack is empty this value is
/// instead returned.
fn push_down(
    state: &mut Vec<Frame>,
    value: Value,
    location: Location,
) -> Option<Result<Value, BadRecord>> {
    if let Some(Frame {
        attrs,
        mut items,
        mut parse_state,
    }) = state.pop()
    {
        match parse_state {
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) => {
                parse_state = ValueParseState::Single(p, value);
                state.push(Frame {
                    attrs,
                    items,
                    parse_state,
                });
                None
            }
            ValueParseState::ReadingSlot(p, key) => {
                items.push(Item::Slot(key, value));
                parse_state = ValueParseState::AfterSlot(p);
                state.push(Frame {
                    attrs,
                    items,
                    parse_state,
                });
                None
            }
            ValueParseState::ReadingAttribute(name) => {
                pack_attribute_body(state, value, attrs, items, name)
            }
            _ => Some(Err(BadRecord(location, RecordError::BadStack))),
        }
    } else {
        Some(Ok(value))
    }
}

/// Add a value to the frame on top of the parser stack the pop that frame, create a record from
/// it and push that record down to the new top of the stack.
fn push_down_and_close(
    state: &mut Vec<Frame>,
    value: Value,
    is_attr: bool,
    location: Location,
) -> Option<Result<Value, BadRecord>> {
    if let Some(Frame {
        attrs,
        mut items,
        parse_state,
    }) = state.pop()
    {
        match parse_state {
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) if p == is_attr => {
                items.push(Item::ValueItem(value));
                let record = Value::Record(attrs, items);
                push_down(state, record, location)
            }
            ValueParseState::ReadingSlot(p, key) if p == is_attr => {
                items.push(Item::Slot(key, value));
                let record = Value::Record(attrs, items);
                push_down(state, record, location)
            }
            ValueParseState::ReadingAttribute(name) if is_attr => {
                pack_attribute_body(state, value, attrs, items, name)
            }
            _ => Some(Err(BadRecord(location, RecordError::BadStackOnClose))),
        }
    } else if is_attr {
        Some(Err(BadRecord(location, RecordError::EmptyStackOnClose)))
    } else {
        Some(Ok(value))
    }
}

/// Add a value to the frame on top of the parser stack, after consuming an item separator or
/// new line.
fn push_down_item(
    state: &mut Vec<Frame>,
    value: Value,
    location: Location,
    closed_with_newline: bool,
) -> Option<Result<Value, BadRecord>> {
    if let Some(Frame {
        attrs,
        mut items,
        mut parse_state,
    }) = state.pop()
    {
        let p = match parse_state {
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) => {
                items.push(Item::ValueItem(value));
                p
            }
            ValueParseState::ReadingSlot(p, key) => {
                items.push(Item::Slot(key, value));
                p
            }
            _ => {
                return Some(Err(BadRecord(location, RecordError::BadStack)));
            }
        };
        parse_state = ValueParseState::InsideBody(p, !closed_with_newline);
        state.push(Frame {
            attrs,
            items,
            parse_state,
        });
        None
    } else {
        Some(Err(BadRecord(location, RecordError::BadStack)))
    }
}

/// Pack a ['Value'] into an attribute body then add that attribute to the frame on top of the
/// stack.
fn pack_attribute_body(
    state: &mut Vec<Frame>,
    value: Value,
    mut attrs: Vec<Attr>,
    items: Vec<Item>,
    name: String,
) -> Option<Result<Value, BadRecord>> {
    let attr = match value {
        Value::Record(rec_attrs, rec_items) if rec_attrs.is_empty() && rec_items.is_empty() => {
            Attr {
                name,
                value: Value::Extant,
            }
        }
        Value::Record(rec_attrs, mut rec_items) if rec_attrs.is_empty() && rec_items.len() == 1 => {
            match rec_items.pop() {
                Some(Item::ValueItem(v)) => Attr { name, value: v },
                Some(slot) => Attr {
                    name,
                    value: Value::singleton(slot),
                },
                _ => Attr {
                    name,
                    value: Value::Extant,
                },
            }
        }
        ow => Attr { name, value: ow },
    };
    attrs.push(attr);
    state.push(Frame {
        attrs,
        items,
        parse_state: ValueParseState::AfterAttribute,
    });
    None
}

/// The initial state when starting to parse a record.
enum StartAt {
    /// Reading the attributes at the start of the record.
    Attrs,
    /// Reading the body of an attribute.
    AttrBody,
    /// Readin the body of a normal record.
    RecordBody,
}

/// A partially constructed record. The state of the parser is a stack of these.
#[derive(Debug)]
struct Frame {
    attrs: Vec<Attr>,
    items: Vec<Item>,
    parse_state: ValueParseState,
}

impl Frame {
    fn new_record(start: StartAt) -> Frame {
        Frame {
            attrs: vec![],
            items: vec![],
            parse_state: match start {
                StartAt::Attrs => ValueParseState::AttributeStart,
                StartAt::AttrBody => ValueParseState::RecordStart(true),
                StartAt::RecordBody => ValueParseState::RecordStart(false),
            },
        }
    }
}

/// An operation to be applied to the parser state after consuming a token.
enum StateModification {
    Fail(RecordError),
    RePush(Frame),
    OpenNew(Frame, StartAt),
    PushDown(Value),
    PushDownItem(Value, bool),
    PushDownAndClose(Value, bool),
}

impl StateModification {
    /// Apply the modification to the parser state stack.
    fn apply(self, state: &mut Vec<Frame>, location: Location) -> Option<Result<Value, BadRecord>> {
        match self {
            StateModification::Fail(err) => Some(Err(BadRecord(location, err))),
            StateModification::RePush(frame) => {
                state.push(frame);
                None
            }
            StateModification::OpenNew(frame, start) => {
                state.push(frame);
                match start {
                    StartAt::Attrs => {}
                    StartAt::AttrBody => {}
                    StartAt::RecordBody => {}
                }
                state.push(Frame::new_record(start));
                None
            }
            StateModification::PushDown(value) => push_down(state, value, location),
            StateModification::PushDownItem(value, closed_with_newline) => {
                push_down_item(state, value, location, closed_with_newline)
            }
            StateModification::PushDownAndClose(value, attr_body) => {
                push_down_and_close(state, value, attr_body, location)
            }
        }
    }
}

/// Push an entry back onto the stack.
fn repush(attrs: Vec<Attr>, items: Vec<Item>, parse_state: ValueParseState) -> StateModification {
    StateModification::RePush(Frame {
        attrs,
        items,
        parse_state,
    })
}

fn open_new(
    attrs: Vec<Attr>,
    items: Vec<Item>,
    parse_state: ValueParseState,
    start_at: StartAt,
) -> StateModification {
    StateModification::OpenNew(
        Frame {
            attrs,
            items,
            parse_state,
        },
        start_at,
    )
}

/// The state transition function for the parser automaton.
fn consume_token<S: TokenStr>(
    state: &mut Vec<Frame>,
    loc_token: LocatedReconToken<S>,
) -> Option<ParseTermination> {
    use ValueParseState::*;

    let LocatedReconToken(token, location) = loc_token;

    if let Some(Frame {
        attrs,
        items,
        parse_state,
    }) = state.pop()
    {
        let state_mod: StateModification = match parse_state {
            AttributeStart => update_attr_start(token, attrs, items),
            ReadingAttribute(name) => update_reading_attr(token, name, attrs, items),
            AfterAttribute => update_after_attr(token, attrs, items),
            RecordStart(attr_body) => update_record_start(token, attr_body, attrs, items),
            InsideBody(attr_body, required) => {
                update_inside_body(token, attr_body, required, attrs, items)
            }
            Single(attr_body, value) => update_single(token, attr_body, value, attrs, items),
            ReadingSlot(attr_body, key) => update_reading_slot(token, attr_body, key, attrs, items),
            AfterSlot(attr_body) => update_after_slot(token, attr_body, attrs, items),
        };
        match state_mod.apply(state, location) {
            Some(Ok(value)) => Some(ParseTermination::EarlyTermination(value)),
            Some(Err(failed)) => Some(ParseTermination::Failed(ParseFailure::InvalidToken(failed))),
            _ => None,
        }
    } else {
        use ReconToken::*;
        match token {
            AttrMarker => {
                state.push(Frame::new_record(StartAt::Attrs));
                None
            }
            RecordBodyStart => {
                state.push(Frame::new_record(StartAt::RecordBody));
                None
            }
            NewLine => None,
            tok if tok.is_value() => match tok.unwrap_value() {
                Some(Ok(value)) => Some(ParseTermination::EarlyTermination(value)),
                Some(Err(_)) => Some(ParseTermination::Failed(ParseFailure::InvalidToken(
                    BadRecord(location, RecordError::InvalidValue),
                ))),
                _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(
                    BadRecord(location, RecordError::NonValueToken),
                ))),
            },
            _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(
                BadRecord(location, RecordError::BadValueStart),
            ))),
        }
    }
}

fn update_attr_start<S: TokenStr>(
    token: ReconToken<S>,
    attrs: Vec<Attr>,
    items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        Identifier(name) => repush(attrs, items, ReadingAttribute(name.into())),
        StringLiteral(name) => match unescape(name.borrow()) {
            Ok(unesc) => repush(attrs, items, ReadingAttribute(unesc)),
            Err(_) => StateModification::Fail(RecordError::InvalidStringLiteral),
        },
        _ => StateModification::Fail(RecordError::InvalidAttributeName),
    }
}

fn update_reading_attr<S: TokenStr>(
    token: ReconToken<S>,
    name: String,
    mut attrs: Vec<Attr>,
    items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        AttrMarker => {
            attrs.push(Attr {
                name,
                value: Value::Extant,
            });
            repush(attrs, items, AttributeStart)
        }
        AttrBodyStart => open_new(attrs, items, ReadingAttribute(name), StartAt::AttrBody),
        RecordBodyStart => {
            attrs.push(Attr {
                name,
                value: Value::Extant,
            });
            repush(attrs, items, RecordStart(false))
        }
        tok if tok.is_value() => {
            let attr = Attr {
                name,
                value: Value::Extant,
            };
            attrs.push(attr);
            match tok.unwrap_value() {
                Some(Ok(value)) => {
                    let record = make_singleton(attrs, value);
                    StateModification::PushDown(record)
                }
                Some(Err(_)) => StateModification::Fail(RecordError::InvalidValue),
                _ => StateModification::Fail(RecordError::NonValueToken),
            }
        }
        tok @ EntrySep | tok @ NewLine => {
            let attr = Attr {
                name,
                value: Value::Extant,
            };
            attrs.push(attr);
            let record = Value::Record(attrs, items);
            StateModification::PushDownItem(record, tok == NewLine)
        }
        tok @ AttrBodyEnd | tok @ RecordBodyEnd => {
            let attr = Attr {
                name,
                value: Value::Extant,
            };
            attrs.push(attr);
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, tok == AttrBodyEnd)
        }
        _ => StateModification::Fail(RecordError::InvalidAttributeValue),
    }
}

fn update_after_attr<S: TokenStr>(
    token: ReconToken<S>,
    attrs: Vec<Attr>,
    items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        AttrMarker => repush(attrs, items, AttributeStart),
        RecordBodyStart => repush(attrs, items, RecordStart(false)),
        tok if tok.is_value() => match tok.unwrap_value() {
            Some(Ok(value)) => {
                let record = make_singleton(attrs, value);
                StateModification::PushDown(record)
            }
            Some(Err(_)) => StateModification::Fail(RecordError::InvalidValue),
            _ => StateModification::Fail(RecordError::NonValueToken),
        },
        tok @ EntrySep | tok @ NewLine => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownItem(record, tok == NewLine)
        }
        tok @ AttrBodyEnd | tok @ RecordBodyEnd => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, tok == AttrBodyEnd)
        }
        _ => StateModification::Fail(RecordError::BadRecordStart),
    }
}

fn update_record_start<S: TokenStr>(
    token: ReconToken<S>,
    attr_body: bool,
    attrs: Vec<Attr>,
    mut items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        AttrBodyEnd if attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        }
        RecordBodyStart => open_new(attrs, items, RecordStart(attr_body), StartAt::RecordBody),
        AttrMarker => open_new(attrs, items, RecordStart(attr_body), StartAt::Attrs),
        tok if tok.is_value() => match tok.unwrap_value() {
            Some(Ok(value)) => repush(attrs, items, Single(attr_body, value)),
            Some(Err(_)) => StateModification::Fail(RecordError::InvalidValue),
            _ => StateModification::Fail(RecordError::NonValueToken),
        },
        SlotDivider => repush(attrs, items, ReadingSlot(attr_body, Value::Extant)),
        EntrySep => {
            items.push(Item::of(Value::Extant));
            repush(attrs, items, InsideBody(attr_body, true))
        }
        NewLine => repush(attrs, items, RecordStart(attr_body)),
        _ => StateModification::Fail(RecordError::BadRecordStart),
    }
}

fn update_inside_body<S: TokenStr>(
    token: ReconToken<S>,
    attr_body: bool,
    required: bool,
    attrs: Vec<Attr>,
    mut items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            if required {
                items.push(Item::ValueItem(Value::Extant));
            }
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        AttrBodyEnd if attr_body => {
            if required {
                items.push(Item::ValueItem(Value::Extant));
            }
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        }
        RecordBodyStart => open_new(
            attrs,
            items,
            InsideBody(attr_body, required),
            StartAt::RecordBody,
        ),
        AttrMarker => open_new(
            attrs,
            items,
            InsideBody(attr_body, required),
            StartAt::Attrs,
        ),
        tok if tok.is_value() => match tok.unwrap_value() {
            Some(Ok(value)) => repush(attrs, items, Single(attr_body, value)),
            Some(Err(_)) => StateModification::Fail(RecordError::InvalidValue),
            _ => StateModification::Fail(RecordError::NonValueToken),
        },
        SlotDivider => repush(attrs, items, ReadingSlot(attr_body, Value::Extant)),
        EntrySep => {
            items.push(Item::of(Value::Extant));
            repush(attrs, items, InsideBody(attr_body, true))
        }
        NewLine => repush(attrs, items, InsideBody(attr_body, required)),
        _ => StateModification::Fail(RecordError::InvalidItemStart),
    }
}

fn update_single<S: TokenStr>(
    token: ReconToken<S>,
    attr_body: bool,
    value: Value,
    attrs: Vec<Attr>,
    mut items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            items.push(Item::ValueItem(value));
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        AttrBodyEnd if attr_body => {
            items.push(Item::ValueItem(value));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        }
        tok @ EntrySep | tok @ NewLine => {
            items.push(Item::ValueItem(value));
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep))
        }
        SlotDivider => repush(attrs, items, ReadingSlot(attr_body, value)),
        _ => StateModification::Fail(RecordError::InvalidAfterItem),
    }
}

fn update_reading_slot<S: TokenStr>(
    token: ReconToken<S>,
    attr_body: bool,
    key: Value,
    attrs: Vec<Attr>,
    mut items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyStart => open_new(
            attrs,
            items,
            ReadingSlot(attr_body, key),
            StartAt::RecordBody,
        ),
        AttrMarker => open_new(attrs, items, ReadingSlot(attr_body, key), StartAt::Attrs),
        tok if tok.is_value() => match tok.unwrap_value() {
            Some(Ok(value)) => {
                items.push(Item::Slot(key, value));
                repush(attrs, items, AfterSlot(attr_body))
            }
            Some(Err(_)) => StateModification::Fail(RecordError::InvalidValue),
            _ => StateModification::Fail(RecordError::NonValueToken),
        },
        RecordBodyEnd if !attr_body => {
            items.push(Item::Slot(key, Value::Extant));
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        AttrBodyEnd if attr_body => {
            items.push(Item::Slot(key, Value::Extant));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        }
        tok @ EntrySep | tok @ NewLine => {
            items.push(Item::Slot(key, Value::Extant));
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep))
        }
        _ => StateModification::Fail(RecordError::InvalidSlotValue),
    }
}

fn update_after_slot<S: TokenStr>(
    token: ReconToken<S>,
    attr_body: bool,
    attrs: Vec<Attr>,
    items: Vec<Item>,
) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        AttrBodyEnd if attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        }
        tok @ EntrySep | tok @ NewLine => {
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep))
        }
        _ => StateModification::Fail(RecordError::InvalidAfterItem),
    }
}

pub struct IterateeDecoder<I>(Option<I>);

impl<I> IterateeDecoder<I>
where
    I: Iteratee<char>,
{
    pub fn new(iteratee: I) -> IterateeDecoder<I> {
        IterateeDecoder(Some(iteratee))
    }
}

fn feed_chars<I, T, E>(iteratee: &mut I, src: &mut BytesMut, eof: bool) -> Result<Option<T>, E>
where
    I: Iteratee<char, Item = Result<T, E>>,
    E: From<io::Error> + From<Utf8Error>,
{
    let as_utf8 = match from_utf8(&*src) {
        Ok(s) => s,
        Err(utf_err) => {
            let good_to = utf_err.valid_up_to();
            if eof || src.remaining() - good_to >= 4 {
                return Err(utf_err.into());
            } else {
                unsafe { from_utf8_unchecked(&src[0..good_to]) }
            }
        }
    };
    let mut chars = as_utf8.char_indices();
    loop {
        match chars.next() {
            Some((off, c)) => match iteratee.feed(c) {
                Some(Ok(result)) => {
                    src.advance(off + c.len_utf8());
                    break Ok(Some(result));
                }
                Some(Err(e)) => {
                    break Err(e);
                }
                _ => {
                    continue;
                }
            },
            _ => {
                src.advance(src.remaining());
                break Ok(None);
            }
        }
    }
}

impl<I, T, E> tokio_util::codec::Decoder for IterateeDecoder<I>
where
    I: Iteratee<char, Item = Result<T, E>>,
    E: From<io::Error> + From<Utf8Error>,
{
    type Item = T;
    type Error = E;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match &mut self.0 {
            Some(iteratee) => feed_chars(iteratee, src, false),
            _ => Ok(None),
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.0.take() {
            Some(mut iteratee) => {
                if buf.is_empty() {
                    match iteratee.flush() {
                        Some(Ok(result)) => Ok(Some(result)),
                        Some(Err(e)) => Err(e),
                        _ => Ok(None),
                    }
                } else {
                    feed_chars(&mut iteratee, buf, true)
                }
            }
            _ => Ok(None),
        }
    }
}
