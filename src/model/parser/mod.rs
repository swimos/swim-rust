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

use super::*;
use std::borrow::Borrow;

#[cfg(test)]
mod tests;

mod token_buffer;

use crate::iteratee::*;
use token_buffer::*;

/// Determine if a character is valid at the start of an identifier.
///
/// #Examples
///
/// ```
/// use swim_rust::model::parser::*;
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
/// use swim_rust::model::parser::*;
///
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
/// use swim_rust::model::parser::*;
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
pub struct BadToken(usize, TokenError);

/// Parse failure indicated that an invalid sequence of tokens was encountered.
#[derive(Eq, Clone, Hash, Debug, PartialEq, Ord, PartialOrd)]
pub struct BadRecord(usize, RecordError);

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

/// Parse a stream of ['Value']s from a stream of characters. More values will be read until either
/// an error is encountered or the end of the stream is reached.
pub fn parse_all<'a>(repr: &'a str) -> impl Iterator<Item = Result<Value, ParseFailure>> + 'a {
    let tokens = tokenize_str(repr).map(|t| Some(t)).chain(iter::once(None));

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
                    _ => handle_tok_stream_end(state)
                };
                Some(current)
            } else {
                None
            }
        })
        .flatten()
}

///Creates a final ['Value'] from the contents of the stack where possible.
fn handle_tok_stream_end(state: &mut Vec<Frame>) -> Option<Result<Value, ParseFailure>> {
    let depth = state.len();
    match state.pop() {
        Some(Frame {
                 mut attrs,
                 items,
                 parse_state,
             }) if depth == 1 => match parse_state {
            ValueParseState::ReadingAttribute(name) => {
                attrs.push(Attr {
                    name: name.to_owned(),
                    value: Value::Extant,
                });
                Some(Ok(Value::Record(attrs, items)))
            }
            ValueParseState::AfterAttribute => {
                Some(Ok(Value::Record(attrs, items)))
            }
            _ => Some(Err(ParseFailure::IncompleteRecord)),
        },
        _ => None,
    }
}

/// Iteratee converting Recon tokens into Recon values.
fn from_tokens_iteratee() -> impl Iteratee<LocatedReconToken<String>, Item = Result<Value, ParseFailure>> {
    unfold_with_flush(vec![], |state: &mut Vec<Frame>, loc_token: LocatedReconToken<String>| {
        consume_token(state, loc_token).map(|res| {
            match res {
                ParseTermination::EarlyTermination(value) => Ok(value),
                ParseTermination::Failed(failure) => Err(failure),
            }
        })
    }, |mut state: Vec<Frame>| {
        handle_tok_stream_end(&mut state)
    })
}

/// Iteratee that parses a stream of UTF characters into Recon ['Value']s.
pub fn parse_iteratee() -> impl Iteratee<(usize, char), Item = Result<Value, ParseFailure>> {
    tokenize_iteratee()
        .and_then_fallible(from_tokens_iteratee())
        .fuse_on_error()
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

trait TokenStr: PartialEq + Borrow<str> + Into<String> {}

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
    Float64Literal(f64),
    BoolLiteral(bool),
}

impl<S: TokenStr> ReconToken<S> {
    /// True iff the token constitutes a ['Value'] in itself.
    fn is_value(&self) -> bool {
        match self {
            ReconToken::Identifier(_)
            | ReconToken::StringLiteral(_)
            | ReconToken::Int32Literal(_)
            | ReconToken::Int64Literal(_)
            | ReconToken::Float64Literal(_)
            | ReconToken::BoolLiteral(_) => true,
            _ => false,
        }
    }

    /// Try to get a value from a single token.
    fn unwrap_value(self) -> Option<Result<Value, String>> {
        match self {
            ReconToken::Identifier(name) => Some(Ok(Value::Text(name.into()))),
            ReconToken::StringLiteral(name) => {
                Some(unescape(name.borrow()).map(|unesc| Value::Text(unesc)))
            }
            ReconToken::Int32Literal(n) => Some(Ok(Value::Int32Value(n))),
            ReconToken::Int64Literal(n) => Some(Ok(Value::Int64Value(n))),
            ReconToken::Float64Literal(x) => Some(Ok(Value::Float64Value(x))),
            ReconToken::BoolLiteral(p) => Some(Ok(Value::BooleanValue(p))),
            _ => None,
        }
    }
}

/// A token along with its offset within the stream (in bytes).
#[derive(PartialEq, Debug)]
struct LocatedReconToken<S>(ReconToken<S>, usize);

fn loc<S>(token: ReconToken<S>, offset: usize) -> LocatedReconToken<S> {
    LocatedReconToken(token, offset)
}

/// States for the tokenization automaton.
#[derive(PartialEq, Eq, Debug)]
enum TokenParseState {
    None,
    ReadingIdentifier(usize),
    ReadingStringLiteral(usize, bool),
    ReadingInteger(usize),
    ReadingMantissa(usize),
    StartExponent(usize),
    ReadingExponent(usize),
    ReadingNewLine(usize),
    Failed(usize, TokenError),
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
        TokenParseState::None => token_start(source, state, index, current, next),
        TokenParseState::ReadingIdentifier(off) => match next {
            Some((_, c)) if is_identifier_char(c) => None,
            _ => {
                let tok = extract_identifier(source, next.map(|p| p.0), *off);
                *state = TokenParseState::None;
                tok
            }
        },
        TokenParseState::ReadingStringLiteral(off, escape) => {
            if current == '\"' {
                if *escape {
                    *escape = false;
                    None
                } else {
                    let start = *off;
                    let token = ReconToken::StringLiteral(source.take(start, index));
                    *state = TokenParseState::None;
                    Some(Result::Ok(loc(token, start)))
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
                        *state = TokenParseState::Failed(index, TokenError::NoClosingQuote);
                        Some(Result::Err(BadToken(index, TokenError::NoClosingQuote)))
                    }
                }
            }
        }
        TokenParseState::ReadingInteger(off) => match next {
            Some((_, c)) if is_numeric_char(c) => {
                if current == '.' {
                    *state = TokenParseState::ReadingMantissa(*off)
                } else if current == 'e' || current == 'E' {
                    *state = TokenParseState::StartExponent(*off)
                }
                None
            }
            _ => {
                let start = *off;
                Some(parse_int_token(
                    state,
                    start,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::ReadingMantissa(off) => match next {
            Some((_, c)) if is_mantissa_char(c) => {
                if current == 'e' || current == 'E' {
                    *state = TokenParseState::StartExponent(*off)
                }
                None
            }
            _ => {
                let start = *off;
                Some(parse_float_token(
                    state,
                    start,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::StartExponent(off) => match next {
            Some((_, c)) if c.is_digit(10) => {
                if current == '-' || current.is_digit(10) {
                    *state = TokenParseState::ReadingExponent(*off);
                    None
                } else {
                    let start = *off;
                    *state = TokenParseState::Failed(start, TokenError::InvalidFloat);
                    Some(Result::Err(BadToken(start, TokenError::InvalidFloat)))
                }
            }
            Some((next_off, _)) => {
                if current.is_digit(10) {
                    let start = *off;
                    Some(parse_float_token(
                        state,
                        start,
                        source.take_ref(start, next_off),
                    ))
                } else {
                    Some(Err(BadToken(*off, TokenError::InvalidFloat)))
                }
            }
            _ => {
                let start = *off;
                Some(parse_float_token(state, start, source.take_all_ref(start)))
            }
        },
        TokenParseState::ReadingExponent(off) => match next {
            Some((_, c)) if c.is_digit(10) => {
                if current.is_digit(10) {
                    *state = TokenParseState::ReadingExponent(*off);
                    None
                } else {
                    let start = *off;
                    *state = TokenParseState::Failed(start, TokenError::InvalidFloat);
                    Some(Result::Err(BadToken(start, TokenError::InvalidFloat)))
                }
            }
            _ => {
                let start = *off;
                Some(parse_float_token(
                    state,
                    start,
                    source.take_opt_ref(start, next.map(|p| p.0)),
                ))
            }
        },
        TokenParseState::ReadingNewLine(off) => match next {
            Some((_, w)) if w.is_whitespace() => None,
            Some(_) => {
                let start = *off;
                *state = TokenParseState::None;
                Some(Ok(loc(ReconToken::NewLine, start)))
            }
            _ => {
                *state = TokenParseState::None;
                None
            }
        },
        TokenParseState::Failed(i, err) => Some(Err(BadToken(*i, *err))),
    }
}

/// Cut an identifier out of the source string.
fn extract_identifier<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    next_index: Option<usize>,
    start: usize,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    let content = source.take_opt(start, next_index);
    let token = if content.borrow() == "true" {
        ReconToken::BoolLiteral(true)
    } else if content.borrow() == "false" {
        ReconToken::BoolLiteral(false)
    } else {
        ReconToken::Identifier(content)
    };
    Some(Result::Ok(loc(token, start)))
}

fn is_numeric_char(c: char) -> bool {
    c == '.' || is_mantissa_char(c)
}

fn is_mantissa_char(c: char) -> bool {
    c == '-' || c == 'e' || c == 'E' || c.is_digit(10)
}

fn parse_int_token<T: TokenStr>(
    state: &mut TokenParseState,
    offset: usize,
    rep: &str,
) -> Result<LocatedReconToken<T>, BadToken> {
    match rep.parse::<i64>() {
        Ok(n) => {
            *state = TokenParseState::None;
            Ok(loc(
                match i32::try_from(n) {
                    Ok(m) => ReconToken::Int32Literal(m),
                    Err(_) => ReconToken::Int64Literal(n),
                },
                offset,
            ))
        }
        Err(_) => {
            *state = TokenParseState::Failed(offset, TokenError::InvalidInteger);
            Err(BadToken(offset, TokenError::InvalidInteger))
        }
    }
}

fn parse_float_token<T: TokenStr>(
    state: &mut TokenParseState,
    offset: usize,
    rep: &str,
) -> Result<LocatedReconToken<T>, BadToken> {
    match rep.parse::<f64>() {
        Ok(x) => {
            *state = TokenParseState::None;
            Ok(loc(ReconToken::Float64Literal(x), offset))
        }
        Err(_) => {
            *state = TokenParseState::Failed(offset, TokenError::InvalidFloat);
            Err(BadToken(offset, TokenError::InvalidFloat))
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
    match current {
        '@' => Some(Result::Ok(loc(ReconToken::AttrMarker, index))),
        '(' => Some(Result::Ok(loc(ReconToken::AttrBodyStart, index))),
        ')' => Some(Result::Ok(loc(ReconToken::AttrBodyEnd, index))),
        '{' => Some(Result::Ok(loc(ReconToken::RecordBodyStart, index))),
        '}' => Some(Result::Ok(loc(ReconToken::RecordBodyEnd, index))),
        ':' => Some(Result::Ok(loc(ReconToken::SlotDivider, index))),
        ',' | ';' => Some(Result::Ok(loc(ReconToken::EntrySep, index))),
        '\r' | '\n' => match next {
            Some((_, c)) if c.is_whitespace() => {
                *state = TokenParseState::ReadingNewLine(index);
                None
            }
            _ => Some(Result::Ok(loc(ReconToken::NewLine, index))),
        },
        w if w.is_whitespace() => None,
        '\"' => match next {
            Some((next_index, _)) => {
                source.mark(false);
                *state = TokenParseState::ReadingStringLiteral(next_index, false);
                None
            }
            _ => {
                *state = TokenParseState::Failed(index, TokenError::NoClosingQuote);
                Some(Err(BadToken(index, TokenError::NoClosingQuote)))
            }
        },
        c if is_identifier_start(c) => match next {
            Some((_, c)) if is_identifier_char(c) => {
                source.mark(true);
                *state = TokenParseState::ReadingIdentifier(index);
                None
            }
            Some((next_off, _)) => Some(Ok(loc(
                ReconToken::Identifier(source.take(index, next_off)),
                index,
            ))),
            _ => Some(Ok(loc(
                ReconToken::Identifier(source.take_all(index)),
                index,
            ))),
        },
        '-' => match next {
            Some((_, c)) if c == '.' || c.is_digit(10) => {
                source.mark(true);
                *state = TokenParseState::ReadingInteger(index);
                None
            }
            _ => {
                *state = TokenParseState::Failed(index, TokenError::InvalidInteger);
                Some(Result::Err(BadToken(index, TokenError::InvalidInteger)))
            }
        },
        c if c.is_digit(10) => match next {
            Some((_, c)) if is_numeric_char(c) => {
                source.mark(true);
                *state = TokenParseState::ReadingInteger(index);
                None
            }
            _ => Some(Result::Ok(loc(
                ReconToken::Int32Literal(c.to_digit(10).unwrap() as i32),
                index,
            ))),
        },
        '.' => match next {
            Some((_, c)) if c.is_digit(10) => {
                source.mark(true);
                *state = TokenParseState::ReadingMantissa(index);
                None
            }
            _ => {
                *state = TokenParseState::Failed(index, TokenError::InvalidFloat);
                Some(Result::Err(BadToken(index, TokenError::InvalidFloat)))
            }
        },
        _ => {
            *state = TokenParseState::Failed(index, TokenError::BadStartChar);
            Some(Result::Err(BadToken(index, TokenError::BadStartChar)))
        }
    }
}

/// Attempt to construct the final token at the end of the stream.
fn final_token<T: TokenStr, B: TokenBuffer<T>>(
    source: &mut B,
    state: &mut TokenParseState,
) -> Option<Result<LocatedReconToken<T>, BadToken>> {
    match state {
        TokenParseState::ReadingIdentifier(off) => extract_identifier(source, None, *off),
        TokenParseState::ReadingInteger(off) => {
            let start = *off;
            Some(parse_int_token(state, start, source.take_all_ref(start)))
        }
        TokenParseState::ReadingMantissa(off) => {
            let start = *off;
            Some(parse_float_token(state, start, source.take_all_ref(start)))
        }
        TokenParseState::StartExponent(off) => Some(Err(BadToken(*off, TokenError::InvalidFloat))),
        TokenParseState::ReadingExponent(off) => {
            let start = *off;
            Some(parse_float_token(state, start, source.take_all_ref(start)))
        }
        TokenParseState::ReadingNewLine(off) => {
            Some(Ok(LocatedReconToken(ReconToken::NewLine, *off)))
        }
        TokenParseState::Failed(off, err) => Some(Err(BadToken(*off, *err))),
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
        .map(|ci| Some(ci))
        .chain(iter::once(None));

    let mut token_buffer = InMemoryInput::new(repr);

    repr.char_indices()
        .zip(following)
        .scan(
            TokenParseState::None,
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
fn tokenize_iteratee() -> impl Iteratee<(usize, char), Item = Result<LocatedReconToken<String>, BadToken>>
{
    let char_look_ahead = look_ahead::<(usize, char)>();
    let tokenize = unfold_with_flush(
        (TokenAccumulator::new(), TokenParseState::None),
        |state, item: ((usize, char), Option<(usize, char)>)| {
            let (token_buffer, parse_state) = state;
            let ((i, current), next) = item;
            tokenize_update(token_buffer, parse_state, i, current, next)
        },
        |state| {
            let (mut token_buffer, mut parse_state) = state;
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
    offset: usize,
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
            _ => Some(Err(BadRecord(offset, RecordError::BadStack))),
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
    offset: usize,
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
                push_down(state, record, offset)
            }
            ValueParseState::ReadingSlot(p, key) if p == is_attr => {
                items.push(Item::Slot(key, value));
                let record = Value::Record(attrs, items);
                push_down(state, record, offset)
            }
            ValueParseState::ReadingAttribute(name) if is_attr => {
                pack_attribute_body(state, value, attrs, items, name)
            }
            _ => Some(Err(BadRecord(offset, RecordError::BadStackOnClose))),
        }
    } else {
        if is_attr {
            Some(Err(BadRecord(offset, RecordError::EmptyStackOnClose)))
        } else {
            Some(Ok(value))
        }
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
    PushDownAndClose(Value, bool),
}

impl StateModification {
    /// Apply the modification to the parser state stack.
    fn apply(self, state: &mut Vec<Frame>, offset: usize) -> Option<Result<Value, BadRecord>> {
        match self {
            StateModification::Fail(err) => Some(Err(BadRecord(offset, err))),
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
            StateModification::PushDown(value) => push_down(state, value, offset),
            StateModification::PushDownAndClose(value, attr_body) => {
                push_down_and_close(state, value, attr_body, offset)
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

    let LocatedReconToken(token, offset) = loc_token;

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
        match state_mod.apply(state, offset) {
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
                    BadRecord(offset, RecordError::InvalidValue),
                ))),
                _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(
                    BadRecord(offset, RecordError::NonValueToken),
                ))),
            },
            _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(
                BadRecord(offset, RecordError::BadValueStart),
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
                name: name.to_owned(),
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
        EntrySep => {
            let attr = Attr {
                name,
                value: Value::Extant,
            };
            attrs.push(attr);
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
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
        NewLine => repush(attrs, items, ReadingAttribute(name)),
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
        EntrySep => {
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        }
        tok @ AttrBodyEnd | tok @ RecordBodyEnd => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, tok == AttrBodyEnd)
        }
        NewLine => repush(attrs, items, AfterAttribute),
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
