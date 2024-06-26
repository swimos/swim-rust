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

mod async_parser;
mod error;
pub mod record;
#[cfg(test)]
mod tests;
mod tokens;

pub use error::ParseError;
use nom::branch::alt;
use nom::character::complete::space0;
use nom::combinator::{eof, map};
use nom::sequence::{delimited, terminated};
use nom::Finish;
use nom_locate::LocatedSpan;
use std::borrow::Cow;
use swimos_form::read::ReadError;
use swimos_form::read::ReadEvent;
use swimos_form::read::{Recognizer, RecognizerReadable};

pub use record::matcher::{extract_header, extract_header_str, HeaderPeeler, MessageExtractError};

/// Wraps a string in a structure that keeps track of the line and column
/// as the input is parsed.
pub type Span<'a> = LocatedSpan<&'a str>;

#[derive(Debug)]
enum FinalAttrStage<'a> {
    Start(Cow<'a, str>),
    EndAttr,
    StartBody,
    EndBody,
}

/// A single parse result can produce several events. This enumeration allows
/// an iterator to consume them in turn. Note that in the cases with multiple
/// events, the events are stored as a stack so are in reverse order.
#[derive(Debug, Default)]
enum ParseEvents<'a> {
    #[default]
    NoEvent,
    SingleEvent(ReadEvent<'a>),
    TwoEvents(ReadEvent<'a>, ReadEvent<'a>),
    ThreeEvents(ReadEvent<'a>, ReadEvent<'a>, ReadEvent<'a>),
    TerminateWithAttr(FinalAttrStage<'a>),
    End,
}

/// Push the events generated by parsing the input into a recognizer.
///
/// * `input` - The input to parse.
/// * `recognizer` - State machines that can recognize a type from a stream of [`ReadEvent`]s.
/// * `allow_comments` - Boolean flag indicating whether or not the parsing should fail on comments.
pub fn parse_recognize_with<R: Recognizer>(
    input: Span<'_>,
    recognizer: &mut R,
    allow_comments: bool,
) -> Result<R::Target, ParseError> {
    let mut iterator = record::ParseIterator::new(input, allow_comments);
    loop {
        if let Some(ev) = iterator.next() {
            if let Some(r) = recognizer.feed_event(ev?) {
                break r;
            }
        } else {
            break recognizer
                .try_flush()
                .unwrap_or(Err(ReadError::IncompleteRecord));
        }
    }
    .map_err(ParseError::Structure)
}

/// Create a [`Recognizer`] state machine for a type, and feed the stream of [`ReadEvent`]s,
/// produced by parsing a Recon string, into it.
///
/// # Arguments
/// * `input` - The input to parse.
/// * `allow_comments` - Boolean flag indicating whether or not the parsing should fail on comments.
pub fn parse_recognize<'a, T: RecognizerReadable>(
    input: impl Into<Span<'a>>,
    allow_comments: bool,
) -> Result<T, ParseError> {
    let mut recognizer = T::make_recognizer();
    parse_recognize_with(input.into(), &mut recognizer, allow_comments)
}

/// Attempt to parse a text token from entirety of the input (either an identifier or the content of
/// a string literal).
///
/// # Arguments
/// * `input` - The input to parse.
pub fn parse_text_token(input: Span<'_>) -> Result<Cow<'_, str>, ParseError> {
    let mut text_parser = terminated(
        delimited(
            space0,
            alt((
                map(tokens::complete::identifier, Cow::Borrowed),
                tokens::string_literal,
            )),
            space0,
        ),
        eof,
    );
    let (_, text) = text_parser(input).finish()?;
    Ok(text)
}

pub use async_parser::{parse_recon_document, AsyncParseError, RecognizerDecoder};
