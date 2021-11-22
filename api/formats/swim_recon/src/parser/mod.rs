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

#[cfg(feature = "async_parser")]
mod async_parser;
mod error;
mod record;
#[cfg(test)]
mod tests;
mod tokens;

pub use crate::parser::error::ParseError;
use nom_locate::LocatedSpan;
use std::borrow::Cow;
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_form::structural::read::ReadError;
use swim_model::Value;

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
#[derive(Debug)]
enum ParseEvents<'a> {
    NoEvent,
    SingleEvent(ReadEvent<'a>),
    TwoEvents(ReadEvent<'a>, ReadEvent<'a>),
    ThreeEvents(ReadEvent<'a>, ReadEvent<'a>, ReadEvent<'a>),
    TerminateWithAttr(FinalAttrStage<'a>),
    End,
}

/// Create an iterator that will parse a sequence of events from a complete string.
///
/// * `input` - The input to parse.
/// * `allow_comments` - Boolean flag indicating whether or not the parsing should fail on comments.
pub fn parse_iterator(
    input: Span<'_>,
    allow_comments: bool,
) -> impl Iterator<Item = Result<ReadEvent<'_>, nom::error::Error<Span<'_>>>> + '_ {
    record::ParseIterator::new(input, allow_comments)
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
/// * `input` - The input to parse.
/// * `allow_comments` - Boolean flag indicating whether or not the parsing should fail on comments.
pub fn parse_recognize<T: RecognizerReadable>(
    input: Span<'_>,
    allow_comments: bool,
) -> Result<T, ParseError> {
    let mut recognizer = T::make_recognizer();
    parse_recognize_with(input, &mut recognizer, allow_comments)
}

/// Parse exactly one ['Value'] from the input, returning an error if the string does not contain
/// the representation of exactly one.
///
/// * `repr` - The input to parse.
/// * `allow_comments` - Boolean flag indicating whether or not the parsing should fail on comments.
pub fn parse_value(repr: &str, allow_comments: bool) -> Result<Value, ParseError> {
    parse_recognize(Span::new(repr), allow_comments)
}

#[cfg(feature = "async_parser")]
pub use async_parser::{
    parse_recognize_with as async_parse_recognize_with, parse_recon_document, AsyncParseError,
};
