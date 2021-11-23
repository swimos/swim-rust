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

#[cfg(test)]
mod tests;

use crate::parser::record::{FinalSegmentParser, IncrementalReconParser};
use crate::parser::ParseError;
use crate::parser::Span;
use bytes::{Buf, BytesMut};
use nom::Parser;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_form::structural::read::ReadError;
use swim_model::{Item, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec::Decoder;

/// Error type for reading a configuration document.
#[derive(Debug)]
pub enum AsyncParseError {
    /// An IO error occurred reading the source data.
    Io(tokio::io::Error),
    /// The input was not valid UTF8 text.
    BadUtf8(Utf8Error),
    /// An error occurred attempting to parse the valid UTF8 input.
    Parser(ParseError),
    /// Some of the input string was not consumed by the parser.
    UnconsumedInput,
}

impl Display for AsyncParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncParseError::Io(err) => write!(f, "IO error loading recon document: {}", err),
            AsyncParseError::BadUtf8(err) => {
                write!(f, "Recon data contained invalid UTF8: {}", err)
            }
            AsyncParseError::Parser(err) => {
                write!(f, "Error parsing recon data: {}", err)
            }
            AsyncParseError::UnconsumedInput => {
                write!(
                    f,
                    "Some of the input string was not consumed by the parser."
                )
            }
        }
    }
}

impl Error for AsyncParseError {}

impl From<tokio::io::Error> for AsyncParseError {
    fn from(err: tokio::io::Error) -> Self {
        AsyncParseError::Io(err)
    }
}

async fn read_to_buffer<In>(
    mut input: In,
    buffer: &mut BytesMut,
    cap: &mut usize,
) -> Result<bool, AsyncParseError>
where
    In: AsyncRead + Unpin,
{
    if buffer.capacity() == 0 {
        *cap = (*cap).min(buffer.len() * 2);
        buffer.reserve(*cap);
    }
    Ok(input.read_buf(buffer).await? > 0)
}

async fn consume_remainder<In>(mut input: In, buffer: &mut BytesMut) -> Result<(), AsyncParseError>
where
    In: AsyncRead + Unpin,
{
    buffer.clear();
    while input.read_buf(buffer).await? > 0 {
        let content = read_utf8(buffer.as_ref())?;
        if !content.chars().all(char::is_whitespace) {
            return Err(AsyncParseError::UnconsumedInput);
        }
    }
    Ok(())
}

fn read_utf8(content: &[u8]) -> Result<&str, AsyncParseError> {
    match core::str::from_utf8(content) {
        Ok(s) => Ok(s),
        Err(e) if e.error_len().is_some() => Err(AsyncParseError::BadUtf8(e)),
        Err(e) => Ok(unsafe { core::str::from_utf8_unchecked(&content[..e.valid_up_to()]) }),
    }
}

async fn run_parser<In, R>(
    mut input: In,
    recognizer: &mut R,
    buffer: &mut BytesMut,
    parser: &mut IncrementalReconParser,
    tracker: &mut LocationTracker,
) -> Result<Option<R::Target>, AsyncParseError>
where
    In: AsyncRead + Unpin,
    R: Recognizer,
{
    let mut cap = buffer.capacity();
    'read: loop {
        if !read_to_buffer(&mut input, buffer, &mut cap).await? {
            break 'read;
        }
        let string = read_utf8(buffer.as_ref())?;
        let mut span = Span::new(string);
        'token: loop {
            match parser.parse(span) {
                Ok((remainder, mut events)) => {
                    span = remainder;

                    'feed: loop {
                        match events.next() {
                            Some(Some(event)) => match recognizer.feed_event(event) {
                                Some(Ok(t)) => {
                                    if events.is_empty() {
                                        return Ok(Some(t));
                                    } else {
                                        return Err(AsyncParseError::UnconsumedInput);
                                    }
                                }
                                Some(Err(e)) => {
                                    return Err(AsyncParseError::Parser(ParseError::from(e)));
                                }
                                _ => {}
                            },
                            Some(None) => {
                                break 'read;
                            }
                            _ => {
                                break 'feed;
                            }
                        }
                    }
                }
                Err(nom::Err::Incomplete(_)) => {
                    break 'token;
                }
                Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
                    return Err(tracker.relativize_error(e));
                }
            }
        }
        tracker.update(&span);
        let to_advance = string.len() - span.len();
        buffer.advance(to_advance);
    }
    Ok(None)
}

const DEFAULT_BUFFER: usize = 4096;
const HEADER: &[u8] = b"{";
const TRAILER: &[u8] = b"}";

/// Attempt to read a Recon document from an asyncronous input.
pub async fn parse_recon_document<In>(input: In) -> Result<Vec<Item>, AsyncParseError>
where
    In: AsyncRead + Unpin,
{
    let mut recognizer = Value::make_recognizer();

    let mut buffer = BytesMut::with_capacity(DEFAULT_BUFFER);
    let mut parser = IncrementalReconParser::default();
    let mut tracker = LocationTracker::default();

    let mut wrapped = AsyncReadExt::chain(AsyncReadExt::chain(HEADER, input), TRAILER);

    let result = run_parser(
        &mut wrapped,
        &mut recognizer,
        &mut buffer,
        &mut parser,
        &mut tracker,
    )
    .await;

    match result {
        Ok(Some(Value::Record(_, items))) => {
            consume_remainder(&mut wrapped, &mut buffer).await?;
            Ok(items)
        }
        Ok(_) => Err(AsyncParseError::Parser(ParseError::Structure(
            ReadError::IncompleteRecord,
        ))),
        Err(AsyncParseError::Parser(ParseError::Syntax {
            kind,
            mut offset,
            line,
            mut column,
        })) => {
            //Correct offset and column for the implicit starting '{'.
            if line == 1 {
                offset -= 1;
                column -= 1;
            }
            Err(AsyncParseError::Parser(ParseError::Syntax {
                kind,
                offset,
                line,
                column,
            }))
        }
        Err(e) => Err(e),
    }
}

/// Asynchronously push the events generated by parsing the input into a recognizer.
pub async fn parse_recognize_with<In, R>(
    input: In,
    recognizer: &mut R,
) -> Result<R::Target, AsyncParseError>
where
    In: AsyncRead + Unpin,
    R: Recognizer,
{
    let mut buffer = BytesMut::new();
    let mut parser = IncrementalReconParser::default();
    let mut tracker = LocationTracker::default();

    if let Some(t) = run_parser(input, recognizer, &mut buffer, &mut parser, &mut tracker).await? {
        return Ok(t);
    }

    if !buffer.is_empty() {
        if let Some(mut terminal_parser) = parser.into_final_parser() {
            let bytes = buffer.as_ref();
            let string = match core::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(e) => {
                    return Err(AsyncParseError::BadUtf8(e));
                }
            };
            let span = Span::new(string);
            match terminal_parser.parse(span) {
                Ok((_, mut events)) => {
                    while let Some(Some(event)) = events.next() {
                        match recognizer.feed_event(event) {
                            Some(Ok(t)) => {
                                if events.is_empty() {
                                    return Ok(t);
                                } else {
                                    return Err(AsyncParseError::UnconsumedInput);
                                }
                            }
                            Some(Err(e)) => {
                                return Err(AsyncParseError::Parser(ParseError::from(e)));
                            }
                            _ => {}
                        }
                    }
                }
                Err(nom::Err::Incomplete(_)) => {
                    return Err(AsyncParseError::Parser(ParseError::Structure(
                        ReadError::IncompleteRecord,
                    )));
                }
                Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
                    return Err(tracker.relativize_error(e));
                }
            }
        }
    }
    match recognizer.try_flush() {
        Some(Ok(t)) => Ok(t),
        Some(Err(e)) => Err(AsyncParseError::Parser(ParseError::from(e))),
        _ => Err(AsyncParseError::Parser(ParseError::Structure(
            ReadError::IncompleteRecord,
        ))),
    }
}

#[derive(Debug)]
struct LocationTracker {
    offset: usize,
    line: u32,
    column: usize,
}

impl Default for LocationTracker {
    fn default() -> Self {
        LocationTracker {
            offset: 0,
            line: 1,
            column: 1,
        }
    }
}

impl LocationTracker {
    fn update(&mut self, span: &Span<'_>) {
        let LocationTracker {
            offset,
            line,
            column,
        } = self;
        *offset += span.location_offset();
        let span_line = span.location_line();
        if span_line == 1 {
            *column += span.get_utf8_column() - 1;
        } else {
            *line += span_line - 1;
            *column = span.get_utf8_column();
        }
    }

    fn relativize_error(&self, err: nom::error::Error<Span<'_>>) -> AsyncParseError {
        let nom::error::Error { input, code } = err;

        let span_line = input.location_line();
        let (line, column) = if span_line == 1 {
            (self.line, self.column + input.get_utf8_column() - 1)
        } else {
            (self.line + span_line - 1, input.get_utf8_column())
        };

        AsyncParseError::Parser(ParseError::Syntax {
            kind: code,
            offset: self.offset + input.location_offset(),
            line,
            column,
        })
    }
}

/// Tokio [`Decoder`] that parses its input as Recon and uses this to drive a [`Recognizer`] until
/// it completes. Not that this is cannot be used as a stand-alone decoder as it has no concept of
/// a separator between frames. I needs to be incorporated into another decoder that can determine
/// where one record ends and another begins.
pub struct RecognizerDecoder<R> {
    parser: IncrementalReconParser,
    recognizer: R,
    location: LocationTracker,
}

impl<R> RecognizerDecoder<R> {
    pub fn new(recognizer: R) -> Self {
        RecognizerDecoder {
            parser: IncrementalReconParser::default(),
            recognizer,
            location: LocationTracker::default(),
        }
    }
}

impl<R: Recognizer> RecognizerDecoder<R> {
    /// Reset the decoder to its initial state.
    pub fn reset(&mut self) {
        self.parser.reset();
        self.recognizer.reset();
        self.location = LocationTracker::default();
    }

    fn decode_inner<'a>(
        &mut self,
        span: Span<'a>,
    ) -> Result<(Span<'a>, Option<R::Target>), AsyncParseError> {
        let RecognizerDecoder {
            parser,
            recognizer,
            location,
        } = self;

        let mut current = span;
        let result = 'outer: loop {
            match parser.parse(current) {
                Ok((rem, events)) => {
                    current = rem;
                    for event_or_end in events {
                        if let Some(event) = event_or_end {
                            match recognizer.feed_event(event) {
                                Some(Ok(target)) => {
                                    break 'outer Ok((current, Some(target)));
                                }
                                Some(Err(e)) => {
                                    break 'outer Err(AsyncParseError::Parser(
                                        ParseError::Structure(e),
                                    ));
                                }
                                _ => {}
                            }
                        } else {
                            break 'outer match recognizer.try_flush() {
                                Some(Ok(target)) => Ok((rem, Some(target))),
                                Some(Err(e)) => {
                                    Err(AsyncParseError::Parser(ParseError::Structure(e)))
                                }
                                _ => Err(AsyncParseError::Parser(ParseError::Structure(
                                    ReadError::IncompleteRecord,
                                ))),
                            };
                        }
                    }
                }
                Err(nom::Err::Incomplete(_)) => {
                    break Ok((current, None));
                }
                Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
                    break Err(location.relativize_error(e));
                }
            }
        };
        location.update(&current);
        result
    }
}

fn feed_final<'a, R>(
    parser: &mut FinalSegmentParser,
    recognizer: &mut R,
    span: Span<'a>,
    location: &mut LocationTracker,
) -> Result<(Span<'a>, Option<R::Target>), AsyncParseError>
where
    R: Recognizer,
{
    match parser.parse(span) {
        Ok((rem, events)) => {
            for event_or_end in events {
                if let Some(event) = event_or_end {
                    match recognizer.feed_event(event) {
                        Some(Ok(target)) => {
                            return Ok((rem, Some(target)));
                        }
                        Some(Err(e)) => {
                            return Err(AsyncParseError::Parser(ParseError::Structure(e)));
                        }
                        _ => {}
                    }
                } else {
                    return match recognizer.try_flush() {
                        Some(Ok(target)) => Ok((rem, Some(target))),
                        Some(Err(e)) => Err(AsyncParseError::Parser(ParseError::Structure(e))),
                        _ => Err(AsyncParseError::Parser(ParseError::Structure(
                            ReadError::IncompleteRecord,
                        ))),
                    };
                }
            }
            Ok((rem, None))
        }
        Err(nom::Err::Incomplete(_)) => Ok((span, None)),
        Err(nom::Err::Error(e) | nom::Err::Failure(e)) => Err(location.relativize_error(e)),
    }
}

impl<R> Decoder for RecognizerDecoder<R>
where
    R: Recognizer,
{
    type Item = R::Target;
    type Error = AsyncParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let content = read_utf8(src.as_ref())?;
        let span = Span::new(content);
        match self.decode_inner(span) {
            Ok((rem, result)) => {
                let consumed = rem.location_offset() - span.location_offset();
                src.advance(consumed);
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let content = read_utf8(buf.as_ref())?;
        let span = Span::new(content);
        match self.decode_inner(span) {
            Ok((rem, output)) => {
                let (final_rem, result) = if output.is_none() {
                    let RecognizerDecoder {
                        parser,
                        recognizer,
                        location,
                    } = self;
                    let (final_rem, final_result) = match parser.final_parser_and_reset() {
                        Some(mut final_parser) => {
                            match feed_final(&mut final_parser, recognizer, rem, location) {
                                Ok((final_rem, None)) => (final_rem, None),
                                Ok((final_rem, ow)) => (final_rem, Some(Ok(ow))),
                                Err(e) => (span, Some(Err(e))),
                            }
                        }
                        _ => (span, None),
                    };
                    let finalized = if let Some(res) = final_result {
                        res
                    } else {
                        match recognizer.try_flush() {
                            Some(Ok(target)) => Ok(Some(target)),
                            Some(Err(e)) => Err(AsyncParseError::Parser(ParseError::Structure(e))),
                            _ => {
                                if buf.is_empty() {
                                    Ok(None)
                                } else {
                                    Err(AsyncParseError::Parser(ParseError::Structure(
                                        ReadError::IncompleteRecord,
                                    )))
                                }
                            }
                        }
                    };
                    (final_rem, finalized)
                } else {
                    (rem, Ok(output))
                };
                let consumed = final_rem.location_offset() - span.location_offset();
                buf.advance(consumed);
                result
            }
            Err(e) => Err(e),
        }
    }
}
