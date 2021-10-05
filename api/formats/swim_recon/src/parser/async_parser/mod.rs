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

#[cfg(test)]
mod tests;

use std::str::Utf8Error;
use std::fmt::{Display, Formatter};
use std::error::Error;
use bytes::{Buf, BytesMut};
use nom::error::ErrorKind;
use nom::Parser;
use tokio::io::{AsyncRead, AsyncReadExt};
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::ReadError;
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_model::{Item, Value};
use crate::parser::Span;
use crate::parser::ParseError;
use crate::parser::record::{IncrementalReconParser};

/// Error type for reading a configuration document.
#[derive(Debug)]
pub enum AsyncParseError {
    /// An IO error occurred reading the source data.
    Io(tokio::io::Error),
    /// The input was not valid UTF8 text.
    BadUtf8(Utf8Error),
    /// An error occurred attempting to parse the valid UTF8 input.
    Parser(ParseError),
    /// Some input tokens were not consumed by the parser.
    UnconsumedTokens,
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
            AsyncParseError::UnconsumedTokens => {
                write!(f, "Some input tokens were not consumed.")
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
    'read: loop {
        if input.read(buffer).await? == 0 {
            break 'read;
        }
        let bytes = buffer.as_ref();
        let string = match core::str::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) if e.error_len().is_some() => {
                return Err(AsyncParseError::BadUtf8(e));
            },
            Err(e) => {
                unsafe { core::str::from_utf8_unchecked(&bytes[..e.valid_up_to()]) }
            }
        };
        let mut span = Span::new(string);
        'token: loop {
            match parser.parse(span) {
                Ok((remainder, mut events)) => {
                    span = remainder;
                    'feed: loop {
                        match events.next() {
                            Some(Some(event)) => {
                                match recognizer.feed_event(event) {
                                    Some(Ok(t)) => {
                                        if events.is_empty() {
                                            return Ok(Some(t));
                                        } else {
                                            return Err(AsyncParseError::UnconsumedTokens);
                                        }
                                    }
                                    Some(Err(e)) => {
                                        return Err(AsyncParseError::Parser(ParseError::from(e)));
                                    }
                                    _ => {}
                                }
                            }
                            Some(None) => {
                                break 'read;
                            }
                            _ => {
                                break 'feed;
                            }
                        }
                    }
                },
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

/// Attempt to read a Recon document from an asyncronous input.
pub async fn parse_recon_document<In>(
    input: In,
) -> Result<Vec<Item>, AsyncParseError>
where
    In: AsyncRead + Unpin,
{
    let mut recognizer = Value::make_recognizer();
    recognizer.feed_event(ReadEvent::StartBody);

    let mut buffer = BytesMut::new();
    let mut parser = IncrementalReconParser::default();
    let mut tracker = LocationTracker::default();

    if run_parser(input, &mut recognizer, &mut buffer, &mut parser, &mut tracker).await?.is_some() {
        Err(AsyncParseError::UnconsumedTokens)
    } else {
        match recognizer.feed_event(ReadEvent::EndRecord) {
            Some(Ok(Value::Record(_, items))) => Ok(items),
            Some(Err(e)) => Err(AsyncParseError::Parser(ParseError::from(e))),
            _ => {
                match recognizer.try_flush() {
                    Some(Ok(Value::Record(_, items))) => Ok(items),
                    Some(Err(e)) => Err(AsyncParseError::Parser(ParseError::from(e))),
                    _ => {
                        Err(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)))
                    }
                }
            }
        }
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
                },
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
                                    return Err(AsyncParseError::UnconsumedTokens);
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
                    return Err(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)));
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
        _ => {
            Err(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)))
        }
    }
}

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
            column: 0,
        }
    }
}

impl LocationTracker {

    fn update(&mut self, span: &Span<'_>)  {
        let LocationTracker {
            offset,
            line,
            column
        } = self;
        *offset += span.location_offset();
        let span_line = span.location_line();
        if span_line == 1 {
            *column += span.get_utf8_column();
        } else {
            *line += span_line - 1;
            *column = span.get_utf8_column();
        }
    }

    fn relativize_error(&self, err: nom::error::Error<Span<'_>>) -> AsyncParseError {
        let nom::error::Error { input, code } = err;

        let span_line = input.location_line();
        let (line, column) = if span_line == 1 {
            (self.line, self.column + input.get_utf8_column())
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
