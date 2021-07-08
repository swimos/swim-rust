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

mod error;
mod record;
#[cfg(test)]
mod tests;
mod tokens;

use crate::form::structural::read::event::ReadEvent;
pub use crate::form::structural::read::parser::error::ParseError;
use crate::form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use crate::form::structural::read::ReadError;
use nom_locate::LocatedSpan;

/// Wraps a string in a strucutre that keeps track of the line and column
/// as the input is parsed.
pub type Span<'a> = LocatedSpan<&'a str>;

/// Create an itearator that will parse a sequence of events from a complete string.
pub fn parse_iterator(
    input: Span<'_>,
) -> impl Iterator<Item = Result<ReadEvent<'_>, nom::error::Error<Span<'_>>>> + '_ {
    record::ParseIterator::new(input)
}

/// Push the events generated by parsing the input into a recognizer.
pub fn parse_recognize_with<R: Recognizer>(
    input: Span<'_>,
    recognizer: &mut R,
) -> Result<R::Target, ParseError> {
    let mut iterator = record::ParseIterator::new(input);
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

/// Create a [`Recognizer`] state machine for a type, and feed the stream of [`ParseEvent`]s,
/// produced by parsing a Recon string, into it.
pub fn parse_recognize<T: RecognizerReadable>(input: Span<'_>) -> Result<T, ParseError> {
    let mut recognizer = T::make_recognizer();
    parse_recognize_with(input, &mut recognizer)
}
