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

use crate::model::parser::token_buffer::MarkState::Marked;

/// Trait for types that keep logical pointers into a range of the input for the purpose of
/// extracting tokens. This allows us to use the same parsing logic to extract references
/// to sub-slices of complete documents in memory and to construct heap allocated strings
/// for streams of characters.
pub(super) trait TokenBuffer<T> {
    /// Add the next character (and byte offset) to the buffer (or ['None'] when we reach
    /// the end of the input.
    fn update(&mut self, next: Option<(usize, char)>) -> ();

    /// Indicate that we will be taking a slice of the input from this point in the future.
    fn mark(&mut self, inclusive: bool) -> ();

    /// Take a slice of the input. The from index must not be before the last index and which mark
    /// was called and the to index must not be greater than the index of the last character
    /// passed to update. The mark will be discarded.
    fn take(&mut self, from: usize, to: usize) -> T;

    /// Take a slice of the input by reference. The from index must not be before the last index
    /// and which mark was called and the to index must not be greater than the index of the last
    /// character passed to update. The mark will be discarded.
    fn take_ref(&mut self, from: usize, to: usize) -> &str;

    /// Take a slice of the input up until the last received character. The from index must not
    /// be before the last index and which mark was called. The mark will be discarded.
    fn take_all(&mut self, from: usize) -> T;

    /// Take a slice of the input up until the last received character, by reference. The from
    /// index must not be before the last index and which mark was called. The mark will be
    /// discarded.
    fn take_all_ref(&mut self, from: usize) -> &str;

    /// Call either 'take()' or 'take_all()' depending on whether an upper bound is provided.
    fn take_opt(&mut self, from: usize, maybe_to: Option<usize>) -> T {
        match maybe_to {
            Some(to) => self.take(from, to),
            _ => self.take_all(from),
        }
    }

    /// Call either 'take_ref()' or 'take_all_ref()' depending on whether an upper bound is
    /// provided.
    fn take_opt_ref(&mut self, from: usize, maybe_to: Option<usize>) -> &str {
        match maybe_to {
            Some(to) => self.take_ref(from, to),
            _ => self.take_all_ref(from),
        }
    }
}

/// A token buffer that simply maintains two pointers into string held entirely in memory.
pub(super) struct InMemoryInput<'a> {
    source: &'a str,
    lower_bound: usize,
    upper_bound: usize,
    marked: bool,
    done: bool,
}

impl<'a> InMemoryInput<'a> {
    pub(super) fn new(source: &'a str) -> InMemoryInput<'a> {
        InMemoryInput {
            source,
            lower_bound: 0,
            upper_bound: 0,
            marked: false,
            done: false,
        }
    }
}

impl<'a> TokenBuffer<&'a str> for InMemoryInput<'a> {
    fn update(&mut self, next: Option<(usize, char)>) -> () {
        let InMemoryInput {
            source,
            lower_bound,
            upper_bound,
            marked,
            done,
        } = self;
        if *done {
            panic!("Token buffer used after end of input.");
        }
        if !*marked {
            *lower_bound = *upper_bound;
        }
        match next {
            Some((next_off, _)) => {
                *upper_bound = next_off;
            }
            _ => {
                *upper_bound = source.len();
                *done = true;
            }
        }
    }

    fn mark(&mut self, _: bool) -> () {
        self.marked = true;
    }

    fn take(&mut self, from: usize, to: usize) -> &'a str {
        let InMemoryInput {
            source,
            lower_bound,
            upper_bound,
            ..
        } = self;
        if from < *lower_bound || to > *upper_bound {
            panic!("Invalid token slice.")
        } else {
            &source[from..to]
        }
    }

    fn take_ref(&mut self, from: usize, to: usize) -> &str {
        self.take(from, to)
    }

    fn take_all(&mut self, from: usize) -> &'a str {
        self.take(from, self.upper_bound)
    }

    fn take_all_ref(&mut self, from: usize) -> &str {
        self.take_all(from)
    }
}

enum MarkState {
    None,
    MarkNext,
    Marked,
}

/// Token buffer the holds the characters seen since the last mark in a ['String'].
pub(super) struct TokenAccumulator {
    buffer: String,
    next_char: Option<char>,
    mark_state: MarkState,
    lower_bound: usize,
    upper_bound: usize,
}

impl TokenAccumulator {
    pub(super) fn new() -> TokenAccumulator {
        TokenAccumulator {
            buffer: String::new(),
            next_char: None,
            mark_state: MarkState::None,
            lower_bound: 0,
            upper_bound: 0,
        }
    }
}

impl TokenBuffer<String> for TokenAccumulator {
    fn update(&mut self, next: Option<(usize, char)>) -> () {
        let TokenAccumulator {
            buffer,
            next_char,
            mark_state,
            lower_bound,
            upper_bound,
        } = self;
        if let Some(c) = next_char {
            match *mark_state {
                MarkState::None => {
                    *lower_bound = *upper_bound;
                    buffer.clear();
                }
                MarkState::MarkNext => {
                    *lower_bound = *upper_bound;
                    buffer.clear();
                    *mark_state = MarkState::Marked;
                }
                Marked => {}
            }
            buffer.push(*c);
            *upper_bound += c.len_utf8();
        }
        *next_char = next.map(|p| p.1);
    }

    fn mark(&mut self, inclusive: bool) -> () {
        self.mark_state = if inclusive {
            MarkState::Marked
        } else {
            MarkState::MarkNext
        }
    }

    fn take(&mut self, from: usize, to: usize) -> String {
        let TokenAccumulator {
            buffer,
            mark_state,
            lower_bound,
            upper_bound,
            ..
        } = self;
        if from > to || from < *lower_bound || to > *upper_bound {
            panic!("Invalid token slice.")
        } else {
            let start = from - *lower_bound;
            let end = to - *lower_bound;

            *mark_state = MarkState::None;
            if start == 0 && to == buffer.len() {
                std::mem::take(buffer)
            } else {
                buffer[start..end].to_owned()
            }
        }
    }

    fn take_ref(&mut self, from: usize, to: usize) -> &str {
        let TokenAccumulator {
            buffer,
            mark_state,
            lower_bound,
            upper_bound,
            ..
        } = self;
        if from > to || from < *lower_bound || to > *upper_bound {
            panic!("Invalid token slice.")
        } else {
            let start = from - *lower_bound;
            let end = to - *lower_bound;

            *mark_state = MarkState::None;
            &buffer[start..end]
        }
    }

    fn take_all(&mut self, from: usize) -> String {
        self.take(from, self.upper_bound)
    }

    fn take_all_ref(&mut self, from: usize) -> &str {
        self.take_ref(from, self.upper_bound)
    }
}
