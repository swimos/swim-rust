// Copyright 2015-2022 Swim Inc.
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

use crate::parser::Span;
use smallvec::SmallVec;
use swim_form::structural::read::event::ReadEvent;

#[cfg(test)]
mod tests;

/// Compare two recon values incrementally, terminating early if a difference is discovered.
///
/// * `first` - The first recon value.
/// * `second` - The second recon value.
pub fn compare_values(first: &str, second: &str) -> bool {
    incremental_compare(
        &mut crate::parser::ParseIterator::new(Span::new(first), false),
        &mut crate::parser::ParseIterator::new(Span::new(second), false),
    )
}

fn incremental_compare<
    'a,
    It: Iterator<Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>>,
>(
    first_iter: &mut It,
    second_iter: &mut It,
) -> bool {
    let mut validator_1 = ValueValidator::new();
    let mut validator_2 = ValueValidator::new();

    loop {
        match (first_iter.next(), second_iter.next()) {
            (Some(Ok(event_1)), Some(Ok(event_2))) if event_1 == event_2 => {
                validator_1.feed_event(event_1);
                validator_2.feed_event(event_2);
            }

            (Some(Ok(mut event_1)), Some(Ok(mut event_2))) if event_1 != event_2 => {
                if event_1 == ReadEvent::StartBody || event_1 == ReadEvent::EndRecord {
                    validator_1.feed_event(event_1);

                    if let Some(Ok(event)) = first_iter.next() {
                        event_1 = event;
                    } else {
                        return false;
                    }
                }

                if event_2 == ReadEvent::StartBody || event_2 == ReadEvent::EndRecord {
                    validator_2.feed_event(event_2);

                    if let Some(Ok(event)) = second_iter.next() {
                        event_2 = event;
                    } else {
                        return false;
                    }
                }

                if event_1 != event_2 {
                    return false;
                }

                validator_1.feed_event(event_1);
                validator_2.feed_event(event_2);
            }

            (Some(Ok(event_1)), None) => {
                validator_1.feed_event(event_1);
            }
            (None, Some(Ok(event_2))) => {
                validator_2.feed_event(event_2);
            }

            (Some(Err(_)), _) | (_, Some(Err(_))) => {
                return false;
            }

            _ => {
                return validator_1 == validator_2;
            }
        }

        if validator_1 != validator_2 {
            return false;
        }
    }
}

#[derive(Debug, Clone)]
enum ValidatorState {
    Init,
    InProgress(InProgressState),
    Invalid,
}

#[derive(Debug, Clone)]
struct InProgressState {
    stack: SmallVec<[BuilderState; 4]>,
    slot_key: bool,
}

impl InProgressState {
    fn new() -> Self {
        InProgressState {
            stack: SmallVec::with_capacity(4),
            slot_key: false,
        }
    }

    fn new_record_frame(&mut self, in_body: bool) {
        let key = if self.slot_key {
            KeyState::Slot
        } else {
            KeyState::NoKey
        };

        self.stack.push(BuilderState { key, in_body });
        self.slot_key = false;
    }

    fn new_record_item(&mut self) -> Result<(), ()> {
        let top = self.stack.last_mut().ok_or(())?;

        if top.in_body {
            self.new_record_frame(true);
        } else {
            top.in_body = true;
        }
        Ok(())
    }

    fn new_attr_frame(&mut self) {
        match self.stack.last() {
            Some(top) if !top.in_body => {}
            _ => self.new_record_frame(false),
        }

        self.stack.push(BuilderState {
            key: KeyState::Attr,
            in_body: true,
        })
    }

    fn set_slot_key(&mut self) -> Result<(), ()> {
        self.stack.last().ok_or(())?;
        self.slot_key = true;
        Ok(())
    }

    fn pop(&mut self, is_attr_end: bool) -> Result<bool, ()> {
        if let Some(BuilderState { key, .. }) = self.stack.pop() {
            match key {
                KeyState::NoKey => {
                    if self.stack.is_empty() {
                        Ok(true)
                    } else {
                        self.add_value()?;
                        Ok(false)
                    }
                }
                KeyState::Slot => {
                    if is_attr_end {
                        Err(())
                    } else {
                        self.add_slot()?;
                        Ok(false)
                    }
                }
                KeyState::Attr => {
                    if is_attr_end {
                        self.add_attr()?;
                        Ok(false)
                    } else {
                        Err(())
                    }
                }
            }
        } else {
            Err(())
        }
    }

    fn add_attr(&mut self) -> Result<(), ()> {
        self.stack.last().ok_or(())?;
        Ok(())
    }

    fn add_item(&mut self) -> Result<(), ()> {
        let top = self.stack.last().ok_or(())?;

        if top.in_body {
            self.slot_key = false;
            Ok(())
        } else {
            Err(())
        }
    }

    fn add_slot(&mut self) -> Result<(), ()> {
        self.stack.last().ok_or(())?;
        Ok(())
    }

    fn add_value(&mut self) -> Result<(), ()> {
        self.stack.last().ok_or(())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum KeyState {
    NoKey,
    Attr,
    Slot,
}

#[derive(Debug, Clone, Copy)]
struct BuilderState {
    key: KeyState,
    in_body: bool,
}

#[derive(Debug, Clone)]
struct ValueValidator {
    state: ValidatorState,
}

impl PartialEq for ValueValidator {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (&self.state, &other.state),
            (ValidatorState::Init, ValidatorState::Init)
                | (ValidatorState::InProgress(_), ValidatorState::InProgress(_))
        )
    }
}

impl ValueValidator {
    fn new() -> Self {
        ValueValidator {
            state: ValidatorState::Init,
        }
    }

    fn feed_event(&mut self, input: ReadEvent<'_>) {
        match &mut self.state {
            ValidatorState::Init => match input {
                ReadEvent::StartAttribute(_) => {
                    let mut state = InProgressState::new();
                    state.new_attr_frame();
                    self.state = ValidatorState::InProgress(state);
                }
                ReadEvent::StartBody => {
                    let mut state = InProgressState::new();
                    state.new_record_frame(true);
                    self.state = ValidatorState::InProgress(state);
                }
                ReadEvent::Slot => self.state = ValidatorState::Invalid,
                ReadEvent::EndAttribute => self.state = ValidatorState::Invalid,
                ReadEvent::EndRecord => self.state = ValidatorState::Invalid,
                _ => {}
            },
            ValidatorState::InProgress(state) => match input {
                ReadEvent::Extant
                | ReadEvent::TextValue(_)
                | ReadEvent::Number(_)
                | ReadEvent::Boolean(_)
                | ReadEvent::Blob(_) => {
                    if state.add_item().is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::StartAttribute(_) => {
                    state.new_attr_frame();
                }
                ReadEvent::StartBody => {
                    if state.new_record_item().is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::Slot => {
                    if state.set_slot_key().is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::EndAttribute => match state.pop(true) {
                    Ok(done) => {
                        if done {
                            self.state = ValidatorState::Init
                        }
                    }
                    Err(_) => self.state = ValidatorState::Invalid,
                },
                ReadEvent::EndRecord => match state.pop(false) {
                    Ok(done) => {
                        if done {
                            self.state = ValidatorState::Init
                        }
                    }
                    Err(_) => self.state = ValidatorState::Invalid,
                },
            },
            ValidatorState::Invalid => {}
        }
    }
}
