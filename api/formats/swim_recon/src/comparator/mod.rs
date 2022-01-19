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
use std::iter::Peekable;
use swim_form::structural::read::event::ReadEvent;

#[cfg(test)]
mod tests;

/// Compare two recon values incrementally, terminating early if a difference is discovered.
///
/// * `first` - The first recon value.
/// * `second` - The second recon value.
pub fn compare_values(first: &str, second: &str) -> bool {
    incremental_compare(
        &mut crate::parser::ParseIterator::new(Span::new(first), false).peekable(),
        &mut crate::parser::ParseIterator::new(Span::new(second), false).peekable(),
    )
}

fn incremental_compare<
    'a,
    It: Iterator<Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>>,
>(
    first_iter: &mut Peekable<It>,
    second_iter: &mut Peekable<It>,
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
                if event_1 == ReadEvent::StartBody {
                    validator_1.feed_event(event_1);

                    if let Some(Ok(event)) = first_iter.next() {
                        event_1 = event;
                    } else {
                        return false;
                    }
                }

                if event_1 == ReadEvent::EndRecord {
                    validator_1.feed_event(event_1);

                    if let Some(Ok(event)) = first_iter.next() {
                        event_1 = event;
                    } else {
                        return false;
                    }
                }

                if event_2 == ReadEvent::StartBody {
                    validator_2.feed_event(event_2);

                    if let Some(Ok(event)) = second_iter.next() {
                        event_2 = event;
                    } else {
                        return false;
                    }
                }

                if event_2 == ReadEvent::EndRecord {
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

                let first_value = validator_1.feed_event(event_1);
                let second_value = validator_2.feed_event(event_2);

                if first_value != second_value {
                    return false;
                }
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
    /// The internal stack of builders.
    stack: SmallVec<[BuilderState; 4]>,
    /// Flag indicating if there is a slot key.
    slot_key: Option<ValueType>,
}

impl InProgressState {
    fn new() -> Self {
        InProgressState {
            stack: SmallVec::with_capacity(4),
            slot_key: None,
        }
    }

    fn new_record_frame(&mut self, in_body: bool) {
        let frame = if let Some(key) = self.slot_key.take() {
            BuilderState {
                key: KeyState::Slot(key),
                in_body,
                attrs: Vec::with_capacity(4),
                items: Vec::with_capacity(4),
            }
        } else {
            BuilderState {
                key: KeyState::NoKey,
                in_body,
                attrs: Vec::with_capacity(4),
                items: Vec::with_capacity(4),
            }
        };

        self.stack.push(frame);
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
            attrs: Vec::with_capacity(4),
            items: Vec::with_capacity(4),
        })
    }

    fn set_slot_key(&mut self) -> Result<(), ()> {
        let key = match self.stack.last_mut().ok_or(())?.items.pop() {
            Some(ItemType::ValueItem(value)) => value,
            _ => ValueType::Primitive,
        };

        self.slot_key = Some(key);
        Ok(())
    }

    /// Pops the top element from the stack of builder states and returns either an Ok(bool), where
    /// the bool indicates whether or not the validator is done, or an error.
    fn pop(&mut self, is_attr_end: bool) -> Result<Option<ValueType>, ()> {
        if let Some(BuilderState {
            key,
            attrs,
            mut items,
            ..
        }) = self.stack.pop()
        {
            match key {
                KeyState::NoKey => {
                    if is_attr_end {
                        Err(())
                    } else {
                        let record = ValueType::Record(attrs, items);
                        if self.stack.is_empty() {
                            Ok(Some(record))
                        } else {
                            self.add_value(record)?;
                            Ok(None)
                        }
                    }
                }
                KeyState::Slot(key) => {
                    if is_attr_end {
                        Err(())
                    } else {
                        let record = ValueType::Record(attrs, items);
                        self.add_slot(key, record)?;
                        Ok(None)
                    }
                }
                KeyState::Attr => {
                    if is_attr_end {
                        let body = if attrs.is_empty() && items.len() <= 1 {
                            match items.pop() {
                                Some(ItemType::ValueItem(value)) => value,
                                Some(slot @ ItemType::Slot(_, _)) => {
                                    ValueType::Record(Vec::new(), vec![slot])
                                }
                                _ => ValueType::Primitive,
                            }
                        } else {
                            ValueType::Record(attrs, items)
                        };
                        self.add_attr(body)?;
                        Ok(None)
                    } else {
                        Err(())
                    }
                }
            }
        } else {
            Err(())
        }
    }

    fn add_attr(&mut self, value: ValueType) -> Result<(), ()> {
        self.stack.last_mut().ok_or(())?.attrs.push(value);
        Ok(())
    }

    fn add_item(&mut self, value: ValueType) -> Result<(), ()> {
        let slot_key = self.slot_key.take();
        let top = self.stack.last_mut().ok_or(())?;

        if top.in_body {
            if let Some(key) = slot_key {
                top.items.push(ItemType::Slot(key, value));
            } else {
                top.items.push(ItemType::ValueItem(value));
            }
            Ok(())
        } else {
            Err(())
        }
    }

    fn add_slot(&mut self, key: ValueType, value: ValueType) -> Result<(), ()> {
        self.stack
            .last_mut()
            .ok_or(())?
            .items
            .push(ItemType::Slot(key, value));
        Ok(())
    }

    fn add_value(&mut self, value: ValueType) -> Result<(), ()> {
        self.stack
            .last_mut()
            .ok_or(())?
            .items
            .push(ItemType::ValueItem(value));
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ItemType {
    ValueItem(ValueType),
    Slot(ValueType, ValueType),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ValueType {
    Primitive,
    Record(Vec<ValueType>, Vec<ItemType>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum KeyState {
    NoKey,
    Attr,
    Slot(ValueType),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct BuilderState {
    /// The type of the key for the current builder.
    key: KeyState,
    /// Flag indicating if the value from the builder is in a body.
    in_body: bool,
    /// A vector containing the attributes of the builder.
    attrs: Vec<ValueType>,
    /// A vector containing the items of the builder.
    items: Vec<ItemType>,
}

#[derive(Debug, Clone)]
struct ValueValidator {
    state: ValidatorState,
}

//Todo dm
impl PartialEq for ValueValidator {
    fn eq(&self, other: &Self) -> bool {
        match (&self.state, &other.state) {
            (ValidatorState::InProgress(self_state), ValidatorState::InProgress(other_state)) => {
                let mut self_iter = self_state.stack.iter().peekable();
                let mut other_iter = other_state.stack.iter().peekable();

                loop {
                    match (self_iter.next(), other_iter.next()) {
                        (Some(self_builder), Some(other_builder)) => {
                            if self_builder == other_builder {
                                continue;
                            } else {
                                let mut self_items = self_builder.items.clone();
                                let mut other_items = other_builder.items.clone();

                                let mut self_attrs = self_builder.attrs.clone();
                                let mut other_attrs = other_builder.attrs.clone();

                                loop {
                                    if let Some(self_builder_next) = self_iter.peek() {
                                        if self_builder_next.key == KeyState::NoKey {
                                            let self_builder_next = self_iter.next().unwrap();
                                            self_items.extend(self_builder_next.items.clone());
                                            self_attrs.extend(self_builder_next.attrs.clone());
                                        } else {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }

                                loop {
                                    if let Some(other_builder_next) = other_iter.peek() {
                                        if other_builder_next.key == KeyState::NoKey {
                                            let other_builder_next = other_iter.next().unwrap();
                                            other_items.extend(other_builder_next.items.clone());
                                            other_attrs.extend(other_builder_next.attrs.clone());
                                        } else {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }

                                if self_items == other_items && self_attrs == other_attrs {
                                    continue;
                                } else {
                                    return false;
                                }
                            }
                        }
                        (Some(self_builder), None) => {
                            if self_builder.key == KeyState::NoKey
                                && self_builder.attrs.len() == 0
                                && self_builder.items.len() == 0
                            {
                                continue;
                            }
                        }
                        (None, Some(other_builder)) => {
                            if other_builder.key == KeyState::NoKey
                                && other_builder.attrs.len() == 0
                                && other_builder.items.len() == 0
                            {
                                continue;
                            }
                        }
                        (None, None) => return true,
                    }
                }
            }
            (ValidatorState::Init, ValidatorState::Init) => true,
            _ => false,
        }
    }
}

impl ValueValidator {
    fn new() -> Self {
        ValueValidator {
            state: ValidatorState::Init,
        }
    }

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<ValueType> {
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
                    if state.add_item(ValueType::Primitive).is_err() {
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
                        if done.is_some() {
                            self.state = ValidatorState::Init
                        }
                    }
                    Err(_) => self.state = ValidatorState::Invalid,
                },
                ReadEvent::EndRecord => match state.pop(false) {
                    Ok(done) => {
                        if let Some(val) = done {
                            self.state = ValidatorState::Init;
                            return Some(val);
                        }
                    }
                    Err(_) => self.state = ValidatorState::Invalid,
                },
            },
            ValidatorState::Invalid => {}
        }
        None
    }
}
