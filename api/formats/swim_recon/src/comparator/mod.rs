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
                    eprintln!("validator_1 = {:?}", validator_1);
                    eprintln!("validator_2 = {:?}", validator_2);

                    if let Some(Ok(event)) = first_iter.next() {
                        event_1 = event;
                    } else {
                        return false;
                    }
                }

                if event_2 == ReadEvent::StartBody {
                    validator_2.feed_event(event_2);
                    eprintln!("validator_1 = {:?}", validator_1);
                    eprintln!("validator_2 = {:?}", validator_2);

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
    InProgress,
    Invalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ItemType {
    ValueItem(ValueType),
    Slot(ValueType, ValueType),
}

impl ItemType {
    fn len(&self) -> usize {
        match self {
            ItemType::ValueItem(val) => val.len(),
            ItemType::Slot(key, val) => key.len() + val.len(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ValueType {
    Primitive,
    Record(usize, usize),
}

impl ValueType {
    fn len(&self) -> usize {
        match self {
            ValueType::Primitive => 1,
            ValueType::Record(attrs, items) => {
                let mut size = 0;

                if *attrs == 0 {
                    size += 1;
                } else {
                    size += *attrs;
                }

                if *items == 0 {
                    size += 1;
                } else {
                    size += items
                }

                size
            }
        }
    }
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
    attrs: usize,
    /// A vector containing the items of the builder.
    items: SmallVec<[ItemType; 4]>,
}

impl BuilderState {
    fn items_len(&self) -> usize {
        let mut size = 0;

        for item in &self.items {
            size += item.len()
        }

        size
    }

    fn attrs_len(&self) -> usize {
        self.attrs
    }
}

#[derive(Debug, Clone)]
struct ValueValidator {
    /// The state of the validator.
    state: ValidatorState,
    /// The internal stack of builders.
    stack: SmallVec<[BuilderState; 4]>,
    /// Flag indicating if there is a slot key.
    slot_key: Option<ValueType>,
}

impl PartialEq for ValueValidator {
    fn eq(&self, other: &Self) -> bool {
        eprintln!("self = {:?}", self);
        eprintln!("other = {:?}", other);

        if self.slot_key != other.slot_key {
            return false;
        }

        match (&self.state, &other.state) {
            (ValidatorState::InProgress, ValidatorState::InProgress) => {
                let mut self_iter = self.stack.iter().peekable();
                let mut other_iter = other.stack.iter().peekable();

                loop {
                    match (self_iter.next(), other_iter.next()) {
                        (Some(self_builder), Some(other_builder)) => {
                            let mut self_items_len = self_builder.items_len();
                            let mut other_items_len = other_builder.items_len();

                            let mut self_attrs_len = self_builder.attrs_len();
                            let mut other_attrs_len = other_builder.attrs_len();

                            while let Some(self_builder_next) = self_iter.peek() {
                                if self_builder_next.key == KeyState::NoKey {
                                    let self_builder_next = self_iter.next().unwrap();
                                    self_items_len += self_builder_next.items_len();
                                    self_attrs_len += self_builder_next.attrs_len();
                                } else {
                                    break;
                                }
                            }
                            while let Some(other_builder_next) = other_iter.peek() {
                                if other_builder_next.key == KeyState::NoKey {
                                    let other_builder_next = other_iter.next().unwrap();
                                    other_items_len += other_builder_next.items_len();
                                    other_attrs_len += other_builder_next.attrs_len();
                                } else {
                                    break;
                                }
                            }

                            if self_items_len == other_items_len
                                && self_attrs_len == other_attrs_len
                            {
                                continue;
                            } else {
                                return false;
                            }
                        }
                        (Some(self_builder), None) => {
                            if self_builder.key == KeyState::NoKey
                                && self_builder.attrs == 0
                                && self_builder.items.is_empty()
                            {
                                continue;
                            }
                        }
                        (None, Some(other_builder)) => {
                            if other_builder.key == KeyState::NoKey
                                && other_builder.attrs == 0
                                && other_builder.items.is_empty()
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
            stack: SmallVec::with_capacity(4),
            slot_key: None,
        }
    }

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<ValueType> {
        eprintln!("input = {:?}", input);
        match &mut self.state {
            ValidatorState::Init => match input {
                ReadEvent::StartAttribute(_) => {
                    self.new_attr_frame();
                    self.state = ValidatorState::InProgress;
                }
                ReadEvent::StartBody => {
                    self.new_record_frame(true);
                    self.state = ValidatorState::InProgress;
                }
                ReadEvent::Slot => self.state = ValidatorState::Invalid,
                ReadEvent::EndAttribute => self.state = ValidatorState::Invalid,
                ReadEvent::EndRecord => self.state = ValidatorState::Invalid,
                _ => {}
            },
            ValidatorState::InProgress => match input {
                ReadEvent::Extant
                | ReadEvent::TextValue(_)
                | ReadEvent::Number(_)
                | ReadEvent::Boolean(_)
                | ReadEvent::Blob(_) => {
                    if self.add_item(ValueType::Primitive).is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::StartAttribute(_) => {
                    self.new_attr_frame();
                }
                ReadEvent::StartBody => {
                    if self.new_record_item().is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::Slot => {
                    if self.set_slot_key().is_err() {
                        self.state = ValidatorState::Invalid
                    }
                }
                ReadEvent::EndAttribute => match self.pop(true) {
                    Ok(done) => {
                        if done.is_some() {
                            self.state = ValidatorState::Init
                        }
                    }
                    Err(_) => self.state = ValidatorState::Invalid,
                },
                ReadEvent::EndRecord => match self.pop(false) {
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

    fn new_record_frame(&mut self, in_body: bool) {
        let frame = if let Some(key) = self.slot_key.take() {
            BuilderState {
                key: KeyState::Slot(key),
                in_body,
                attrs: 0,
                items: SmallVec::with_capacity(4),
            }
        } else {
            BuilderState {
                key: KeyState::NoKey,
                in_body,
                attrs: 0,
                items: SmallVec::with_capacity(4),
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
            attrs: 0,
            items: SmallVec::with_capacity(4),
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
                        let mut size = 0;
                        for i in items {
                            size += i.len();
                        }
                        if size == 0 {
                            size += 1;
                        }

                        let record = ValueType::Record(attrs, size);
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
                        let mut size = 0;
                        for i in items {
                            size += i.len();
                        }
                        if size == 0 {
                            size += 1;
                        }

                        let record = ValueType::Record(attrs, size);
                        self.add_slot(key, record)?;
                        Ok(None)
                    }
                }
                KeyState::Attr => {
                    if is_attr_end {
                        let body = if attrs == 0 && items.len() <= 1 {
                            match items.pop() {
                                Some(ItemType::ValueItem(value)) => value,
                                Some(slot @ ItemType::Slot(_, _)) => {
                                    ValueType::Record(0, slot.len())
                                }
                                _ => ValueType::Primitive,
                            }
                        } else {
                            let mut size = 0;
                            for i in items {
                                size += i.len();
                            }
                            if size == 0 {
                                size += 1;
                            }

                            ValueType::Record(attrs, size)
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
        self.stack.last_mut().ok_or(())?.attrs += value.len();
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
