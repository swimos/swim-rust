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

use crate::parser::{ParseError, Span};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use swim_form::structural::read::event::ReadEvent;

#[cfg(test)]
mod tests;

pub fn calculate_hash<H: Hasher + Clone>(value: &str, hasher: H) -> Result<u64, HashError> {
    hash_incremental(
        &mut crate::parser::ParseIterator::new(Span::new(value), false),
        hasher,
    )
}

fn hash_incremental<'a, It, H>(iter: &mut It, hasher: H) -> Result<u64, HashError>
where
    It: Iterator<Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>>,
    H: Hasher + Clone,
{
    let mut recon_hasher = NormalisationHasher::new(hasher);

    for maybe_event in iter {
        let event = maybe_event.map_err(ParseError::from)?;

        match event {
            ReadEvent::StartAttribute(_) => {
                recon_hasher.push(event)?;
                recon_hasher.add_attr();
            }
            ReadEvent::EndAttribute => {
                let attr_contents = recon_hasher.pop().ok_or(ParseError::InvalidEventStream)?;
                recon_hasher.extend(attr_contents)?;
                recon_hasher.push(event)?;
            }
            _ => {
                recon_hasher.push(event)?;
            }
        }
    }

    recon_hasher.finish()
}

#[derive(Debug, Clone)]
struct NormalisationHasher<H> {
    stack: smallvec::SmallVec<[AttrContents<H>; 4]>,
    hasher: H,
}

impl<H: Hasher + Clone> NormalisationHasher<H> {
    fn new(hasher: H) -> Self {
        NormalisationHasher {
            stack: Default::default(),
            hasher,
        }
    }

    fn add_attr(&mut self) {
        self.stack.push(AttrContents::new(self.hasher.clone()));
    }

    fn push(&mut self, event: ReadEvent<'_>) -> Result<(), HashError> {
        match self.stack.last_mut() {
            Some(last) => last.push(event),
            None => {
                let mut attr_contents = AttrContents::new(self.hasher.clone());
                attr_contents.push(event)?;
                self.stack.push(attr_contents);
                Ok(())
            }
        }
    }

    fn pop(&mut self) -> Option<AttrContents<H>> {
        self.stack.pop()
    }

    fn extend(&mut self, attr_contents: AttrContents<H>) -> Result<(), HashError> {
        let attr_hash = attr_contents.finish();

        attr_hash.hash(
            &mut self
                .stack
                .last_mut()
                .ok_or(ParseError::InvalidEventStream)?
                .original_hasher,
        );

        attr_hash.hash(
            &mut self
                .stack
                .last_mut()
                .ok_or(ParseError::InvalidEventStream)?
                .normalised_hasher,
        );

        Ok(())
    }

    fn finish(mut self) -> Result<u64, HashError> {
        Ok(self
            .stack
            .pop()
            .ok_or(ParseError::InvalidEventStream)?
            .original_hasher
            .finish())
    }
}

#[derive(Debug, Clone)]
struct AttrContents<H> {
    /// Hasher calculating the hash of the original read events.
    original_hasher: H,
    /// Hasher calculating the hash of the normalised read events.
    normalised_hasher: H,
    /// Shows how deep we are in nested records.
    nested_level: usize,
    /// Flag showing if the attribute contains any slots.
    has_slot: bool,
    /// The number of items that the attribute contains.
    items: usize,
    /// Flag showing whether or not the last item that was added was an attribute.
    is_last_attr: bool,
}

impl<H: Hasher + Clone> AttrContents<H> {
    fn new(hasher: H) -> Self {
        let mut normalised_hasher = hasher.clone();
        ReadEvent::StartBody.hash(&mut normalised_hasher);

        AttrContents {
            original_hasher: hasher,
            normalised_hasher,
            nested_level: 0,
            has_slot: false,
            items: 0,
            is_last_attr: true,
        }
    }

    fn push(&mut self, event: ReadEvent<'_>) -> Result<(), HashError> {
        if matches!(event, ReadEvent::EndRecord | ReadEvent::EndAttribute) {
            self.nested_level = self
                .nested_level
                .checked_sub(1)
                .ok_or(ParseError::InvalidEventStream)?;
        } else if self.nested_level == 0 {
            if matches!(event, ReadEvent::Slot) {
                self.has_slot = true;
            } else if self.can_concat_attrs(&event) {
                self.is_last_attr = false;
                self.items += 1;
            }
        }

        if matches!(event, ReadEvent::StartAttribute(_)) {
            self.is_last_attr = true;
        }

        if matches!(event, ReadEvent::StartBody | ReadEvent::StartAttribute(_)) {
            self.nested_level += 1;
        }

        event.hash(&mut self.original_hasher);
        event.hash(&mut self.normalised_hasher);
        Ok(())
    }

    fn can_concat_attrs(&mut self, event: &ReadEvent<'_>) -> bool {
        !(matches!(
            event,
            ReadEvent::StartAttribute(_) | ReadEvent::EndAttribute
        ) && self.is_last_attr)
    }

    fn is_implicit_record(&self) -> bool {
        self.has_slot || self.items > 1
    }

    fn finish(mut self) -> u64 {
        if self.is_implicit_record() {
            ReadEvent::EndRecord.hash(&mut self.normalised_hasher);
            self.normalised_hasher.finish()
        } else {
            self.original_hasher.finish()
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashError(ParseError);

impl From<ParseError> for HashError {
    fn from(err: ParseError) -> Self {
        HashError(err)
    }
}

impl Error for HashError {}

impl Display for HashError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
