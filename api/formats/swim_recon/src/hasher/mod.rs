use crate::parser::{ParseError, Span};
use smallvec::IntoIter;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use swim_form::structural::read::event::ReadEvent;

#[cfg(test)]
mod tests;

pub fn calculate_hash(value: &str) -> Result<u64, HashError> {
    let mut hasher = DefaultHasher::new();

    let stack = normalise(&mut crate::parser::ParseIterator::new(
        Span::new(value),
        false,
    ))?;

    stack.hash(&mut hasher);
    Ok(hasher.finish())
}

fn normalise<'a, It: Iterator<Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>>>(
    iter: &mut It,
) -> Result<NormalisationStack<'a>, HashError> {
    let mut stack = NormalisationStack::new();

    for maybe_event in iter {
        let event = maybe_event.map_err(ParseError::from)?;

        if matches!(event, ReadEvent::StartAttribute(_)) {
            stack.push(event)?;
            stack.add_attr();
        } else if matches!(event, ReadEvent::EndAttribute) {
            let attr_contents = stack.pop().ok_or(ParseError::InvalidEventStream)?;

            if attr_contents.is_implicit_record() {
                stack.push(ReadEvent::StartBody)?;
                stack.extend(attr_contents)?;
                stack.push(ReadEvent::EndRecord)?;
            } else {
                stack.extend(attr_contents)?;
            }

            stack.push(event)?;
        } else {
            stack.push(event)?;
        }
    }

    Ok(stack)
}

#[derive(Debug, Clone, Hash)]
struct NormalisationStack<'a> {
    stack: smallvec::SmallVec<[AttrContents<'a>; 4]>,
}

impl<'a> NormalisationStack<'a> {
    fn new() -> Self {
        NormalisationStack {
            stack: Default::default(),
        }
    }

    fn add_attr(&mut self) {
        self.stack.push(AttrContents::new());
    }

    fn push(&mut self, event: ReadEvent<'a>) -> Result<(), HashError> {
        match self.stack.last_mut() {
            Some(last) => last.push(event),
            None => {
                let mut attr_contents = AttrContents::new();
                attr_contents.push(event)?;
                self.stack.push(attr_contents);
                Ok(())
            }
        }
    }

    fn pop(&mut self) -> Option<AttrContents<'a>> {
        self.stack.pop()
    }

    fn extend(&mut self, attr_contents: AttrContents<'a>) -> Result<(), HashError> {
        self.stack
            .last_mut()
            .ok_or(ParseError::InvalidEventStream)?
            .contents
            .extend(attr_contents);

        Ok(())
    }
}

#[derive(Debug, Clone, Hash)]
struct AttrContents<'a> {
    /// The contents of the attribute.
    contents: smallvec::SmallVec<[ReadEvent<'a>; 10]>,
    /// Shows how deep we are in nested records.
    nested_level: usize,
    /// Flag showing if the attribute contains any slots.
    has_slot: bool,
    /// The number of items that the attribute contains.
    items: usize,
    /// Flag showing whether or not the last item that was added was an attribute.
    is_last_attr: bool,
}

impl<'a> AttrContents<'a> {
    fn new() -> Self {
        AttrContents {
            contents: Default::default(),
            nested_level: 0,
            has_slot: false,
            items: 0,
            is_last_attr: true,
        }
    }

    fn push(&mut self, event: ReadEvent<'a>) -> Result<(), HashError> {
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

        self.contents.push(event);
        Ok(())
    }

    fn can_concat_attrs(&mut self, event: &ReadEvent<'a>) -> bool {
        !(matches!(
            event,
            ReadEvent::StartAttribute(_) | ReadEvent::EndAttribute
        ) && self.is_last_attr)
    }

    fn is_implicit_record(&self) -> bool {
        self.has_slot || self.items > 1
    }
}

impl<'a> IntoIterator for AttrContents<'a> {
    type Item = ReadEvent<'a>;
    type IntoIter = IntoIter<[ReadEvent<'a>; 10]>;

    fn into_iter(self) -> Self::IntoIter {
        self.contents.into_iter()
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
