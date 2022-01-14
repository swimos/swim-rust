use crate::parser::Span;
use smallvec::IntoIter;
use std::iter::Peekable;
use swim_form::structural::read::event::ReadEvent;

#[cfg(test)]
mod tests;

struct NormalisationStack<'a> {
    stack: smallvec::SmallVec<[AttrContents<'a>; 4]>,
}

impl<'a> NormalisationStack<'a> {
    fn new() -> Self {
        NormalisationStack {
            stack: Default::default(),
        }
    }

    fn push(&mut self, event: ReadEvent<'a>) {
        match self.stack.last_mut() {
            Some(last) => last.push(event),
            None => {
                let mut attr_contents = AttrContents::new();
                attr_contents.push(event);
                self.stack.push(attr_contents);
            }
        }
    }

    fn pop(&mut self) -> Option<AttrContents<'a>> {
        self.stack.pop()
    }

    fn start_attr(&mut self) {
        self.stack.push(AttrContents::new());
    }

    fn end_attr(&mut self) {
        self.stack.push(AttrContents::new());
    }
}

struct AttrContents<'a> {
    contents: smallvec::SmallVec<[ReadEvent<'a>; 10]>,
    has_slot: bool,
    primitives: usize,
}

impl<'a> AttrContents<'a> {
    fn new() -> Self {
        AttrContents {
            contents: Default::default(),
            has_slot: false,
            primitives: 0,
        }
    }

    fn push(&mut self, event: ReadEvent<'a>) {
        //Todo dm check if you need to add brackets
        if matches!(event, ReadEvent::Slot) {
            self.has_slot = true;
        }

        if event.is_primitive() {
            self.primitives += 1;
        }

        self.contents.push(event);
    }

    fn implicit_record(&self) -> bool {
        //Todo dm implement
        false
    }
}

impl<'a> IntoIterator for AttrContents<'a> {
    type Item = ReadEvent<'a>;
    type IntoIter = IntoIter<[ReadEvent<'a>; 10]>;

    fn into_iter(self) -> Self::IntoIter {
        self.contents.into_iter()
    }
}

fn normalise<'a, It: Iterator<Item = Result<ReadEvent<'a>, nom::error::Error<Span<'a>>>>>(
    iter: &mut Peekable<It>,
) -> Result<Vec<ReadEvent<'a>>, ()> {
    let mut stack = NormalisationStack::new();

    while let Some(maybe_event) = iter.next() {
        let event = maybe_event.map_err(|_| ())?;

        if matches!(event, ReadEvent::StartAttribute(_)) {
            stack.push(event);
            stack.start_attr();
        } else if matches!(event, ReadEvent::EndAttribute) {
            let attr_contents = stack.pop().ok_or(())?;

            if attr_contents.implicit_record() {
                stack.push(ReadEvent::StartBody);
                stack
                    .stack
                    .last_mut()
                    .ok_or(())?
                    .contents
                    .extend(attr_contents);
                stack.push(ReadEvent::EndRecord);
            } else {
                stack
                    .stack
                    .last_mut()
                    .ok_or(())?
                    .contents
                    .extend(attr_contents);
            }

            stack.push(event);
        } else {
            stack.push(event);
        }
    }

    // Ok(stack.into_iter().fold(vec![], |mut normalised, n| {
    //     normalised.extend(n);
    //     normalised
    // }))
    Err(())
}
