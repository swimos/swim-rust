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

/// Flags indicating whether a transaction variable was read or written in a single frame of a
/// transaction.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadWrite {
    Read,
    Write,
    ReadWrite,
}

const MAX_SIZE: usize = 64;

/// Mask that tracks the variables that were read and written in a single frame of a transaction.
#[derive(Debug, Default)]
pub struct FrameMask(u128);

#[derive(Debug)]
pub struct FrameMaskIter<'a>(&'a FrameMask, usize, usize);

impl FrameMask {
    pub fn new() -> Self {
        Default::default()
    }

    /// Record that a variable was read in this frame.
    pub fn read(&mut self, n: usize) {
        assert!(
            n < MAX_SIZE,
            "A transaction can refer to at most {} variables.",
            MAX_SIZE
        );
        if !self.writes(n) {
            // If a value is read after it was written, no read to the underlying variable occurs.
            let FrameMask(m) = self;
            *m |= 1 << (2 * n);
        }
    }

    /// Record that a variable was written in this frame.
    pub fn write(&mut self, n: usize) {
        assert!(
            n < MAX_SIZE,
            "A transaction can refer to at most {} variables.",
            MAX_SIZE
        );
        let FrameMask(m) = self;
        *m |= 1 << (2 * n + 1);
    }

    fn extract(&self, n: usize) -> u8 {
        let FrameMask(m) = self;
        ((*m >> (2 * n)) & 0x3) as u8
    }

    fn writes(&self, n: usize) -> bool {
        self.extract(n) > 1
    }

    /// Get the flags for a specific variable.
    pub fn get(&self, n: usize) -> Option<ReadWrite> {
        match self.extract(n) {
            3 => Some(ReadWrite::ReadWrite),
            2 => Some(ReadWrite::Write),
            1 => Some(ReadWrite::Read),
            _ => None,
        }
    }

    /// Iterate over the variables that have flags set.
    pub fn iter(&self) -> FrameMaskIter<'_> {
        FrameMaskIter(self, 2 * MAX_SIZE - self.0.leading_zeros() as usize, 0)
    }
}

impl<'a> Iterator for FrameMaskIter<'a> {
    type Item = (usize, ReadWrite);

    fn next(&mut self) -> Option<Self::Item> {
        let FrameMaskIter(m, last_set, i) = self;
        if *last_set <= 2 * *i {
            None
        } else {
            let mut current = None;
            while *i < MAX_SIZE && current.is_none() {
                if let Some(rw) = m.get(*i) {
                    current = Some((*i, rw));
                }
                *i += 1;
            }
            current
        }
    }
}
