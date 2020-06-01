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

#[cfg(test)]
mod tests;

const MAX_SIZE: usize = 128;

#[derive(Debug, Default)]
pub struct FrameMask(u128);

#[derive(Debug)]
pub struct FrameMaskIter<'a>(&'a u128, usize, usize);

impl FrameMask {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn insert(&mut self, n: usize) {
        assert!(
            n < MAX_SIZE,
            "A transaction can refer to at most {} variables.",
            MAX_SIZE
        );
        let FrameMask(m) = self;
        *m = *m | (1 << n);
    }

    pub fn contains(&self, n: usize) -> bool {
        let FrameMask(m) = self;
        (*m >> n) & 0x1 != 0
    }

    pub fn iter(&self) -> FrameMaskIter<'_> {
        let FrameMask(m) = self;
        FrameMaskIter(m, MAX_SIZE - m.leading_zeros() as usize, 0)
    }
}

impl<'a> Iterator for FrameMaskIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let FrameMaskIter(m, last_set, i) = self;
        let mask = *m;
        if *last_set <= *i {
            None
        } else {
            while *i < MAX_SIZE && (*mask & (1 << *i)) == 0 {
                *i += 1;
            }
            if *i >= MAX_SIZE {
                None
            } else {
                let index = *i;
                *i += 1;
                Some(index)
            }
        }
    }
}
