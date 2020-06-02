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

#[derive(Debug)]
pub struct NonEmptyStack<T>(Vec<T>);

impl<T> NonEmptyStack<T> {
    pub fn new(entry: T, capacity: usize) -> Self {
        let mut contents = Vec::with_capacity(capacity);
        contents.push(entry);
        NonEmptyStack(contents)
    }

    pub fn peek(&self) -> &T {
        let NonEmptyStack(contents) = self;
        &contents[contents.len() - 1]
    }

    pub fn peek_mut(&mut self) -> &mut T {
        let NonEmptyStack(contents) = self;
        let offset = contents.len() - 1;
        &mut contents[offset]
    }

    pub fn push(&mut self, entry: T) {
        let NonEmptyStack(contents) = self;
        contents.push(entry);
    }

    pub fn pop(&mut self) -> Option<T> {
        let NonEmptyStack(contents) = self;
        if contents.len() > 1 {
            contents.pop()
        } else {
            None
        }
    }

    pub fn take_top(self) -> T {
        let NonEmptyStack(mut contents) = self;
        contents.pop().unwrap()
    }
}
