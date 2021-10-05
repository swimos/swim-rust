// Copyright 2015-2021 SWIM.AI inc.
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

use std::fmt::{Display, Formatter};

struct Joined<'a, T>(&'a T, &'a str);

impl<'a, T> Display for Joined<'a, T>
where
    &'a T: IntoIterator + 'a,
    <&'a T as IntoIterator>::Item: Display + 'a,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Joined(value, sep) = self;
        let mut it = value.into_iter();
        if let Some(item) = it.next() {
            write!(f, "{}", item)?;
        }
        for item in it {
            write!(f, "{}{}", sep, item)?;
        }
        Ok(())
    }
}

/// Format a sequence, placing separator between successive elements.
pub fn join<'a, T>(value: &'a T, sep: &'a str) -> impl Display + 'a
where
    &'a T: IntoIterator + 'a,
    <&'a T as IntoIterator>::Item: Display + 'a,
{
    Joined(value, sep)
}

/// Print out a sequence with commas between the elements.
pub fn comma_sep<'a, T>(value: &'a T) -> impl Display + 'a
where
    &'a T: IntoIterator + 'a,
    <&'a T as IntoIterator>::Item: Display + 'a,
{
    join(value, ", ")
}
