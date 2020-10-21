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

use http::uri::{InvalidUri, Uri};
use std::borrow::{Borrow, BorrowMut};
use std::cmp::Ordering;
use std::convert::{Infallible, TryFrom};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::str;
use std::str::FromStr;
use utilities::uri::{BadRelativeUri, RelativeUri};

const SMALL_SIZE: usize = 3 * std::mem::size_of::<usize>();

enum TextInner {
    Small(usize, [u8; SMALL_SIZE]),
    Large(String),
}

/// A container for a UTF-8 string that has a small string optimization for strings consisting of
/// `SMALL_SIZE` bytes (allowing such strings to be held entirely within the object rather than
/// requiring a separate allocation. This can be used in exactly the same way as [`String`] in
/// most circumstances.
pub struct Text(TextInner);

impl Text {
    /// Create a new [`Text`] instance from UTF-8 characters.
    pub fn new(string: &str) -> Self {
        string.into()
    }

    /// Create an empty [`Text`].
    pub fn empty() -> Self {
        Text::default()
    }

    /// Move a [`String`] into a [`Text`] instance.
    pub fn from_string(string: String) -> Self {
        string.into()
    }

    /// Borrow the characters stored in the [`Text`].
    pub fn as_str(&self) -> &str {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, bytes) => small_str(*len, bytes),
            TextInner::Large(str) => str.borrow(),
        }
    }

    /// Mutably borrow the characters stored in the [`Text`].
    pub fn as_str_mut(&mut self) -> &mut str {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, bytes) => small_str_mut(*len, bytes),
            TextInner::Large(str) => str.borrow_mut(),
        }
    }

    /// The length of the [`Text`] in bytes.
    pub fn len(&self) -> usize {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, _) => *len,
            TextInner::Large(string) => string.len(),
        }
    }

    /// Whether the [`Text`] is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the [`Text`] is contained entirely in this object.
    pub fn is_small(&self) -> bool {
        let Text(inner) = self;
        match inner {
            TextInner::Small(_, _) => true,
            TextInner::Large(_) => false,
        }
    }

    /// Append a character to this [`Text`].
    pub fn push(&mut self, ch: char) {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => {
                let ch_len = ch.len_utf8();
                if *len + ch_len <= SMALL_SIZE {
                    ch.encode_utf8(&mut arr[*len..]);
                    *len += ch_len;
                } else {
                    let mut replacement = small_str(*len, arr).to_string();
                    replacement.push(ch);
                    *self = Text(TextInner::Large(replacement));
                }
            }
            TextInner::Large(string) => {
                string.push(ch);
            }
        }
    }

    /// Append a sequence of characters to this [`Text`].
    pub fn push_str(&mut self, string: &str) {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => {
                let str_len = string.len();
                if *len + str_len <= SMALL_SIZE {
                    (&mut arr[*len..*len + str_len]).clone_from_slice(string.as_bytes());
                    *len += str_len;
                } else {
                    let mut replacement = small_str(*len, arr).to_string();
                    replacement.push_str(string);
                    *self = Text(TextInner::Large(replacement));
                }
            }
            TextInner::Large(large_string) => {
                large_string.push_str(string);
            }
        }
    }

    /// Borrow the bytes underlying this [`Text`].
    pub fn as_bytes(&self) -> &[u8] {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => &arr[..*len],
            TextInner::Large(str) => str.as_bytes(),
        }
    }

    /// Clear this instance, flipping it to the small representation if it is large.
    pub fn clear(&mut self) {
        *self = Text::default()
    }
}

impl Borrow<str> for Text {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl BorrowMut<str> for Text {
    fn borrow_mut(&mut self) -> &mut str {
        self.as_str_mut()
    }
}

impl From<String> for Text {
    fn from(string: String) -> Self {
        if string.len() <= SMALL_SIZE {
            small_from_str(string.as_str())
        } else {
            Text(TextInner::Large(string))
        }
    }
}

impl Into<String> for Text {
    fn into(self) -> String {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => small_str(len, &arr).to_string(),
            TextInner::Large(string) => string,
        }
    }
}

impl From<&str> for Text {
    fn from(string: &str) -> Self {
        if string.len() <= SMALL_SIZE {
            small_from_str(string)
        } else {
            Text(TextInner::Large(string.to_owned()))
        }
    }
}

impl From<&mut str> for Text {
    fn from(string: &mut str) -> Self {
        From::from(&*string)
    }
}

impl From<&String> for Text {
    fn from(string: &String) -> Self {
        From::from(string.as_str())
    }
}

impl From<&mut String> for Text {
    fn from(string: &mut String) -> Self {
        From::from(string.as_str())
    }
}

impl From<&Text> for Text {
    fn from(text: &Text) -> Self {
        text.clone()
    }
}

impl From<&mut Text> for Text {
    fn from(text: &mut Text) -> Self {
        (&*text).clone()
    }
}

impl From<Box<String>> for Text {
    fn from(string: Box<String>) -> Self {
        From::from(*string)
    }
}

impl From<Box<str>> for Text {
    fn from(string: Box<str>) -> Self {
        From::from(&*string)
    }
}

impl From<Box<Text>> for Text {
    fn from(boxed: Box<Text>) -> Self {
        *boxed
    }
}

impl From<char> for Text {
    fn from(ch: char) -> Self {
        let mut arr: [u8; SMALL_SIZE] = [0; SMALL_SIZE];
        let ch_len = ch.len_utf8();
        ch.encode_utf8(&mut arr[..ch_len]);
        Text(TextInner::Small(ch_len, arr))
    }
}

impl FromStr for Text {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(From::from(s))
    }
}

impl PartialEq for Text {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&Text> for Text {
    fn eq(&self, other: &&Text) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&mut Text> for Text {
    fn eq(&self, other: &&mut Text) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<str> for Text {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<Box<str>> for Text {
    fn eq(&self, other: &Box<str>) -> bool {
        self.as_str() == &**other
    }
}

impl PartialEq<Box<String>> for Text {
    fn eq(&self, other: &Box<String>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<Box<Text>> for Text {
    fn eq(&self, other: &Box<Text>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Text {}

impl PartialEq<String> for Text {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&String> for Text {
    fn eq(&self, other: &&String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&mut String> for Text {
    fn eq(&self, other: &&mut String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&str> for Text {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<&mut str> for Text {
    fn eq(&self, other: &&mut str) -> bool {
        self.as_str() == *other
    }
}

impl PartialOrd for Text {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for Text {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<str> for Text {
    fn as_mut(&mut self) -> &mut str {
        self.as_str_mut()
    }
}

impl Hash for Text {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

#[derive(Debug)]
enum TextKind {
    Small,
    Large,
}

impl Debug for Text {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => f
                .debug_tuple("Text")
                .field(&TextKind::Small)
                .field(&small_str(*len, arr))
                .finish(),
            TextInner::Large(string) => f
                .debug_tuple("Text")
                .field(&TextKind::Large)
                .field(string)
                .finish(),
        }
    }
}

impl Display for Text {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

impl Default for Text {
    fn default() -> Self {
        Text(TextInner::Small(0, [0; SMALL_SIZE]))
    }
}

impl Clone for Text {
    fn clone(&self) -> Self {
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => Text(TextInner::Small(*len, *arr)),
            TextInner::Large(string) => Text(TextInner::Large(string.clone())),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        match (&mut self.0, &source.0) {
            (TextInner::Small(len, this), TextInner::Small(len_other, other)) => {
                *len = *len_other;
                this.clone_from(other);
            }
            (TextInner::Large(this), TextInner::Large(other)) => {
                this.clone_from(other);
            }
            (ref mut ow, TextInner::Small(len_other, other)) => {
                **ow = TextInner::Small(*len_other, *other);
            }
            (ref mut ow, TextInner::Large(other)) => {
                **ow = TextInner::Large(other.clone());
            }
        }
    }
}

impl Extend<char> for Text {
    fn extend<T: IntoIterator<Item = char>>(&mut self, iter: T) {
        let it = iter.into_iter();
        let (min, max) = it.size_hint();
        let Text(inner) = self;
        match inner {
            TextInner::Small(len, arr) => {
                if *len + min > SMALL_SIZE {
                    for ch in it {
                        self.push(ch);
                    }
                } else {
                    let n = if let Some(m) = max { m } else { min };
                    let mut string = String::with_capacity(n);
                    string.push_str(small_str(*len, arr));
                    for ch in it {
                        string.push(ch);
                    }
                    *self = string.into();
                }
            }
            TextInner::Large(string) => {
                for ch in it {
                    string.push(ch);
                }
            }
        }
    }
}

impl<'a> Extend<&'a char> for Text {
    fn extend<T: IntoIterator<Item = &'a char>>(&mut self, iter: T) {
        self.extend(iter.into_iter().cloned())
    }
}

impl<'a> Extend<&'a mut char> for Text {
    fn extend<T: IntoIterator<Item = &'a mut char>>(&mut self, iter: T) {
        self.extend(iter.into_iter().map(|ch| &*ch))
    }
}

impl<'a> Extend<&'a str> for Text {
    fn extend<T: IntoIterator<Item = &'a str>>(&mut self, iter: T) {
        iter.into_iter().for_each(|s| self.push_str(s))
    }
}

impl Extend<String> for Text {
    fn extend<T: IntoIterator<Item = String>>(&mut self, iter: T) {
        iter.into_iter().for_each(|s| self.push_str(s.as_str()))
    }
}

impl<'a> Extend<&'a String> for Text {
    fn extend<T: IntoIterator<Item = &'a String>>(&mut self, iter: T) {
        iter.into_iter().for_each(|s| self.push_str(s.as_str()))
    }
}

impl Extend<Text> for Text {
    fn extend<T: IntoIterator<Item = Text>>(&mut self, iter: T) {
        iter.into_iter().for_each(|s| self.push_str(s.as_str()))
    }
}

impl<'a> Extend<&'a Text> for Text {
    fn extend<T: IntoIterator<Item = &'a Text>>(&mut self, iter: T) {
        iter.into_iter().for_each(|s| self.push_str(s.as_str()))
    }
}

impl<Element> FromIterator<Element> for Text
where
    Text: Extend<Element>,
{
    fn from_iter<T: IntoIterator<Item = Element>>(iter: T) -> Self {
        let mut text = Text::empty();
        text.extend(iter);
        text
    }
}

fn small_from_str(string: &str) -> Text {
    let len = string.len();
    let mut arr = [0; SMALL_SIZE];
    (&mut arr[..len]).copy_from_slice(string.as_bytes());
    Text(TextInner::Small(len, arr))
}

fn small_str(len: usize, small: &[u8; SMALL_SIZE]) -> &str {
    unsafe { str::from_utf8_unchecked(small_slice(len, small)) }
}

fn small_str_mut(len: usize, small: &mut [u8; SMALL_SIZE]) -> &mut str {
    unsafe { str::from_utf8_unchecked_mut(small_slice_mut(len, small)) }
}

fn small_slice(len: usize, small: &[u8; SMALL_SIZE]) -> &[u8] {
    &small[..len]
}

fn small_slice_mut(len: usize, small: &mut [u8; SMALL_SIZE]) -> &mut [u8] {
    &mut small[..len]
}

impl TryFrom<Text> for Uri {
    type Error = InvalidUri;

    fn try_from(value: Text) -> Result<Self, Self::Error> {
        match value {
            Text(TextInner::Large(string)) => Uri::try_from(string),
            Text(TextInner::Small(len, bytes)) => Uri::try_from(small_str(len, &bytes)),
        }
    }
}

impl TryFrom<Text> for RelativeUri {
    type Error = BadRelativeUri;

    fn try_from(value: Text) -> Result<Self, Self::Error> {
        match value {
            Text(TextInner::Large(string)) => RelativeUri::try_from(string),
            Text(TextInner::Small(len, bytes)) => RelativeUri::try_from(small_str(len, &bytes)),
        }
    }
}
