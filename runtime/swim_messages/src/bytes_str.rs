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

use std::fmt::{Formatter, Display, Debug};

use bytes::Bytes;

#[derive(Default, PartialEq, Eq, Clone)]
pub struct BytesStr(Bytes);

impl TryFrom<Bytes> for BytesStr {
    type Error = std::str::Utf8Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(value.as_ref())?;
        Ok(BytesStr(value))
    }
}

impl AsRef<str> for BytesStr {
    fn as_ref(&self) -> &str {
        //A BytesStr can only be constructed through means that guarantee it is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

impl Debug for BytesStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BytesStr").field(&self.as_ref()).finish()
    }
}

impl Display for BytesStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl From<String> for BytesStr {
    fn from(string: String) -> Self {
        BytesStr(Bytes::from(string))
    }
}

impl From<&str> for BytesStr {
    fn from(string: &str) -> Self {
        BytesStr::from(string.to_string())
    }
}

impl BytesStr {

    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

}

impl PartialEq<str> for BytesStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<String> for BytesStr {
    fn eq(&self, other: &String) -> bool {
        self.as_ref() == other
    }
}
