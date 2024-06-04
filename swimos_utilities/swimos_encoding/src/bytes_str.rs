// Copyright 2015-2023 Swim Inc.
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

use std::fmt::{Debug, Display, Formatter};

use bytes::Bytes;

/// As string based on a sub-range within a [`Bytes`] buffer. The bytes are guaranteed to contain valid
/// UTF-8. The purpose of this type is to allow a [`tokio_util::codec::Decoder`]s to return a string from
/// its input buffer without performing a copy or an allocation and without a lifetime dependency on the
/// buffer.
#[derive(Default, PartialEq, Eq, Clone)]
pub struct BytesStr(Bytes);

impl BytesStr {
    /// Attempt to wrap a buffer as a bytes string. If the buffer does not contain only valid UTF-8, the
    /// raw buffer is returned instead.
    pub fn try_wrap(bytes: Bytes) -> Result<Self, Bytes> {
        if std::str::from_utf8(bytes.as_ref()).is_ok() {
            Ok(BytesStr(bytes))
        } else {
            Err(bytes)
        }
    }

    /// Create a bytes string from a static string (which is guaranteed to be valid).
    pub const fn from_static_str(content: &'static str) -> BytesStr {
        BytesStr(Bytes::from_static(content.as_bytes()))
    }
}

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

impl From<BytesStr> for Bytes {
    fn from(value: BytesStr) -> Self {
        value.0
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

/// Trait for types that can be created from a buffer that potentially contains UTF-8. This duplicates
/// the standard library [`TryFrom`] but allows us to implement it for types we do not control.
pub trait TryFromUtf8Bytes: Sized {
    fn try_from_utf8_bytes(bytes: Bytes) -> Result<Self, std::str::Utf8Error>;
}

impl TryFromUtf8Bytes for BytesStr {
    fn try_from_utf8_bytes(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        BytesStr::try_from(bytes)
    }
}

impl TryFromUtf8Bytes for String {
    fn try_from_utf8_bytes(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        Ok(std::str::from_utf8(bytes.as_ref())?.to_string())
    }
}
