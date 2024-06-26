// Copyright 2015-2024 Swim Inc.
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

use base64::{engine::general_purpose::URL_SAFE, write::EncoderWriter, DecodeError, Engine};
use core::fmt;
use std::borrow::{Borrow, BorrowMut};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::io;
use std::io::Write;

/// A Binary Large OBject (BLOB) structure for encoding and decoding base-64 data. A URL-safe
/// encoding (UTF-7) is used.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blob {
    data: Vec<u8>,
}

impl Blob {
    /// Construct a new blob object of the provided capacity.
    pub fn from_vec(data: Vec<u8>) -> Blob {
        Blob { data }
    }

    /// Consumes this blob object and returns the underlying data.
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    /// Attempts to encode this blob's data into the provided writer.
    pub fn encode_to_writer<W: Write>(&self, mut writer: W) -> io::Result<()> {
        EncoderWriter::new(&mut writer, &URL_SAFE).write_all(&self.data)
    }

    /// Consumes this BLOB and returns the decoded data.
    pub fn into_decoded(self) -> Result<Vec<u8>, DecodeError> {
        URL_SAFE.decode(self.data)
    }

    /// Clone the underlying data and decode it.
    pub fn as_decoded(&self) -> Result<Vec<u8>, DecodeError> {
        URL_SAFE.decode(self.data.clone())
    }

    /// Attempts to decode the provided data. Returning a result containing either the decoded data
    /// as a [`Blob`] or a [`base64::DecodeError`].
    pub fn try_decode<T>(encoded: T) -> Result<Blob, DecodeError>
    where
        T: AsRef<[u8]>,
    {
        URL_SAFE.decode(encoded).map(Blob::from_vec)
    }

    /// Encodes the provided data into a [`Blob`].
    pub fn encode<T: AsRef<[u8]>>(input: T) -> Blob {
        let encoded = URL_SAFE.encode(input);
        Blob {
            data: Vec::from(encoded.as_bytes()),
        }
    }

    /// Creates a BLOB from pre-encoded data. Effectively providing a wrapper around base64 encoded data.
    pub fn from_encoded(data: Vec<u8>) -> Blob {
        Blob { data }
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = String::from_utf8(self.data.clone()).map_err(|_| fmt::Error)?;
        write!(f, "{}", s)
    }
}

impl Borrow<Vec<u8>> for Blob {
    fn borrow(&self) -> &Vec<u8> {
        &self.data
    }
}

impl BorrowMut<Vec<u8>> for Blob {
    fn borrow_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str() {
        let blob = Blob::encode("swimming");
        assert_eq!(blob.to_string(), "c3dpbW1pbmc=".to_string());
    }

    #[test]
    fn from_encoded() {
        let encoded = URL_SAFE.encode("swimming");
        let decoded = URL_SAFE.decode(encoded.as_bytes()).unwrap();
        let blob = Blob::from_encoded(Vec::from(encoded.as_bytes()));

        assert_eq!(decoded, blob.into_decoded().unwrap())
    }
}
