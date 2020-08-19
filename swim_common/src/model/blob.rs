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

use core::fmt;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::io;
use std::io::Write;

use base64::write::EncoderWriter;
use base64::{DecodeError, URL_SAFE};
use futures::io::IoSlice;
use serde::de::{Error as DeError, SeqAccess, Visitor};
use serde::ser::{Error as SerError, SerializeStructVariant};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::{Borrow, BorrowMut};

pub const EXT_BLOB: &str = "___BLOB";

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
        EncoderWriter::new(&mut writer, URL_SAFE).write_all(&self.data)
    }

    /// Consumes this BLOB and returns the decoded data.
    pub fn into_decoded(self) -> Result<Vec<u8>, DecodeError> {
        base64::decode_config(self.data, URL_SAFE)
    }

    /// Clone the underlying data and decode it.
    pub fn as_decoded(&self) -> Result<Vec<u8>, DecodeError> {
        base64::decode_config(self.data.clone(), URL_SAFE)
    }

    /// Attempts to decode the provided data. Returning a result containing either the decoded data
    /// as a [`Blob`] or a [`base64::DecodeError`].
    pub fn try_decode<T>(encoded: T) -> Result<Blob, DecodeError>
    where
        T: AsRef<[u8]>,
    {
        base64::decode_config(encoded.as_ref(), URL_SAFE).map(Blob::from_vec)
    }

    /// Encodes the provided data into a [`Blob`].
    pub fn encode<T: AsRef<[u8]>>(input: T) -> Blob {
        let encoded = base64::encode_config(input, URL_SAFE);
        Blob {
            data: Vec::from(encoded.as_bytes()),
        }
    }

    /// Creates a BLOB from pre-encoded data. Effectively providing a wrapper around base64 encoded data.
    pub fn from_encoded(data: Vec<u8>) -> Blob {
        Blob { data }
    }
}

impl Write for Blob {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.data.write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.data.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.data.write_all(buf)
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

impl Serialize for Blob {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let s = String::from_utf8(self.data.clone()).map_err(SerError::custom)?;
        serializer.serialize_str(&s)
    }
}

struct WrappedBlobVisitor;

impl<'de> Visitor<'de> for WrappedBlobVisitor {
    type Value = Blob;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "A valid base64 encoded string or byte array")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_encoded(Vec::from(v.as_bytes())))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_encoded(Vec::from(v.as_bytes())))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_encoded(Vec::from(v.as_bytes())))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_vec(v.to_owned()))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_vec(v.to_owned()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(Blob::from_vec(v))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, <A as SeqAccess<'de>>::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));

        while let Some(byte) = seq.next_element()? {
            vec.push(byte);
        }

        Ok(Blob::from_vec(vec))
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(WrappedBlobVisitor)
    }
}

pub fn serialize_blob_as_value<S>(bi: &Blob, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut r = serializer
        .serialize_struct_variant(EXT_BLOB, u32::max_value(), EXT_BLOB, usize::max_value())
        .expect("infallible");

    r.serialize_field(EXT_BLOB, &bi)?;
    r.end()
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
        let encoded = base64::encode_config("swimming", URL_SAFE);
        let decoded = base64::decode_config(encoded.as_bytes(), URL_SAFE).unwrap();
        let blob = Blob::from_encoded(Vec::from(encoded.as_bytes()));

        assert_eq!(decoded, blob.into_decoded().unwrap())
    }
}
