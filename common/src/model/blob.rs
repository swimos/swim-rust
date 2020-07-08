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
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::str::FromStr;

use base64::display::Base64Display;
use base64::write::EncoderWriter;
use base64::{Config, DecodeError, URL_SAFE};
use futures::io::IoSlice;
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A Binary Large OBject (BLOB) structure for encoding and decoding base-64 data. By default, a
/// URL-safe encoding (UTF-7) is used but an alternative configuration (provided by the base64 crate) object
/// can be provided for an alternative encoding and decoding strategy.
#[derive(Debug, Clone)]
pub struct Blob {
    data: Vec<u8>,
    config: Config,
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Default for Blob {
    fn default() -> Blob {
        Blob {
            data: Vec::new(),
            config: URL_SAFE,
        }
    }
}

impl Blob {
    /// Construct a new blob object using the provided configuration object for encoding and decoding.
    pub fn new(config: Config) -> Blob {
        Blob {
            data: Vec::new(),
            config,
        }
    }

    /// Construct a new blob object of the provided capacity using the provided configuration
    /// object for encoding and decoding.
    pub fn with_capacity(cap: usize, config: Config) -> Blob {
        Blob {
            data: Vec::with_capacity(cap),
            config,
        }
    }

    /// Construct a new blob object of the provided capacity using the provided configuration
    /// object for encoding and decoding.
    pub fn from_vec(data: Vec<u8>, config: Config) -> Blob {
        Blob { data, config }
    }

    /// Consumes this blob object and returns the underlying data.
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    /// Returns the size of the contained data.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Reserve `n` more bytes in the underlying vector.
    pub fn reserve(&mut self, n: usize) {
        self.data.reserve(n);
    }

    /// Attempts to encode this blob's data into the provided writer.
    pub fn try_encode<W: Write>(&self, mut writer: W) -> io::Result<()> {
        EncoderWriter::new(&mut writer, self.config).write_all(&self.data)
    }

    /// Attempts to decode the provided data using the configuration provided. Returning a result
    /// containing either the decoded data as a [`Blob`] or a [`base64::DecodeError`].
    pub fn try_decode<T>(encoded: T, config: Config) -> Result<Blob, DecodeError>
    where
        T: AsRef<[u8]>,
    {
        base64::decode_config(encoded.as_ref(), config).map(|b| Blob::from_vec(b, config))
    }
}

impl FromStr for Blob {
    type Err = DecodeError;

    /// Attempts to decode the provided str using URL-safe characters.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Blob::try_decode(s, URL_SAFE)
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

impl Hash for Blob {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state)
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Base64Display::with_config(&self.data, self.config).fmt(f)
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
        serializer.serialize_str(&base64::encode_config(&self.data, self.config))
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
        E: Error,
    {
        Blob::from_str(v).map_err(E::custom)
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Blob::from_str(v).map_err(E::custom)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Blob::from_str(&v).map_err(E::custom)
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Blob::from_vec(v.to_owned(), URL_SAFE))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Blob::from_vec(v.to_owned(), URL_SAFE))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Blob::from_vec(v.to_owned(), URL_SAFE))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, <A as SeqAccess<'de>>::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));

        while let Some(byte) = seq.next_element()? {
            vec.push(byte);
        }

        Ok(Blob::from_vec(vec, URL_SAFE))
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
    unimplemented!()
}

pub fn deserialize_value_to_blob<'de, D>(deserializer: D) -> Result<Blob, D::Error>
where
    D: Deserializer<'de>,
{
    unimplemented!()
}
