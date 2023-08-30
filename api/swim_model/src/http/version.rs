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

use bytes::{Buf, BufMut, BytesMut};
use lazy_static::lazy_static;
use std::{collections::HashMap, fmt::Formatter};
use thiserror::Error;

/// An HTTP version number. (At this time, Swim only supports HTTP 1.1).
#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Version(VersionInner);

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum VersionInner {
    V0_9,
    V1_0,
    #[default]
    V1_1,
    V2_0,
    V3_0,
}

impl Version {
    pub const HTTP_0_9: Version = Version(VersionInner::V0_9);
    pub const HTTP_1_0: Version = Version(VersionInner::V1_0);
    pub const HTTP_1_1: Version = Version(VersionInner::V1_1);
    pub const HTTP_2_0: Version = Version(VersionInner::V2_0);
    pub const HTTP_3_0: Version = Version(VersionInner::V3_0);

    pub fn major_version(&self) -> u8 {
        match self.0 {
            VersionInner::V0_9 => 0,
            VersionInner::V1_0 => 1,
            VersionInner::V1_1 => 1,
            VersionInner::V2_0 => 2,
            VersionInner::V3_0 => 3,
        }
    }

    pub fn minor_version(&self) -> u8 {
        match self.0 {
            VersionInner::V0_9 => 9,
            VersionInner::V1_0 => 0,
            VersionInner::V1_1 => 1,
            VersionInner::V2_0 => 0,
            VersionInner::V3_0 => 0,
        }
    }
}

impl From<Version> for http::Version {
    fn from(value: Version) -> Self {
        match value.0 {
            VersionInner::V0_9 => http::Version::HTTP_09,
            VersionInner::V1_0 => http::Version::HTTP_10,
            VersionInner::V1_1 => http::Version::HTTP_11,
            VersionInner::V2_0 => http::Version::HTTP_2,
            VersionInner::V3_0 => http::Version::HTTP_3,
        }
    }
}

lazy_static! {
    static ref VERSIONS: HashMap<http::Version, Version> = {
        let mut m = HashMap::new();
        m.insert(http::Version::HTTP_09, Version::HTTP_0_9);
        m.insert(http::Version::HTTP_10, Version::HTTP_1_0);
        m.insert(http::Version::HTTP_11, Version::HTTP_1_1);
        m.insert(http::Version::HTTP_2, Version::HTTP_2_0);
        m.insert(http::Version::HTTP_3, Version::HTTP_3_0);
        m
    };
}

impl TryFrom<http::Version> for Version {
    type Error = UnsupportedVersion;

    fn try_from(value: http::Version) -> Result<Self, Self::Error> {
        VERSIONS
            .get(&value)
            .copied()
            .ok_or_else(|| UnsupportedVersion(format!("{:?}", value)))
    }
}

#[derive(Debug, Error)]
#[error("HTTP version '{0}' is not supported.")]
pub struct UnsupportedVersion(String);

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            VersionInner::V0_9 => write!(f, "HTTP/0.9"),
            VersionInner::V1_0 => write!(f, "HTTP/1.0"),
            VersionInner::V1_1 => write!(f, "HTTP/1.1"),
            VersionInner::V2_0 => write!(f, "HTTP/2"),
            VersionInner::V3_0 => write!(f, "HTTP/3"),
        }
    }
}

impl Version {
    pub const ENCODED_LENGTH: usize = 1;
}

impl Version {
    pub fn encode(&self, dst: &mut BytesMut) {
        let code = match &self.0 {
            VersionInner::V0_9 => 0,
            VersionInner::V1_0 => 1,
            VersionInner::V1_1 => 2,
            VersionInner::V2_0 => 3,
            VersionInner::V3_0 => 4,
        };
        dst.put_u8(code);
    }

    pub fn decode(src: &mut impl Buf) -> Result<Option<Self>, VersionDecodeError> {
        if src.remaining() >= Self::ENCODED_LENGTH {
            match src.get_u8() {
                0 => Ok(Some(Version::HTTP_0_9)),
                1 => Ok(Some(Version::HTTP_1_0)),
                2 => Ok(Some(Version::HTTP_1_1)),
                3 => Ok(Some(Version::HTTP_2_0)),
                4 => Ok(Some(Version::HTTP_3_0)),
                ow => Err(VersionDecodeError(ow)),
            }
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Error)]
#[error("{0} does not encode a HTTP version.")]
pub struct VersionDecodeError(u8);

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::Version;

    fn roundtrip_version(version: Version) {
        let mut buffer = BytesMut::new();
        version.encode(&mut buffer);

        let restored = Version::decode(&mut buffer)
            .expect("Decoding failed.")
            .expect("Incomplete.");
        assert_eq!(restored, version);
        assert!(buffer.is_empty());
    }

    #[test]
    fn version_encoding() {
        roundtrip_version(Version::HTTP_0_9);
        roundtrip_version(Version::HTTP_1_0);
        roundtrip_version(Version::HTTP_1_1);
        roundtrip_version(Version::HTTP_2_0);
        roundtrip_version(Version::HTTP_3_0);
    }

    #[test]
    fn invalid_methods() {
        let mut buffer = BytesMut::new();
        assert!(matches!(Version::decode(&mut buffer), Ok(None)));

        buffer.put_u8(u8::MAX);
        assert!(matches!(Version::decode(&mut buffer), Err(_)));
    }
}
