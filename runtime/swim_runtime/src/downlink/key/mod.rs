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

use bytes::Bytes;
use std::hash::{Hash, Hasher};
use swim_recon::comparator;

#[derive(Debug, Clone, Eq)]
pub struct ReconKey {
    content: Bytes,
}

impl From<String> for ReconKey {
    fn from(content: String) -> Self {
        ReconKey {
            content: Bytes::from(content.into_bytes()),
        }
    }
}

impl From<&str> for ReconKey {
    fn from(text: &str) -> Self {
        text.to_string().into()
    }
}

impl TryFrom<&[u8]> for ReconKey {
    type Error = std::str::Utf8Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let text = std::str::from_utf8(value)?;
        Ok(text.into())
    }
}

impl TryFrom<Bytes> for ReconKey {
    type Error = std::str::Utf8Error;

    fn try_from(content: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(content.as_ref())?;
        Ok(ReconKey { content })
    }
}

impl AsRef<str> for ReconKey {
    fn as_ref(&self) -> &str {
        // Safe as we only all a key to be constructed from bytes containg valid UTF8.
        unsafe { std::str::from_utf8_unchecked(self.content.as_ref()) }
    }
}

impl PartialEq for ReconKey {
    fn eq(&self, other: &Self) -> bool {
        comparator::compare_values(self.as_ref(), other.as_ref())
    }
}

impl Hash for ReconKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //TODO Use the Recon hasher.
        self.content.hash(state)
    }
}

impl ReconKey {
    pub fn into_bytes(self) -> Bytes {
        self.content
    }
}
