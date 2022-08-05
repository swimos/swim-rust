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

use bytes::{BufMut, BytesMut};
use swim_api::protocol::map::RawMapOperation;
use tokio_util::codec::Encoder;

#[cfg(test)]
mod tests;

const CLEAR: &[u8] = b"@clear";
const UPDATE: &[u8] = b"@update(key:) ";
const REMOVE: &[u8] = b"@remove(key:)";
const KEY_OFFSET: usize = 12;

#[derive(Debug, Default)]
pub struct MapOperationReconEncoder;

impl Encoder<RawMapOperation> for MapOperationReconEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RawMapOperation, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RawMapOperation::Update { key, value } => {
                dst.reserve(UPDATE.len() + key.len() + value.len());
                dst.put(&UPDATE[..KEY_OFFSET]);
                dst.put(key);
                dst.put(&UPDATE[KEY_OFFSET..]);
                dst.put(value);
            }
            RawMapOperation::Remove { key } => {
                dst.reserve(REMOVE.len() + key.len());
                dst.put(&REMOVE[..KEY_OFFSET]);
                dst.put(key);
                dst.put(&REMOVE[KEY_OFFSET..]);
            }
            RawMapOperation::Clear => {
                dst.reserve(CLEAR.len());
                dst.put(CLEAR);
            }
        }
        Ok(())
    }
}
