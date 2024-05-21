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

//! MessagePack support for Swim serialization.
//!
//! Provides a MessagesPack backend for the Swim serialization system. This consists of two parts:
//!
//! - A function [`read_from_msg_pack`] that will attempt to deserialize any type that implements
//!  [`swimos_form::structural::read::StructuralReadable`] from a buffer containing MessagePack data.
//! - The type [`MsgPackInterpreter`] that implements [`swimos_form::structural::write::StructuralWriter`]
//! allowing any type that implements [`swimos_form::structural::write::StructuralWritable`] to be
//! serialized as MessagePack.
//!
//! # Examples
//!
//! ```
//! use bytes::{BufMut, BytesMut};
//! use swimos_form::structural::write::StructuralWritable;
//! use swimos_msgpack::{read_from_msg_pack, MsgPackInterpreter};
//!
//! let mut buffer = BytesMut::with_capacity(128);
//! let data = vec!["first".to_owned(), "second".to_owned(), "third".to_owned()];
//! let mut writer = (&mut buffer).writer();
//!
//! let interpreter = MsgPackInterpreter::new(&mut writer);
//! assert!(data.write_with(interpreter).is_ok());
//!
//! let mut bytes = buffer.split().freeze();
//! let restored = read_from_msg_pack::<Vec<String>, _>(&mut bytes);
//!
//! assert_eq!(restored, Ok(data));
//! ```

mod reader;
#[cfg(test)]
mod tests;
mod writer;

pub use reader::{read_from_msg_pack, MsgPackReadError};
pub use writer::{MsgPackInterpreter, MsgPackWriteError};

const BIG_INT_EXT: i8 = 0;
const BIG_UINT_EXT: i8 = 1;
