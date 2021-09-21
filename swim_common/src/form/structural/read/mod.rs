// Copyright 2015-2021 SWIM.AI inc.
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

pub mod event;
pub mod materializers;
pub mod msgpack;
pub mod parser;
pub mod recognizer;

use crate::form::structural::write::StructuralWritable;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;

pub mod error;

use crate::form::structural::bridge::RecognizerBridge;
use crate::form::structural::read::event::ReadEvent;
use crate::form::structural::read::recognizer::{Recognizer, RecognizerReadable};
pub use error::ReadError;

#[doc(hidden)]
pub use form_derive::StructuralReadable;

/// Trait for types that can be structurally deserialized, from the Swim data model.
pub trait StructuralReadable: RecognizerReadable {
    /// Attempt to write a value of a ['StructuralWritable'] type into an instance of this type.
    fn try_read_from<T: StructuralWritable>(writable: &T) -> Result<Self, ReadError> {
        let bridge = RecognizerBridge::new(Self::make_recognizer());
        writable.write_with(bridge)
    }

    /// Attempt to transform a value of a ['StructuralWritable'] type into an instance of this type.
    fn try_transform<T: StructuralWritable>(writable: T) -> Result<Self, ReadError> {
        let bridge = RecognizerBridge::new(Self::make_recognizer());
        writable.write_into(bridge)
    }

    fn read_extant() -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(ReadEvent::Extant)
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_i32(value: i32) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_i64(value: i64) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_u32(value: u32) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_u64(value: u64) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_f64(value: f64) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_bool(value: bool) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_big_int(value: BigInt) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_big_uint(value: BigUint) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        let mut rec = Self::make_recognizer();
        rec.feed_event(value.into())
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
}

impl<T> StructuralReadable for T where T: RecognizerReadable {}