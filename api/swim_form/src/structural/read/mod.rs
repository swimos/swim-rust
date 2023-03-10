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

pub mod event;
pub mod from_model;
pub mod recognizer;

use std::borrow::Cow;
use swim_model::bigint::{BigInt, BigUint};

pub mod error;

use crate::structural::read::event::ReadEvent;
use crate::structural::read::recognizer::{Recognizer, RecognizerReadable};
pub use error::ReadError;

#[doc(hidden)]
pub use swim_form_derive::StructuralReadable;

/// Trait for types that can be structurally deserialized, from the Swim data model.
pub trait StructuralReadable: RecognizerReadable {
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
