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

//! # Error Accumulation Framework
//!
//! This crate provides [`Validation`], an alternative to the standard library [`Result`] type.
//! A function that returns a [`Result`] may succeed or fail with a single error. In contrast,
//! [`Validation`] is used for processes that can produce an numbers of errors that do no
//! necessarily cause processing to terminate. A function that returns a [`Validation`] may
//! succeed (returning a value and no error), return a value and an error (indicating that
//! processing may continue despite the error) or return an unconditional error (indicating that
//! processing cannot continue).
//!
//! Validations are composed as the process continues, therefore the error type must be composable
//! that errors can be accumulated. Typically, the error type of a [`Validation`] will be a
//! collection of error values and the composition operation will take the union of the two collections.

mod accumulator;
mod recoverable;
mod validation;

pub use accumulator::Errors;
pub use recoverable::Recoverable;
pub use validation::{validate2, validate3, Append, Validation, ValidationItExt};
