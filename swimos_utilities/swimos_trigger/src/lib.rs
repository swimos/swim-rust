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

//! # Asynchronous Trigger
//!
//! This crate provides a [SPMC barrier](trigger::trigger) where any number of parties can wait for
//! a single other party to complete an action.
//!
//! Additionally, a [promise](promise::promise) implementation is provided, implemented in terms of
//! the trigger, that allows any number of parties to wait for a single producer to generate a value.

pub mod promise;
mod trigger;

pub use trigger::{trigger, Receiver, Sender, TriggerError};
