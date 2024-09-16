// Copyright 2015-2024 Swim Inc.
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

use std::num::ParseIntError;

use crate::deserialization::DeserializationError;
use thiserror::Error;

/// Errors that can be produced attempting to select components of a message.
#[derive(Debug, Error)]
pub enum LaneSelectorError {
    /// A selector failed to provide a value for a required lane.
    #[error("The lane '{0}' is required but did not occur in a message.")]
    MissingRequiredLane(String),
    /// Deserializing a component of a message failed.
    #[error("Deserializing the content of a message failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}

/// Error type for an invalid lane selector specification.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum InvalidLaneSpec {
    /// The string describing the selector was invalid.
    #[error(transparent)]
    Selector(#[from] BadSelector),
    /// The lane name could not be inferred from the selector and was not provided explicitly.
    #[error("No name provided and it cannot be inferred from the selector.")]
    NameCannotBeInferred,
}

/// Error type produced for invalid lane descriptors.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum InvalidLanes {
    /// The specification of a line as not valid
    #[error(transparent)]
    Spec(#[from] InvalidLaneSpec),
    /// A connector has too many lanes.
    #[error("The connector has {0} lanes which cannot fit in a u32.")]
    TooManyLanes(usize),
    /// There are lane descriptors with the same name.
    #[error("The lane name {0} occurs more than once.")]
    NameCollision(String),
}

/// Error type for an invalid selector descriptor.
#[derive(Clone, Copy, Error, Debug, PartialEq, Eq)]
pub enum BadSelector {
    /// An empty string does not describe a valid selector.
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    /// A selector component pay not be empty.
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    /// The root of a selector must be a valid component of a message.
    #[error("Invalid root selector (must be one of '$key' or '$payload' with an optional index or '$topic')."
    )]
    InvalidRoot,
    /// A component of the descriptor did not describe a valid selector.
    #[error(
        "Invalid component selector (must be an attribute or slot name with an optional index)."
    )]
    InvalidComponent,
    /// The index for an index selector was too large for usize.
    #[error("An index specified was not a valid usize.")]
    IndexOutOfRange,
    /// The topic root cannot have any other components.
    #[error("The topic does not have components.")]
    TopicWithComponent,
}

/// An error type that can be produced when attempting to load a deserializer.
#[derive(Debug, Error)]
pub enum DerserializerLoadError {
    /// Attempting to read a required resource (for example, a file) failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A required resource was invalid.
    #[error(transparent)]
    InvalidDescriptor(#[from] Box<dyn std::error::Error + Send + 'static>),
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}
