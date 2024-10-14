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

use crate::DeserializationError;
use std::num::ParseIntError;
use swimos_recon::parser::ParseError;
use thiserror::Error;

/// Errors that can be produced attempting to select components of a Message.
#[derive(Debug, Error)]
pub enum SelectorError {
    /// A selector failed to provide a value for a required lane.
    #[error("The lane '{0}' is required but did not occur in a message.")]
    MissingRequiredLane(String),
    /// Deserializing a component of a message failed.
    #[error("Deserializing the content of a message failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
    #[error("Selector `{0}` failed to select from input")]
    Selector(String),
    /// A node or lane selector failed to select from a record as it was not a primitive type.
    #[error("Invalid record structure. Expected a primitive type")]
    InvalidRecord(String),
}

/// Error type for an invalid lane selector specification.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
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
    /// There are lane descriptors with the same name.
    #[error("The lane name {0} occurs more than once.")]
    NameCollision(String),
}

/// Error type for an invalid selector descriptor.
#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum BadSelector {
    /// An empty string does not describe a valid selector.
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    /// A selector component may not be empty.
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    /// The root of a selector must be a valid component of a message.
    #[error("Invalid root selector (must be one of '$key' or '$payload' with an optional index or '$topic').")]
    InvalidRoot,
    /// Not all pub-sub connectors have messages containing all of the selector roots.
    #[error("The connector does not support the specified selector root.")]
    UnsupportedRoot,
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
    /// A provided lane/node/payload selector was malformed.
    #[error("The selector formed an invalid path.")]
    InvalidPath,
    /// Invalid Recon was specified for a payload selector.
    #[error("Failed to parse Recon value for selector payload")]
    InvalidRecon(#[from] ParseError),
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}
