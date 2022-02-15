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

use swim_form::structural::read::ReadError;
use swim_model::Text;
use swim_recon::parser::AsyncParseError;
use thiserror::Error;

/// Indicates that an agent or downlink failed to read a frame from a byte stream.
#[derive(Error, Debug)]
pub enum FrameIoError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    BadFrame(#[from] InvalidFrame),
}

impl From<AsyncParseError> for FrameIoError {
    fn from(e: AsyncParseError) -> Self {
        FrameIoError::BadFrame(InvalidFrame::InvalidMessageBody(e))
    }
}

/// Indicates that the content of a frame was invalid.
#[derive(Error, Debug)]
pub enum InvalidFrame {
    #[error("An incoming frame was incomplete.")]
    Incomplete,
    #[error("Invalid frame header: {problem}")]
    InvalidHeader { problem: Text },
    #[error("Invalid frame body: {0}")]
    InvalidMessageBody(#[from] AsyncParseError),
}

/// Possible failure modes for a downlink consumer.
#[derive(Error, Debug)]
pub enum DownlinkTaskError {
    #[error("The downlink failed to start.")]
    FailedToStart,
    #[error("A synced envelope was received with no data provided.")]
    SyncedWithNoValue,
    #[error("{0}")]
    BadFrame(#[from] FrameIoError),
    #[error("Failed to deserialize frame body: {0}")]
    DeserializationFailed(#[from] ReadError),
}
