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

use std::convert::Infallible;

use bytes::{BufMut, Bytes, BytesMut};
use swim_api::protocol::map::{extract_header, MapMessageEncoder, RawMapOperationEncoder};
use swim_recon::parser::MessageExtractError;
use tokio_util::codec::Encoder;

/// A possible transformation to apply to an incoming event body, before passing it on
/// to the downlink implementation.
pub trait DownlinkInterpretation {
    type Error;

    /// Whether the state of the downlink can always be determined by the contents of a single envelope.
    /// For example, this would be false for map downlinks as maps could require any number of
    /// envelopes to determine their state.
    const SINGLE_FRAME_STATE: bool = true;

    /// Interpret the body held in the incoming buffer and write the appropriately transformed
    /// data into the buffer.
    fn interpret_frame_data(
        &mut self,
        frame: Bytes,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error>;
}

pub struct FnMutInterpretation<F>(F);

impl<F, E> DownlinkInterpretation for FnMutInterpretation<F>
where
    F: FnMut(Bytes, &mut BytesMut) -> Result<(), E>,
{
    type Error = E;

    fn interpret_frame_data(
        &mut self,
        frame: Bytes,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.0(frame, buffer)
    }
}

/// Trivial interpretation that simply copies between the buffers (used for value-like downlinks).
fn trivial_interpretation(frame: Bytes, buffer: &mut BytesMut) -> Result<(), Infallible> {
    buffer.reserve(frame.len());
    buffer.put(frame);
    Ok(())
}

/// For value downlinks, simply copy between the buffers.
pub fn value_interpretation() -> impl DownlinkInterpretation<Error = Infallible> {
    FnMutInterpretation(trivial_interpretation)
}

/// Interpretation for map downlinks that attempts to extract key and value information
/// from the event.
#[derive(Debug, Default)]
pub struct MapInterpretation {
    encoder: MapMessageEncoder<RawMapOperationEncoder>,
}

impl DownlinkInterpretation for MapInterpretation {
    type Error = MessageExtractError;

    const SINGLE_FRAME_STATE: bool = false;

    fn interpret_frame_data(
        &mut self,
        frame: Bytes,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let MapInterpretation { encoder } = self;
        let header = extract_header(&frame)?;
        encoder
            .encode(header, buffer)
            .expect("Encoding a raw message into a BytesMut is infallible.");
        Ok(())
    }
}

pub struct NoInterpretation;
impl DownlinkInterpretation for NoInterpretation {
    type Error = Infallible;

    const SINGLE_FRAME_STATE: bool = false;

    fn interpret_frame_data(
        &mut self,
        frame: Bytes,
        buffer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        buffer.extend(frame);
        Ok(())
    }
}
