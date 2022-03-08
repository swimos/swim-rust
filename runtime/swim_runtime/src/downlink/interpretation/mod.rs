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

use std::convert::Infallible;

use bytes::{BufMut, Bytes, BytesMut};
use swim_api::protocol::map::{extract_header, MapMessageEncoder, RawMapOperationEncoder};
use swim_recon::parser::MessageExtractError;
use tokio_util::codec::Encoder;

pub trait DownlinkInterpretation {
    type Error;

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

pub fn trivial_interpretation(frame: Bytes, buffer: &mut BytesMut) -> Result<(), Infallible> {
    buffer.reserve(frame.len());
    buffer.put(frame);
    Ok(())
}

pub fn value_interpretation() -> impl DownlinkInterpretation<Error = Infallible> {
    FnMutInterpretation(trivial_interpretation)
}

#[derive(Debug, Default)]
pub struct MapInterpretation {
    encoder: MapMessageEncoder<RawMapOperationEncoder>,
}

impl DownlinkInterpretation for MapInterpretation {
    type Error = MessageExtractError;

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
