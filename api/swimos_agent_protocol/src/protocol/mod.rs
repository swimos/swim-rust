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

use bytes::{Buf, BufMut, BytesMut};
use swimos_form::structural::{read::recognizer::Recognizer, write::StructuralWritable};
use swimos_recon::{
    parser::{AsyncParseError, RecognizerDecoder},
    write_recon,
};
use tokio_util::codec::{Decoder, Encoder};

pub mod agent;
pub mod downlink;
pub mod map;
#[cfg(test)]
mod tests;

type DecoderResult<D> = Result<Option<<D as Decoder>::Item>, <D as Decoder>::Error>;

fn consume_bounded<D: Decoder>(
    remaining: &mut usize,
    src: &mut BytesMut,
    decoder: &mut D,
) -> (usize, BytesMut, DecoderResult<D>) {
    let to_split = (*remaining).min(src.remaining());
    let rem = src.split_off(to_split);
    let buf_remaining = src.remaining();
    let end_of_message = *remaining <= buf_remaining;
    let decode_result = if end_of_message {
        decoder.decode_eof(src)
    } else {
        decoder.decode(src)
    };
    let new_remaining = src.remaining();
    let consumed = buf_remaining - new_remaining;
    *remaining -= consumed;
    (new_remaining, rem, decode_result)
}

const TAG_SIZE: usize = std::mem::size_of::<u8>();
const LEN_SIZE: usize = std::mem::size_of::<u64>();

/// Encodes a value as a Recon string following the length of the string as a 64 bit integer.
#[derive(Debug, Clone, Copy, Default)]
pub struct WithLenReconEncoder;

impl<T: StructuralWritable> Encoder<T> for WithLenReconEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write_recon_with_len(dst, &item);
        Ok(())
    }
}

fn write_recon_with_len<T: StructuralWritable>(dst: &mut BytesMut, body: &T) {
    dst.reserve(LEN_SIZE);
    let body_len_offset = dst.remaining();
    dst.put_u64(0);
    let body_len = write_recon(dst, body);
    let mut rewound = &mut dst.as_mut()[body_len_offset..];
    rewound.put_u64(body_len as u64);
}

enum WithLenRecognizerDecoderState<R: Recognizer> {
    ReadingHeader,
    ReadingBody {
        remaining: usize,
    },
    AfterBody {
        remaining: usize,
        value: Option<R::Target>,
    },
    Discarding {
        remaining: usize,
        error: Option<AsyncParseError>,
    },
}

/// Length delimited wrapper around [`RecognizerDecoder`]. This decoder expects the length of the string
/// to be consumed by the inner decoder to be written to the buffer as an unsigned, 64bit integer.
/// The inner reader will not be permitted to read beyond the written length.
pub struct WithLenRecognizerDecoder<R: Recognizer> {
    inner: RecognizerDecoder<R>,
    state: WithLenRecognizerDecoderState<R>,
}

impl<R: Recognizer> WithLenRecognizerDecoder<R> {
    pub fn new(recognizer: R) -> Self {
        WithLenRecognizerDecoder {
            inner: RecognizerDecoder::new(recognizer),
            state: WithLenRecognizerDecoderState::ReadingHeader,
        }
    }
}

const BODY_LEN: usize = std::mem::size_of::<u64>();

impl<R: Recognizer> Decoder for WithLenRecognizerDecoder<R> {
    type Item = R::Target;

    type Error = AsyncParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let WithLenRecognizerDecoder { inner, state } = self;
        loop {
            match state {
                WithLenRecognizerDecoderState::ReadingHeader => {
                    if src.remaining() < BODY_LEN {
                        src.reserve(BODY_LEN);
                        break Ok(None);
                    } else {
                        *state = WithLenRecognizerDecoderState::ReadingBody {
                            remaining: src.get_u64() as usize,
                        };
                    }
                }
                WithLenRecognizerDecoderState::ReadingBody { remaining } => {
                    let (new_remaining, rem, decode_result) =
                        consume_bounded(remaining, src, inner);
                    match decode_result {
                        Ok(Some(result)) => {
                            src.unsplit(rem);
                            *state = WithLenRecognizerDecoderState::AfterBody {
                                value: Some(result),
                                remaining: *remaining,
                            }
                        }
                        Ok(None) => {
                            break Ok(None);
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                *state = WithLenRecognizerDecoderState::ReadingHeader;
                                break Err(e);
                            } else {
                                *state = WithLenRecognizerDecoderState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                WithLenRecognizerDecoderState::AfterBody { remaining, value } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = value.take();
                        *state = WithLenRecognizerDecoderState::ReadingHeader;
                        break Ok(result);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                WithLenRecognizerDecoderState::Discarding { remaining, error } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = WithLenRecognizerDecoderState::ReadingHeader;
                        break Err(err);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
            }
        }
    }
}
