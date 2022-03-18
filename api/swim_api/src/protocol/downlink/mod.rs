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

use bytes::{Buf, BufMut, Bytes};
use swim_form::structural::{read::recognizer::Recognizer, write::StructuralWritable};
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, RecognizerDecoder};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{FrameIoError, InvalidFrame};

#[cfg(test)]
mod tests;

/// Message type for communication from the runtime to a downlink subscriber.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DownlinkNotification<T> {
    Linked,
    Synced,
    Event { body: T },
    Unlinked,
}

/// Message type for communication from a downlink subscriber to the runtime.
#[derive(Debug, PartialEq, Eq)]
pub struct DownlinkOperation<T> {
    pub body: T,
}

impl<T> DownlinkOperation<T> {
    pub fn new(body: T) -> Self {
        DownlinkOperation { body }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DownlinkNotificationEncoder;

const LINKED: u8 = 1;
const SYNCED: u8 = 2;
const EVENT: u8 = 3;
const UNLINKED: u8 = 4;

use super::{LEN_SIZE, TAG_SIZE};

impl<T: AsRef<[u8]>> Encoder<DownlinkNotification<T>> for DownlinkNotificationEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: DownlinkNotification<T>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            DownlinkNotification::Linked => {
                dst.reserve(TAG_SIZE);
                dst.put_u8(LINKED);
            }
            DownlinkNotification::Synced => {
                dst.reserve(TAG_SIZE);
                dst.put_u8(SYNCED);
            }
            DownlinkNotification::Event { body } => {
                let body_bytes = body.as_ref();
                let body_len = body_bytes.len();
                dst.reserve(TAG_SIZE + LEN_SIZE + body_len);
                dst.put_u8(EVENT);
                dst.put_u64(body_len as u64);
                dst.put(body_bytes);
            }
            DownlinkNotification::Unlinked => {
                dst.reserve(TAG_SIZE);
                dst.put_u8(UNLINKED);
            }
        }
        Ok(())
    }
}
pub enum DownlinkNotifiationDecoderState<T> {
    ReadingHeader,
    ReadingBody {
        remaining: usize,
    },
    AfterBody {
        message: Option<DownlinkNotification<T>>,
        remaining: usize,
    },
    Discarding {
        error: Option<AsyncParseError>,
        remaining: usize,
    },
}

pub struct DownlinkNotifiationDecoder<T, R> {
    state: DownlinkNotifiationDecoderState<T>,
    recognizer: RecognizerDecoder<R>,
}

impl<R: Recognizer> DownlinkNotifiationDecoder<R::Target, R> {
    pub fn new(recognizer: R) -> Self {
        DownlinkNotifiationDecoder {
            state: DownlinkNotifiationDecoderState::ReadingHeader,
            recognizer: RecognizerDecoder::new(recognizer),
        }
    }
}

impl<T, R> Decoder for DownlinkNotifiationDecoder<T, R>
where
    R: Recognizer<Target = T>,
{
    type Item = DownlinkNotification<T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let DownlinkNotifiationDecoder {
            state, recognizer, ..
        } = self;
        loop {
            match state {
                DownlinkNotifiationDecoderState::ReadingHeader => {
                    if src.remaining() < TAG_SIZE {
                        src.reserve(TAG_SIZE);
                        break Ok(None);
                    }
                    let tag = src.as_ref()[0];
                    match tag {
                        LINKED => {
                            src.advance(1);
                            break Ok(Some(DownlinkNotification::Linked));
                        }
                        SYNCED => {
                            src.advance(1);
                            break Ok(Some(DownlinkNotification::Synced));
                        }
                        EVENT => {
                            if src.remaining() < TAG_SIZE + LEN_SIZE {
                                let required = TAG_SIZE + LEN_SIZE - src.remaining();
                                src.reserve(required);
                                break Ok(None);
                            } else {
                                src.advance(1);
                                let len = src.get_u64() as usize;
                                recognizer.reset();
                                *state =
                                    DownlinkNotifiationDecoderState::ReadingBody { remaining: len };
                            }
                        }
                        UNLINKED => {
                            src.advance(1);
                            break Ok(Some(DownlinkNotification::Unlinked));
                        }
                        t => {
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!(
                                    "Invalid downlink notification tag: {}",
                                    t
                                )),
                            }));
                        }
                    }
                }
                DownlinkNotifiationDecoderState::ReadingBody { remaining } => {
                    let (new_remaining, rem, decode_result) =
                        super::consume_bounded(remaining, src, recognizer);
                    match decode_result {
                        Ok(Some(result)) => {
                            src.unsplit(rem);
                            *state = DownlinkNotifiationDecoderState::AfterBody {
                                message: Some(DownlinkNotification::Event { body: result }),
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
                                *state = DownlinkNotifiationDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = DownlinkNotifiationDecoderState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                DownlinkNotifiationDecoderState::AfterBody { message, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = message.take();
                        *state = DownlinkNotifiationDecoderState::ReadingHeader;
                        break Ok(result);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                DownlinkNotifiationDecoderState::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = DownlinkNotifiationDecoderState::ReadingHeader;
                        break Err(err.into());
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
pub struct DownlinkOperationEncoder;

impl<T> Encoder<DownlinkOperation<T>> for DownlinkOperationEncoder
where
    T: StructuralWritable,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: DownlinkOperation<T>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let DownlinkOperation { body } = item;
        super::write_recon(dst, &body);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DownlinkOperationDecoder;

impl Decoder for DownlinkOperationDecoder {
    type Item = DownlinkOperation<Bytes>;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() >= LEN_SIZE {
            let len = src.as_ref().get_u64() as usize;
            if src.remaining() >= len + LEN_SIZE {
                src.advance(LEN_SIZE);
                let body = src.split_to(len).freeze();
                Ok(Some(DownlinkOperation { body }))
            } else {
                src.reserve(LEN_SIZE + len);
                Ok(None)
            }
        } else {
            src.reserve(LEN_SIZE);
            Ok(None)
        }
    }
}
