use std::io::ErrorKind;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Stream;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

use swim_api::error::FrameIoError;
use swim_api::protocol::agent::{LaneRequest, LaneRequestDecoder};
use swim_api::protocol::map::{
    MapMessageDecoder, MapMessageEncoder, MapOperation, RawMapOperationDecoder,
};
use swim_api::protocol::WithLengthBytesCodec;
use swim_utilities::io::byte_channel::ByteReader;

#[cfg(test)]
mod tests;

type LaneCodec<T> = FramedRead<ByteReader, LaneRequestDecoder<T>>;
type ValueReaderCodec = LaneCodec<WithLengthBytesCodec>;
type MapReaderCodec = LaneCodec<MapMessageDecoder<RawMapOperationDecoder>>;

const TAG_SIZE: usize = size_of::<u8>();
const LEN_SIZE: usize = size_of::<i64>();

pub enum LaneReaderCodec {
    Value(ValueReaderCodec),
    Map(MapReaderCodec),
}

impl LaneReaderCodec {
    pub fn value(reader: ByteReader) -> LaneReaderCodec {
        LaneReaderCodec::Value(LaneCodec::new(reader, LaneRequestDecoder::default()))
    }

    pub fn map(reader: ByteReader) -> LaneReaderCodec {
        LaneReaderCodec::Map(LaneCodec::new(reader, LaneRequestDecoder::default()))
    }
}

struct MapOperationBytesEncoder;

impl MapOperationBytesEncoder {
    const UPDATE: u8 = 0;
    const REMOVE: u8 = 1;
    const CLEAR: u8 = 2;

    const OVERSIZE_KEY: &'static str = "Key too large.";
    const OVERSIZE_RECORD: &'static str = "Record too large.";
}

impl Encoder<MapOperation<BytesMut, BytesMut>> for MapOperationBytesEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: MapOperation<BytesMut, BytesMut>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            MapOperation::Update { key, value } => {
                let total_len = key.len() + value.len() + LEN_SIZE + TAG_SIZE;
                dst.reserve(total_len + LEN_SIZE);
                dst.put_u64(u64::try_from(total_len).expect(Self::OVERSIZE_RECORD));
                dst.put_u8(Self::UPDATE);
                let key_len = u64::try_from(key.len()).expect(Self::OVERSIZE_KEY);
                dst.put_u64(key_len);
                dst.put(key);
                dst.put(value);
            }
            MapOperation::Remove { key } => {
                let total_len = key.len() + TAG_SIZE;
                dst.reserve(total_len + LEN_SIZE);
                dst.put_u64(u64::try_from(total_len).expect(Self::OVERSIZE_RECORD));
                dst.put_u8(Self::REMOVE);
                dst.put(key);
            }
            MapOperation::Clear => {
                dst.reserve(LEN_SIZE + TAG_SIZE);
                dst.put_u64(TAG_SIZE as u64);
                dst.put_u8(Self::CLEAR);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DiscriminatedLaneRequest {
    pub map_like: bool,
    pub request: LaneRequest<BytesMut>,
}

impl Stream for LaneReaderCodec {
    type Item = Result<DiscriminatedLaneRequest, FrameIoError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            LaneReaderCodec::Value(ref mut inner) => match Pin::new(inner).poll_next(cx) {
                Poll::Ready(Some(Ok(request))) => Poll::Ready(Some(Ok(DiscriminatedLaneRequest {
                    map_like: false,
                    request,
                }))),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            LaneReaderCodec::Map(ref mut inner) => match Pin::new(inner).poll_next(cx) {
                Poll::Ready(Some(Ok(op))) => {
                    let request = match op {
                        LaneRequest::Command(command) => {
                            let mut buf = BytesMut::new();
                            MapMessageEncoder::new(MapOperationBytesEncoder)
                                .encode(command, &mut buf)
                                .expect("Map encoding should be infallible");
                            LaneRequest::Command(buf)
                        }
                        LaneRequest::InitComplete => LaneRequest::InitComplete,
                        LaneRequest::Sync(id) => LaneRequest::Sync(id),
                    };
                    Poll::Ready(Some(Ok(DiscriminatedLaneRequest {
                        map_like: true,
                        request,
                    })))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
enum LaneResponseDecoderState {
    #[default]
    Header,
    ResponseHeader,
    ResponseBody {
        lane_id: u64,
        len: usize,
    },
}

#[derive(PartialEq, Debug)]
pub enum LaneResponseElement {
    Feed,
    Response { lane_id: u64, data: Bytes },
}

#[derive(Default)]
pub struct LaneResponseDecoder {
    state: LaneResponseDecoderState,
    more_available: bool,
}

impl LaneResponseDecoder {
    const MORE_AVAILABLE: usize = size_of::<i8>();
    const RESPONSE_HEADER: usize = size_of::<u64>() * 2;
}

impl Decoder for LaneResponseDecoder {
    type Item = LaneResponseElement;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let LaneResponseDecoder {
            state,
            more_available,
        } = self;

        loop {
            match *state {
                LaneResponseDecoderState::Header => {
                    if src.remaining() >= Self::MORE_AVAILABLE {
                        match src.get_i8() {
                            0 => *more_available = false,
                            1 => *more_available = true,
                            _ => {
                                return Err(ErrorKind::InvalidData.into());
                            }
                        }

                        *state = LaneResponseDecoderState::ResponseHeader;
                    } else {
                        return Ok(None);
                    }
                }
                LaneResponseDecoderState::ResponseHeader => {
                    if src.remaining() >= Self::RESPONSE_HEADER {
                        let lane_id = src.get_u64();
                        let len = src.get_u32() as usize;
                        *state = LaneResponseDecoderState::ResponseBody { lane_id, len };
                    } else {
                        *state = LaneResponseDecoderState::Header;
                        return if *more_available {
                            Ok(Some(LaneResponseElement::Feed))
                        } else {
                            Ok(None)
                        };
                    }
                }
                LaneResponseDecoderState::ResponseBody { lane_id, len } => {
                    return if src.remaining() >= len {
                        let response_body = src.split_to(len).freeze();
                        *state = LaneResponseDecoderState::ResponseHeader;
                        Ok(Some(LaneResponseElement::Response {
                            lane_id,
                            data: response_body,
                        }))
                    } else {
                        Err(ErrorKind::InvalidData.into())
                    };
                }
            }
        }
    }
}

pub struct LaneReader {
    idx: u64,
    codec: LaneReaderCodec,
}

impl LaneReader {
    pub fn value(idx: u64, reader: ByteReader) -> LaneReader {
        LaneReader {
            idx,
            codec: LaneReaderCodec::value(reader),
        }
    }

    pub fn map(idx: u64, reader: ByteReader) -> LaneReader {
        LaneReader {
            idx,
            codec: LaneReaderCodec::map(reader),
        }
    }
}

impl Stream for LaneReader {
    type Item = (u64, Result<DiscriminatedLaneRequest, FrameIoError>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let LaneReader { idx, codec } = self.get_mut();
        Pin::new(codec)
            .poll_next(cx)
            .map(|r| r.map(|result| (*idx, result)))
    }
}
