use crate::codec::CodecFlags;
use crate::errors::{Error, ErrorKind};
use crate::protocol::frame::{apply_mask, FrameHeader};
use crate::WebSocketStream;
use bytes::{Buf, BytesMut};
use either::Either;
use futures::{AsyncReadExt, AsyncWriteExt};
use std::fs::read;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub struct FramedIo<S> {
    pub io: Compat<S>,
}

impl<S> FramedIo<S>
where
    S: WebSocketStream,
{
    pub fn new(io: S) -> FramedIo<S> {
        FramedIo { io: io.compat() }
    }

    pub async fn read_frame(
        &mut self,
        read_buffer: &mut BytesMut,
    ) -> Result<(FrameHeader, BytesMut), Error> {
        loop {
            let (header, header_len, payload_len) =
                match FrameHeader::read_from(read_buffer, &CodecFlags::empty(), usize::MAX)? {
                    Either::Left(r) => r,
                    Either::Right(count) => {
                        let len = read_buffer.len();
                        read_buffer.resize(len + count, 0u8);
                        self.io.read_exact(&mut read_buffer[len..]).await?;
                        continue;
                    }
                };

            let frame_len = header_len + payload_len;
            let len = read_buffer.len();

            if read_buffer.len() < frame_len {
                let dif = frame_len - read_buffer.len();
                read_buffer.resize(len + dif, 0u8);
                self.io.read_exact(&mut read_buffer[len..]).await?;
            }

            read_buffer.advance(header_len);

            let mut payload = read_buffer.split_to(payload_len);
            if let Some(mask) = header.mask {
                apply_mask(mask, &mut payload);
            }

            break Ok((header, payload));
        }
    }

    pub async fn write(&mut self, write_buffer: &[u8]) -> Result<(), Error> {
        self.io
            .write_all(write_buffer)
            .await
            .map_err(|e| Error::with_cause(ErrorKind::IO, e))
    }
}
