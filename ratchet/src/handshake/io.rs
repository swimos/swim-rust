use crate::errors::Error;
use crate::WebSocketStream;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct BufferedIo<'s, S> {
    socket: &'s mut S,
    pub buffer: BytesMut,
}

impl<'s, S> BufferedIo<'s, S>
where
    S: WebSocketStream,
{
    pub fn new(socket: &'s mut S, buffer: BytesMut) -> BufferedIo<'s, S> {
        BufferedIo { socket, buffer }
    }

    pub async fn write(&mut self) -> Result<(), Error> {
        let BufferedIo { socket, buffer } = self;

        socket.write_all(&buffer).await?;
        socket.flush().await?;

        Ok(())
    }

    pub async fn read(&mut self) -> Result<(), Error> {
        let BufferedIo { socket, buffer } = self;

        let len = buffer.len();
        buffer.resize(len + (8 * 1024), 0);
        let read_count = socket.read(&mut buffer[len..]).await?;
        buffer.truncate(len + read_count);

        Ok(())
    }

    pub fn advance(&mut self, count: usize) {
        self.buffer.advance(count);
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}
