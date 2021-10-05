// Copyright 2015-2021 SWIM.AI inc.
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

use crate::errors::Error;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const RESIZE: usize = 8 * 1024;

pub struct BufferedIo<'s, S> {
    socket: &'s mut S,
    pub buffer: &'s mut BytesMut,
}

impl<'s, S> BufferedIo<'s, S> {
    pub fn new(socket: &'s mut S, buffer: &'s mut BytesMut) -> BufferedIo<'s, S> {
        BufferedIo { socket, buffer }
    }

    pub async fn write(&mut self) -> Result<(), Error>
    where
        S: AsyncWrite + Unpin,
    {
        let BufferedIo { socket, buffer } = self;

        socket.write_all(buffer).await?;
        socket.flush().await?;

        Ok(())
    }

    pub async fn read(&mut self) -> Result<(), Error>
    where
        S: AsyncRead + Unpin,
    {
        let BufferedIo { socket, buffer } = self;

        let len = buffer.len();
        buffer.resize(len + RESIZE, 0);
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
