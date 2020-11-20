// Copyright 2015-2020 SWIM.AI inc.
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

use crate::routing::remote::net::Listener;
use either::Either;
use futures::ready;
use futures::stream::Fuse;
use futures::FutureExt;
use futures::StreamExt;
use futures::{select, Stream};
use futures_util::stream::FuturesUnordered;
use pin_project::pin_project;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::Context;
use tokio::io::ErrorKind;
use tokio::macros::support::Poll;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tls::{TlsAcceptor, TlsStream};

type TlsHandshakeResult = io::Result<(TlsStream<TcpStream>, SocketAddr)>;
type TcpHandshakeResult = io::Result<(TcpStream, SocketAddr)>;

pub struct TlsListener(TcpListener);

impl Listener for TlsListener {
    type Socket = TlsStream<TcpStream>;
    type AcceptStream = Fuse<mpsc::Receiver<TlsHandshakeResult>>;

    fn into_stream(self) -> Self::AcceptStream {
        unimplemented!()
    }
}

#[pin_project]
struct TcpStreamWithAddr(#[pin] TcpListener);

impl Stream for TcpStreamWithAddr {
    type Item = TcpHandshakeResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (socket, addr) = ready!(self.project().0.poll_accept(cx))?;
        Poll::Ready(Some(Ok((socket, addr))))
    }
}

pub struct PendingTlsConnections {
    listener: TcpStreamWithAddr,
    acceptor: TlsAcceptor,
    sender: mpsc::Sender<TlsHandshakeResult>,
}

impl PendingTlsConnections {
    pub fn new(
        listener: TcpListener,
        acceptor: TlsAcceptor,
        max_pending: NonZeroUsize,
    ) -> mpsc::Receiver<TlsHandshakeResult> {
        let (tx, rx) = mpsc::channel(max_pending.get());

        let pending = PendingTlsConnections {
            listener: TcpStreamWithAddr(listener),
            acceptor,
            sender: tx,
        };

        let _jh = tokio::spawn(pending.run());

        rx
    }

    async fn run(self) {
        let PendingTlsConnections {
            listener,
            acceptor,
            mut sender,
        } = self;

        let mut fused_accept = listener.fuse();
        let mut pending = FuturesUnordered::new();

        loop {
            let next: Option<Either<TcpHandshakeResult, TlsHandshakeResult>> = select! {
                stream = fused_accept.next() => stream.map(Either::Left),
                result = pending.next() => result.map(Either::Right),
            };

            match next {
                Some(Either::Left(result)) => match result {
                    Ok((stream, addr)) => {
                        let accept_future = acceptor.accept(stream).map(move |r| match r {
                            Ok(stream) => Ok((stream, addr)),
                            Err(e) => {
                                // todo: remove after upgrade to Tokio 0.3
                                Err(io::Error::new(ErrorKind::Other, e))
                            }
                        });

                        pending.push(Box::pin(accept_future));
                    }
                    Err(e) => {
                        // todo: map err
                        if sender.send(Err(e)).await.is_err() {
                            // todo: log
                        }
                    }
                },
                Some(Either::Right(result)) => {
                    if sender.send(result).await.is_err() {
                        // todo: log
                    }
                }
                None => {
                    return;
                }
            }
        }
    }
}
