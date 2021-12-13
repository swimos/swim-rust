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

use futures::Stream;
use futures_util::future::Either;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Decoder, FramedRead};

pub type AttachRequest = oneshot::Sender<ByteWriter>;

pub async fn read_bridge<F, D, S>(
    buffer_size: NonZeroUsize,
    mut requests: S,
    decoder: F,
    sink: mpsc::Sender<D::Item>,
) -> Result<(), ()>
where
    S: Stream<Item = AttachRequest> + Unpin,
    F: Fn() -> D,
    D: Decoder,
    D::Error: Debug,
{
    let mut selector = Selector::new(|mut framed: FramedRead<ByteReader, D>| async move {
        framed.next().await.map(|r| r.map(|r| (framed, r)))
    });

    loop {
        let action = select! {
            read = selector.read() => Some(Either::Left(read)),
            request = requests.next() => request.map(Either::Right)
        };

        match action {
            Some(Either::Left(message)) => {
                let message = message.expect("Decode error");
                if let Err(_) = sink.send(message).await {
                    return Err(());
                }
            }
            Some(Either::Right(callback)) => {
                let (writer, reader) = byte_channel(buffer_size);
                let framed_read = FramedRead::new(reader, decoder());

                selector.attach(framed_read);
                let _ = callback.send(writer);
            }
            None => {
                break;
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct PendingFutures<Y>(FuturesUnordered<Y>);

impl<Y> Stream for PendingFutures<Y>
where
    Y: Future,
{
    type Item = Y::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Ready(Some(out)) => Poll::Ready(Some(out)),
            Poll::Ready(None) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct Selector<F, Y> {
    tasks: PendingFutures<Y>,
    fac: F,
}

impl<F, Y, P, O, E> Selector<F, Y>
where
    F: Fn(P) -> Y,
    Y: Future<Output = Option<Result<(P, O), E>>>,
{
    pub fn new(fac: F) -> Selector<F, Y> {
        Selector {
            tasks: PendingFutures(FuturesUnordered::default()),
            fac,
        }
    }

    pub fn attach(&self, producer: P) {
        let Selector { tasks, fac } = self;
        tasks.0.push(fac(producer));
    }

    pub async fn read(&mut self) -> Result<O, E> {
        loop {
            match self.tasks.next().await.flatten() {
                Some(Ok((producer, output))) => {
                    self.attach(producer);
                    break Ok(output);
                }
                Some(Err(e)) => break Err(e),
                None => {
                    // a task has completed but yielded nothing
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::byte_routing::selector::{read_bridge, Selector};
    use crate::compat::{AgentMessageDecoder, Operation, RawRequestMessageEncoder, RequestMessage};
    use crate::routing::RoutingAddr;
    use futures_util::future::join3;
    use futures_util::{FutureExt, SinkExt};
    use swim_form::structural::read::recognizer::RecognizerReadable;
    use swim_model::path::RelativePath;
    use swim_model::Value;
    use swim_utilities::algebra::non_zero_usize;
    use swim_utilities::io::byte_channel::ByteWriter;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::codec::FramedWrite;

    #[tokio::test]
    async fn completes() {
        let mut selector = Selector::new(|mut p: Vec<i32>| async move {
            match p.pop() {
                Some(i) => Some(Ok((p, i))),
                None => None,
            }
        });

        selector.attach(vec![1, 2, 3]);
        selector.attach(vec![4, 5, 6]);

        for _ in 0..5 {
            let event: Result<i32, ()> = selector.read().await;
            assert!(event.is_ok());
        }

        assert!(selector.read().now_or_never().is_none());
    }

    #[tokio::test]
    async fn bridge() {
        let (attach_tx, attach_rx) = mpsc::channel(8);
        let (sink_tx, mut sink_rx) = mpsc::channel(8);
        let write_tx = attach_tx.clone();

        let bridge_task = read_bridge(
            non_zero_usize!(128),
            ReceiverStream::new(attach_rx),
            || AgentMessageDecoder::new(Value::make_recognizer()),
            sink_tx,
        );
        let receiver_task = async move {
            async fn read(sink: &mut mpsc::Receiver<RequestMessage<Value>>, op: Operation<Value>) {
                let message = sink.recv().await.unwrap();
                assert_eq!(
                    message,
                    RequestMessage {
                        origin: RoutingAddr::remote(13),
                        path: RelativePath::new("node", "lane"),
                        envelope: op
                    }
                );
            }

            read(&mut sink_rx, Operation::Link).await;
            read(&mut sink_rx, Operation::Sync).await;
            read(&mut sink_rx, Operation::Unlink).await;

            // The task will stop once this is dropped
            let _guard = attach_tx;
        };

        let write_task = async move {
            let (attacher, attachee) = oneshot::channel();
            assert!(write_tx.send(attacher).await.is_ok());

            let writer = attachee.await.unwrap();
            let mut framed = FramedWrite::new(writer, RawRequestMessageEncoder);

            async fn write(
                framed: &mut FramedWrite<ByteWriter, RawRequestMessageEncoder>,
                op: Operation<&[u8]>,
            ) {
                assert!(framed
                    .send(RequestMessage {
                        origin: RoutingAddr::remote(13),
                        path: RelativePath::new("node", "lane"),
                        envelope: op
                    })
                    .await
                    .is_ok());
            }

            write(&mut framed, Operation::Link).await;
            write(&mut framed, Operation::Sync).await;
            write(&mut framed, Operation::Unlink).await;
        };

        join3(bridge_task, receiver_task, write_task).await;
    }
}
