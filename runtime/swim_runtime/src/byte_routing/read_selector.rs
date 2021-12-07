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

use crate::compat::{AgentMessageDecoder, RequestMessage};
use futures_util::future::Either;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::future::Future;
use std::num::NonZeroUsize;
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Value;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;

pub type AttachRequest = oneshot::Sender<ByteWriter>;

pub async fn read_bridge(
    buffer_size: NonZeroUsize,
    rx: mpsc::Receiver<AttachRequest>,
    sink: mpsc::Sender<RequestMessage<Value>>,
) {
    let mut requests = ReceiverStream::new(rx);
    let mut selector = ReadSelector::new(
        |mut framed: FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>| async move {
            framed.next().await.map(|r| r.map(|r| (framed, r)))
        },
    );

    loop {
        let action = if selector.is_empty() {
            requests.next().await.map(Either::Right)
        } else {
            select! {
                read = selector.read() => read.map(Either::Left),
                request = requests.next() => request.map(Either::Right)
            }
        };

        match action {
            Some(Either::Left(message)) => {
                let message = message.expect("Decode error");
                sink.send(message).await.expect("Bridge channel closed");
            }
            Some(Either::Right(callback)) => {
                let (writer, reader) = byte_channel(buffer_size);
                let framed_read =
                    FramedRead::new(reader, AgentMessageDecoder::new(Value::make_recognizer()));

                selector.attach_reader(framed_read);
                callback.send(writer).unwrap();
            }
            None => {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub struct ReadSelector<F, Y> {
    tasks: FuturesUnordered<Y>,
    fac: F,
}

impl<F, Y, P, O, E> ReadSelector<F, Y>
where
    F: Fn(P) -> Y,
    Y: Future<Output = Option<Result<(P, O), E>>>,
{
    pub fn new(fac: F) -> ReadSelector<F, Y> {
        ReadSelector {
            tasks: FuturesUnordered::default(),
            fac,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn attach_reader(&self, producer: P) {
        let ReadSelector { tasks, fac } = self;
        tasks.push(fac(producer));
    }

    pub async fn read(&mut self) -> Option<Result<O, E>> {
        match self.tasks.next().await.flatten() {
            Some(Ok((producer, output))) => {
                self.attach_reader(producer);
                Some(Ok(output))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::byte_routing::read_selector::{read_bridge, ReadSelector};
    use crate::compat::{Operation, RawRequestMessageEncoder, RequestMessage};
    use crate::routing::RoutingAddr;
    use futures_util::future::join3;
    use futures_util::SinkExt;
    use swim_model::path::RelativePath;
    use swim_model::Value;
    use swim_utilities::algebra::non_zero_usize;
    use swim_utilities::io::byte_channel::ByteWriter;
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::codec::FramedWrite;

    #[tokio::test]
    async fn completes() {
        let mut selector = ReadSelector::new(|mut p: Vec<i32>| async move {
            match p.pop() {
                Some(i) => Some(Ok((p, i))),
                None => None,
            }
        });

        selector.attach_reader(vec![1, 2, 3]);
        selector.attach_reader(vec![4, 5, 6]);

        for _ in 0..10 {
            let event: Option<Result<i32, ()>> = selector.read().await;
            println!("{:?}", event);
        }
    }

    #[tokio::test]
    async fn bridge() {
        let (attach_tx, attach_rx) = mpsc::channel(8);
        let (sink_tx, mut sink_rx) = mpsc::channel(8);
        let write_tx = attach_tx.clone();

        let bridge_task = read_bridge(non_zero_usize!(128), attach_rx, sink_tx);
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
