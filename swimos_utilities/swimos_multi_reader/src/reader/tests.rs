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

use crate::MultiReader;
use bytes::Bytes;
use futures::future::join;
use futures::Stream;
use futures::{SinkExt, StreamExt};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use swimos_api::address::RelativeAddress;
use swimos_byte_channel::byte_channel;
use swimos_form::read::RecognizerReadable;
use swimos_messages::protocol::{
    Operation, RawRequestMessageEncoder, RequestMessage, RequestMessageDecoder,
};
use swimos_model::{Text, Value};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

#[tokio::test]
async fn test_single_message_single_stream() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add(FramedRead::new(
        first_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(
                    Text::new("node_1").to_string(),
                    Text::new("lane_1").to_string(),
                ),
                envelope,
            })
            .await
            .unwrap();

        drop(first_framed);
    };

    let read = async move {
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope: Operation::Link,
            }
        );

        let message = multi_reader.next().await;
        assert!(message.is_none());

        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_multiple_messages_single_stream() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add(FramedRead::new(
        first_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope,
            })
            .await
            .unwrap();

        drop(first_framed);
    };

    let read = async move {
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope: Operation::Link,
            }
        );

        let message = multi_reader.next().await;
        assert!(message.is_none());

        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_single_message_multiple_streams() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (second_writer, second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add(FramedRead::new(
        first_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add(FramedRead::new(
        second_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);
        let mut second_framed = FramedWrite::new(second_writer, RawRequestMessageEncoder);

        let envelope: Operation<Bytes> = Operation::Sync;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Sync;

        second_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope,
            })
            .await
            .unwrap();

        drop(first_framed);
        drop(second_framed);
    };

    let read = async move {
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope: Operation::Sync,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope: Operation::Sync,
            }
        );

        let message = multi_reader.next().await;
        assert!(message.is_none());

        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_multiple_messages_multiple_streams() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (second_writer, second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (third_writer, third_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add(FramedRead::new(
        first_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add(FramedRead::new(
        second_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add(FramedRead::new(
        third_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);
        let mut second_framed = FramedWrite::new(second_writer, RawRequestMessageEncoder);
        let third_framed = FramedWrite::new(third_writer, RawRequestMessageEncoder);

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;

        second_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        second_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(4),
                path: RelativeAddress::text("node_4", "lane_4"),
                envelope,
            })
            .await
            .unwrap();

        drop(first_framed);
        drop(second_framed);
        drop(third_framed);
    };

    let read = async move {
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        let envelope: Operation<Value> = Operation::Link;
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(4),
                path: RelativeAddress::text("node_4", "lane_4"),
                envelope,
            }
        );
        let message = multi_reader.next().await;
        assert!(message.is_none());

        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_replace_stream() {
    let (first_writer, first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (second_writer, second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (third_writer, third_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add(FramedRead::new(
        first_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));
    multi_reader.add(FramedRead::new(
        second_reader,
        RequestMessageDecoder::new(Value::make_recognizer()),
    ));

    let write = async move {
        let mut first_framed = FramedWrite::new(first_writer, RawRequestMessageEncoder);
        let mut second_framed = FramedWrite::new(second_writer, RawRequestMessageEncoder);
        let mut third_framed = FramedWrite::new(third_writer, RawRequestMessageEncoder);

        let envelope: Operation<Bytes> = Operation::Link;
        first_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        second_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope,
            })
            .await
            .unwrap();

        drop(first_framed);

        let envelope: Operation<Bytes> = Operation::Link;
        third_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope,
            })
            .await
            .unwrap();

        let envelope: Operation<Bytes> = Operation::Link;
        second_framed
            .send(RequestMessage {
                origin: Uuid::from_u128(4),
                path: RelativeAddress::text("node_4", "lane_4"),
                envelope,
            })
            .await
            .unwrap();

        drop(second_framed);
        drop(third_framed);
    };

    let read = async move {
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(1),
                path: RelativeAddress::new(Text::new("node_1"), Text::new("lane_1")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(2),
                path: RelativeAddress::new(Text::new("node_2"), Text::new("lane_2")),
                envelope: Operation::Link,
            }
        );

        multi_reader.add(FramedRead::new(
            third_reader,
            RequestMessageDecoder::new(Value::make_recognizer()),
        ));

        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(3),
                path: RelativeAddress::new(Text::new("node_3"), Text::new("lane_3")),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await.unwrap().unwrap();
        assert_eq!(
            message,
            RequestMessage {
                origin: Uuid::from_u128(4),
                path: RelativeAddress::text("node_4", "lane_4"),
                envelope: Operation::Link,
            }
        );
        let message = multi_reader.next().await;
        assert!(message.is_none());

        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_512_streams() {
    let mut multi_reader = MultiReader::new();
    let mut writers = vec![];
    for _ in 0..512 {
        let (writer, reader) = byte_channel(NonZeroUsize::new(16).unwrap());

        writers.push(FramedWrite::new(writer, RawRequestMessageEncoder));

        multi_reader.add(FramedRead::new(
            reader,
            RequestMessageDecoder::new(Value::make_recognizer()),
        ));
    }

    let write = async move {
        for (idx, writer) in writers.iter_mut().enumerate() {
            let envelope: Operation<Bytes> = Operation::Link;
            writer
                .send(RequestMessage {
                    origin: Uuid::from_u128(idx as u128),
                    path: RelativeAddress::new(format!("node_{}", idx), format!("lane_{}", idx)),
                    envelope,
                })
                .await
                .unwrap();
        }
    };

    let read = async move {
        let mut idx = 0;
        while let Some(Ok(message)) = multi_reader.next().await {
            assert_eq!(
                message,
                RequestMessage {
                    origin: Uuid::from_u128(idx),
                    path: RelativeAddress::new(
                        format!("node_{}", idx).into(),
                        format!("lane_{}", idx).into()
                    ),
                    envelope: Operation::Link,
                }
            );

            idx += 1;
        }

        assert_eq!(idx, 512);
        assert_eq!(multi_reader.streams.len(), 0)
    };

    join(timeout(Duration::from_secs(5), read), write)
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn test_multiple_streams_polled() {
    let mut multi_reader = MultiReader::new();

    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());

    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;

    for (_, reader) in multi_reader.streams {
        assert_eq!(reader.polled, 1)
    }
}

#[tokio::test]
async fn test_multiple_streams_completing() {
    let mut multi_reader = MultiReader::new();

    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());

    assert_eq!(multi_reader.streams.len(), 5);
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    assert_eq!(multi_reader.streams.len(), 5);

    multi_reader.streams.get_mut(0).unwrap().close();
    multi_reader.streams.get_mut(0).unwrap().wake();
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    assert_eq!(multi_reader.streams.len(), 4);

    multi_reader.streams.get_mut(1).unwrap().close();
    multi_reader.streams.get_mut(1).unwrap().wake();
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    assert_eq!(multi_reader.streams.len(), 3);

    multi_reader.streams.get_mut(2).unwrap().close();
    multi_reader.streams.get_mut(2).unwrap().wake();
    multi_reader.streams.get_mut(3).unwrap().close();
    multi_reader.streams.get_mut(3).unwrap().wake();
    multi_reader.streams.get_mut(4).unwrap().close();
    multi_reader.streams.get_mut(4).unwrap().wake();
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    assert_eq!(multi_reader.streams.len(), 0);
}

#[tokio::test]
async fn test_streams_non_biased_single_bucket() {
    let mut multi_reader = MultiReader::new();

    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());
    multi_reader.add(TestReader::new());

    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;

    multi_reader.streams.get_mut(0).unwrap().ready();
    multi_reader.streams.get_mut(0).unwrap().wake();
    multi_reader.streams.get_mut(1).unwrap().ready();
    multi_reader.streams.get_mut(1).unwrap().wake();
    multi_reader.streams.get_mut(2).unwrap().ready();
    multi_reader.streams.get_mut(2).unwrap().wake();

    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;

    for (_, reader) in multi_reader.streams {
        assert_eq!(reader.polled, 2)
    }
}

#[tokio::test]
async fn test_streams_non_biased_multiple_buckets() {
    let mut multi_reader = MultiReader::new();

    for _ in 0..512 {
        multi_reader.add(TestReader::new());
    }

    let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;

    for i in 0..512 {
        multi_reader.streams.get_mut(i).unwrap().ready();
        multi_reader.streams.get_mut(i).unwrap().wake();
    }

    for _ in 0..512 * 2 {
        let _ = timeout(Duration::from_millis(10), multi_reader.next()).await;
    }

    for (_, reader) in multi_reader.streams {
        assert_eq!(reader.polled, 3)
    }
}

enum ReaderState {
    Ready,
    Closed,
    Pending,
}

struct TestReader {
    polled: usize,
    state: ReaderState,
    waker: Option<Waker>,
}

impl TestReader {
    fn new() -> Self {
        TestReader {
            polled: 0,
            state: ReaderState::Pending,
            waker: None,
        }
    }

    fn wake(&self) {
        self.waker.as_ref().unwrap().wake_by_ref();
    }

    fn close(&mut self) {
        self.state = ReaderState::Closed;
    }

    fn ready(&mut self) {
        self.state = ReaderState::Ready;
    }
}

impl Stream for TestReader {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled += 1;

        if self.waker.is_none() {
            self.waker = Some(cx.waker().clone());
        }

        match self.state {
            ReaderState::Ready => Poll::Ready(Some(())),
            ReaderState::Closed => Poll::Ready(None),
            ReaderState::Pending => Poll::Pending,
        }
    }
}
