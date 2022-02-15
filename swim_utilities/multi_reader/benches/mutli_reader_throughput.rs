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

use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::time::Duration;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::stream::SelectAll;
use futures_util::future::join;
use futures_util::{SinkExt, Stream, StreamExt};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tokio::runtime::Builder;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};

use byte_channel::byte_channel;
use byte_channel::{ByteReader, ByteWriter};
use multi_reader::mailbox::fan_in::{mailbox_channel, RawChannelSender};
use multi_reader::MultiReader;
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_runtime::compat::{
    AgentMessageDecoder, MessageDecodeError, Operation, RawRequestMessageEncoder, RequestMessage,
};
use swim_runtime::routing::RoutingAddr;

const MESSAGE_COUNTS: &[usize] = &[/*10, 100, */ 1000 /*, 10000*/];
const CHANNEL_COUNTS: &[usize] = &[/*2, 8, */ 64 /*, 256, 512*/];

const SEED: &[u8; 32] = &[55; 32];

#[derive(Debug, Clone, Copy)]
struct TestParams {
    message_count: usize,
    channel_count: usize,
}

impl Display for TestParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[message_count = {}, channel_count = {}]",
            self.message_count, self.channel_count
        )
    }
}

fn test_params() -> Vec<TestParams> {
    let mut params = vec![];

    for i in MESSAGE_COUNTS {
        for j in CHANNEL_COUNTS {
            params.push(TestParams {
                message_count: *i,
                channel_count: *j,
            })
        }
    }
    params
}

fn multi_reader_benchmark(c: &mut Criterion) {
    let param_sets = test_params();

    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("Failed to crate Tokio runtime.");

    let mut group = c.benchmark_group("Read selector");
    group.measurement_time(Duration::from_secs(20));

    for params in param_sets {
        group.throughput(Throughput::Elements(
            (params.message_count * params.channel_count) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::new("Mailbox (fan in)", &params),
            &params,
            |bencher, params| {
                bencher
                    .to_async(&runtime)
                    .iter(|| fan_in_mailbox_test(*params))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Select all", &params),
            &params,
            |bencher, params| bencher.to_async(&runtime).iter(|| select_all_test(*params)),
        );

        group.bench_with_input(
            BenchmarkId::new("Multi reader", &params),
            &params,
            |bencher, params| {
                bencher
                    .to_async(&runtime)
                    .iter(|| multi_reader_test(*params))
            },
        );
    }
}

criterion_group!(benches, multi_reader_benchmark);
criterion_main!(benches);

type Writer = FramedWrite<ByteWriter, RawRequestMessageEncoder>;
type Reader = FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>;

fn create_channels(n: usize) -> (Vec<Writer>, Vec<Reader>) {
    let mut writers = Vec::new();
    let mut readers = Vec::new();
    for _ in 0..n {
        let (writer, reader) = create_framed_channel();
        writers.push(writer);
        readers.push(reader);
    }
    (writers, readers)
}

fn create_framed_channel() -> (Writer, Reader) {
    let (writer, reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let framed_writer = FramedWrite::new(writer, RawRequestMessageEncoder);
    let framed_reader = FramedRead::new(reader, AgentMessageDecoder::new(Value::make_recognizer()));

    (framed_writer, framed_reader)
}

async fn read<T: Stream<Item = Result<RequestMessage<Value>, MessageDecodeError>> + Unpin>(
    mut reader: T,
    params: TestParams,
) {
    let mut count = 0;
    while let Some(result) = reader.next().await {
        result.unwrap();
        count += 1;

        if (params.message_count * params.channel_count) - count == 0 {
            break;
        }
    }
    assert_eq!(count, params.message_count * params.channel_count);
}

async fn write(mut writers: Vec<Writer>, message_count: usize) {
    for i in 0..message_count {
        for writer in &mut writers {
            writer
                .send(RequestMessage {
                    origin: RoutingAddr::remote(i as u32),
                    path: RelativePath::new(format!("node_{}", i), format!("lane_{}", i)),
                    envelope: Operation::Link,
                })
                .await
                .unwrap();
        }
    }
}

async fn select_all_test(params: TestParams) {
    let (writers, readers) = create_channels(params.channel_count);

    let mut multi_reader = SelectAll::new();
    for reader in readers {
        multi_reader.push(reader)
    }

    let _ = join(
        read(multi_reader, params),
        write(writers, params.message_count),
    )
    .await;
}

async fn multi_reader_test(params: TestParams) {
    let (writers, readers) = create_channels(params.channel_count);

    let mut multi_reader = MultiReader::new();
    for reader in readers {
        multi_reader.add_reader(reader)
    }

    let _ = join(
        read(multi_reader, params),
        write(writers, params.message_count),
    )
    .await;
}

async fn fan_in_mailbox_test(params: TestParams) {
    let (registrar, mut rx) = mailbox_channel(NonZeroUsize::new(256).unwrap());

    let mut writers = Vec::with_capacity(params.channel_count);
    for _ in 0..params.channel_count {
        writers.push(registrar.register().unwrap());
    }

    async fn mailbox_write(mut writers: Vec<RawChannelSender>, message_count: usize) {
        let mut buf = BytesMut::new();
        for i in 0..message_count {
            for writer in &mut writers {
                RawRequestMessageEncoder
                    .encode(
                        RequestMessage {
                            origin: RoutingAddr::remote(i as u32),
                            path: RelativePath::new(format!("node_{}", i), format!("lane_{}", i)),
                            envelope: Operation::Link,
                        },
                        &mut buf,
                    )
                    .unwrap();

                writer.send(&mut buf).await.unwrap();

                buf.clear()
            }
        }
    }

    let _ = join(
        read(
            FramedRead::new(rx, AgentMessageDecoder::new(Value::make_recognizer())),
            params,
        ),
        mailbox_write(writers, params.message_count),
    )
    .await;
}
