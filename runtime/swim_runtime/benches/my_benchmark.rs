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

// use crate::routing::RoutingAddr;
use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::stream::SelectAll;
use futures::Sink;
use futures_util::future::join;
use futures_util::SinkExt;
use ratchet::Message;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::path::RelativePath;
use swim_model::{Item, Value};
use swim_runtime::compat::{
    AgentMessageDecoder, MessageDecodeError, Operation, RawRequestMessageEncoder,
    TaggedRequestMessage,
};
use swim_runtime::routing::RoutingAddr;
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter, MultiReader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Builder;
use tokio::time::{sleep, timeout};
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite};

const MESSAGE_COUNT: u32 = 100;
const CHANNEL_COUNT: u32 = 50;
const SAMPLE_SIZE: usize = 1000;

fn multi_reader_benchmark(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("Failed to crate Tokio runtime.");

    let mut group = c.benchmark_group("Read selector");

    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function(BenchmarkId::new("Select all", 1), |bencher| {
        bencher.to_async(&runtime).iter(|| select_all_test())
    });
    group.bench_function(BenchmarkId::new("Multi reader", 1), |bencher| {
        bencher.to_async(&runtime).iter(|| multi_reader_test())
    });

    group.finish();
}

criterion_group!(benches, multi_reader_benchmark);
criterion_main!(benches);

fn create_channels(
    n: u32,
) -> (
    Vec<FramedWrite<ByteWriter, RawRequestMessageEncoder>>,
    Vec<FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>>,
) {
    let mut writers = Vec::new();
    let mut readers = Vec::new();
    for _ in 0..n {
        let (writer, reader) = create_framed_channel();
        writers.push(writer);
        readers.push(reader);
    }
    (writers, readers)
}

fn create_framed_channel() -> (
    FramedWrite<ByteWriter, RawRequestMessageEncoder>,
    FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>,
) {
    let (writer, reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let framed_writer = FramedWrite::new(writer, RawRequestMessageEncoder);
    let framed_reader = FramedRead::new(reader, AgentMessageDecoder::new(Value::make_recognizer()));

    (framed_writer, framed_reader)
}

async fn read<T: Stream<Item = Result<TaggedRequestMessage<Value>, MessageDecodeError>> + Unpin>(
    mut reader: T,
) {
    let mut count = 0;
    while let Some(result) = reader.next().await {
        result.unwrap();
        count += 1;
    }

    assert_eq!(count, MESSAGE_COUNT * CHANNEL_COUNT);
}

async fn write(mut writers: Vec<FramedWrite<ByteWriter, RawRequestMessageEncoder>>) {
    for i in 0..MESSAGE_COUNT {
        for writer in &mut writers {
            writer
                .feed(TaggedRequestMessage {
                    origin: RoutingAddr::remote(i),
                    path: RelativePath::new(format!("node_{}", i), format!("lane_{}", i)),
                    envelope: Operation::Link,
                })
                .await
                .unwrap();
        }
    }

    for writer in &mut writers {
        writer.flush().await.unwrap();
    }
}

async fn select_all_test() {
    let (writers, readers) = create_channels(CHANNEL_COUNT);

    let mut multi_reader = SelectAll::new();
    for reader in readers {
        multi_reader.push(reader)
    }

    let _ = join(read(multi_reader), write(writers)).await;
}

async fn multi_reader_test() {
    let (writers, readers) = create_channels(CHANNEL_COUNT);

    let mut multi_reader = MultiReader::new();
    for reader in readers {
        multi_reader.add_reader(reader)
    }

    let _ = join(read(multi_reader), write(writers)).await;
}
