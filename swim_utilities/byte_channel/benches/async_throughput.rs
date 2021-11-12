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

use byte_channel::byte_channel;

use bytes::{Bytes, BytesMut};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use futures::future::join;
use rand::Rng;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::runtime::Builder;

const WORKER_THREADS: usize = 2;

fn channel_throughput_benchmark(c: &mut Criterion) {
    let test_data = generate_test_data().freeze();
    let param_sets = test_params(test_data);
    let mut group = c.benchmark_group("Channels");
    group.sampling_mode(SamplingMode::Flat);
    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .build()
        .expect("Failed to crate Tokio runtime.");

    for params in param_sets {
        group.throughput(Throughput::Bytes(params.test_data.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("Tokio Duplex Stream", &params),
            &params,
            |b, p| {
                b.to_async(&runtime).iter(|| {
                    let TestParams {
                        test_data,
                        buffer_size,
                        chunk_size,
                    } = p;
                    duplex_stream_throughput(*buffer_size, test_data.clone(), *chunk_size)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Byte Channel", &params),
            &params,
            |b, p| {
                b.to_async(&runtime).iter(|| {
                    let TestParams {
                        test_data,
                        buffer_size,
                        chunk_size,
                    } = p;
                    byte_channel_throughput(*buffer_size, test_data.clone(), *chunk_size)
                })
            },
        );
    }
}

criterion_group!(channel_benches, channel_throughput_benchmark);
criterion_main!(channel_benches);

async fn channel_throughput<W, R>(mut writer: W, mut reader: R, test_data: Bytes, chunk_size: usize)
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    let write_task = async move {
        for chunk in test_data.chunks(chunk_size) {
            writer.write_all(chunk).await.expect("Channel failed.");
        }
    };

    let read_task = async move {
        let mut sink = BytesMut::new();
        loop {
            if matches!(reader.read_buf(&mut sink).await, Ok(0) | Err(_)) {
                break;
            } else {
                sink.clear();
            }
        }
    };

    let (r1, r2) = join(tokio::spawn(write_task), tokio::spawn(read_task)).await;
    assert!(r1.is_ok() && r2.is_ok());
}

async fn duplex_stream_throughput(length: usize, test_data: Bytes, chunk_size: usize) {
    let (c1, c2) = tokio::io::duplex(length);
    let (_, tx) = tokio::io::split(c1);
    let (rx, _) = tokio::io::split(c2);

    channel_throughput(tx, rx, test_data, chunk_size).await;
}

async fn byte_channel_throughput(length: usize, test_data: Bytes, chunk_size: usize) {
    let (tx, rx) = byte_channel(NonZeroUsize::new(length).unwrap());

    channel_throughput(tx, rx, test_data, chunk_size).await;
}

const TEST_DATA_MAX: usize = 1048576;

fn generate_test_data() -> BytesMut {
    let mut rnd = rand::thread_rng();
    std::iter::from_fn(|| Some(rnd.gen::<u8>()))
        .take(TEST_DATA_MAX)
        .collect()
}

#[derive(Clone)]
struct TestParams {
    test_data: Bytes,
    buffer_size: usize,
    chunk_size: usize,
}

impl Display for TestParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[data_len = {}, buffer_size = {}, chunk_size = {}]",
            self.test_data.len(),
            self.buffer_size,
            self.chunk_size
        )
    }
}

const TEST_DATA_SIZES: &[usize] = &[1024, 16384, 262144, 1048576, 2097152];
const BUFFER_SIZES: &[usize] = &[128, 1024, 4096, 16384, 32768];
const CHUNK_SIZES: &[usize] = &[32, 64, 128, 512, 1024, 4096, 8192, 16384];

fn test_params(all_test_data: Bytes) -> Vec<TestParams> {
    let mut params = vec![];
    for i in TEST_DATA_SIZES {
        let mut test_data = all_test_data.clone();
        test_data.truncate(*i);
        for j in BUFFER_SIZES {
            for k in CHUNK_SIZES.iter().take_while(|n| **n < *j) {
                params.push(TestParams {
                    test_data: test_data.clone(),
                    buffer_size: *j,
                    chunk_size: *k,
                })
            }
        }
    }
    params
}
