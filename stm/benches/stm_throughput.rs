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

use criterion::{
    criterion_group, criterion_main, Criterion, SamplingMode, Throughput,
};
use futures::StreamExt;
use std::num::NonZeroUsize;
use stm::var::TVar;
use tokio::runtime::Builder;

const WORKER_THREADS: usize = 2;
const NUM_VALUES: usize = 64 * 1024;

fn stm_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Channels");
    group.sampling_mode(SamplingMode::Flat);
    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .build()
        .expect("Failed to crate Tokio runtime.");

    group.throughput(Throughput::Elements(NUM_VALUES as u64));

    group.bench_function("stm set", |b| {
        b.to_async(&runtime).iter(stm_set_throughput)
    });

    group.bench_function("stm set observed", |b| {
        b.to_async(&runtime).iter(stm_set_throughput_observed)
    });
}

criterion_group!(channel_benches, stm_throughput_benchmark);
criterion_main!(channel_benches);

async fn stm_set_throughput() {
    let var = TVar::new(0);
    for i in 0..NUM_VALUES {
        var.store(i).await;
    }
}

async fn stm_set_throughput_observed() {
    let (var, observer) = TVar::new_with_observer(0, NonZeroUsize::new(16).unwrap());
    let set_task = tokio::spawn(async move {
        for i in 0..NUM_VALUES {
            var.store(i).await;
        }
    });
    let consume_task = tokio::spawn(async move {
        let mut stream = observer.into_stream();
        for _ in 0..NUM_VALUES {
            assert!(stream.next().await.is_some());
        }
    });

    let (result1, result2) = futures::future::join(set_task, consume_task).await;
    assert!(result1.is_ok());
    assert!(result2.is_ok());
}
