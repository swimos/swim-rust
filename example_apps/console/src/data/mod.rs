// Copyright 2015-2024 Swim Inc.
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

use std::{ops::Range, time::Duration};

use futures::{future::Either, Stream, StreamExt};
use rand::{seq::SliceRandom, Rng};
use swimos_model::Value;

const WORDS: &str = include_str!("words.txt");

fn word_list() -> Vec<&'static str> {
    WORDS.lines().map(|w| w.trim()).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DataKind {
    I32(Option<Range<i32>>),
    I64(Option<Range<i64>>),
    Words,
}

type ValueIt = Box<dyn Iterator<Item = Value> + Send + Sync + 'static>;

fn values(kind: DataKind) -> ValueIt {
    match kind {
        DataKind::I32(maybe_range) => Box::new(std::iter::from_fn(move || {
            let n = if let Some(range) = &maybe_range {
                rand::thread_rng().gen_range(range.clone())
            } else {
                rand::thread_rng().gen()
            };
            Some(Value::from(n))
        })),
        DataKind::I64(maybe_range) => Box::new(std::iter::from_fn(move || {
            let n = if let Some(range) = &maybe_range {
                rand::thread_rng().gen_range(range.clone())
            } else {
                rand::thread_rng().gen()
            };
            Some(Value::from(n))
        })),
        DataKind::Words => {
            let words = word_list();
            Box::new(std::iter::from_fn(move || {
                let mut rng = rand::thread_rng();
                let w = words.choose(&mut rng).expect("Word list is non-empty.");
                Some(Value::text(*w))
            }))
        }
    }
}

pub fn value_stream(
    kind: DataKind,
    delay: Duration,
    limit: Option<usize>,
) -> impl Stream<Item = Value> + Send + Sync + 'static {
    let values = futures::stream::iter(values(kind)).then(move |v| async move {
        tokio::time::sleep(delay).await;
        v
    });
    if let Some(lim) = limit {
        Either::Left(values.take(lim))
    } else {
        Either::Right(values)
    }
}
