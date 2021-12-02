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

use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::future::Future;

#[derive(Debug)]
struct Selector<F, Y> {
    tasks: FuturesUnordered<Y>,
    fac: F,
}

impl<F, Y, P, O, E> Selector<F, Y>
where
    F: Fn(P) -> Y,
    Y: Future<Output = Option<Result<(P, O), E>>>,
{
    fn new(fac: F) -> Selector<F, Y> {
        Selector {
            tasks: FuturesUnordered::default(),
            fac,
        }
    }

    fn attach(&self, producer: P) {
        let Selector { tasks, fac } = self;
        tasks.push(fac(producer));
    }

    pub async fn next(&mut self) -> Option<Result<O, E>> {
        match self.tasks.next().await.flatten() {
            Some(Ok((producer, output))) => {
                self.attach(producer);
                Some(Ok(output))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::byte_routing::selector::Selector;

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

        for _ in 0..10 {
            let event: Option<Result<i32, ()>> = selector.next().await;
            println!("{:?}", event);
        }
    }
}
