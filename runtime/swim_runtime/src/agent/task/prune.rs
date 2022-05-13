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

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{ready, FutureExt, Stream};
use tokio::time::{Instant, Sleep};
use uuid::Uuid;

#[derive(Debug)]
pub struct PruneRemotes<'a> {
    next_id: Option<Uuid>,
    delay: Pin<&'a mut Sleep>,
    remote_ids: VecDeque<(Uuid, Instant)>,
}

impl<'a> PruneRemotes<'a> {
    pub fn new(delay: Pin<&'a mut Sleep>) -> Self {
        PruneRemotes {
            next_id: None,
            delay,
            remote_ids: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.next_id.is_none()
    }

    pub fn push(&mut self, id: Uuid, timeout: Duration) {
        let PruneRemotes {
            next_id,
            delay,
            remote_ids,
        } = self;
        let timeout_at = Instant::now()
            .checked_add(timeout)
            .expect("Timer overflow.");
        if next_id.is_some() {
            remote_ids.push_back((id, timeout_at));
        } else {
            *next_id = Some(id);
            delay.as_mut().reset(timeout_at);
        }
    }
}

impl<'a> Stream for PruneRemotes<'a> {
    type Item = Uuid;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_empty() {
            Poll::Ready(None)
        } else {
            let PruneRemotes {
                next_id,
                delay,
                remote_ids,
            } = self.get_mut();
            ready!(delay.poll_unpin(cx));
            let result = next_id.take();
            if let Some((id, timeout_at)) = remote_ids.pop_front() {
                *next_id = Some(id);
                delay.as_mut().reset(timeout_at);
            }
            Poll::Ready(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PruneRemotes;
    use futures::StreamExt;
    use pin_utils::pin_mut;
    use tokio::time::Instant;
    use uuid::Uuid;

    const TIMEOUT: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn single_id() {
        let delay = tokio::time::sleep(Duration::ZERO);
        pin_mut!(delay);
        let mut prune_remotes = PruneRemotes::new(delay);
        assert!(prune_remotes.is_empty());
        assert!(prune_remotes.next().await.is_none());

        prune_remotes.push(Uuid::from_u128(7473), TIMEOUT);
        assert!(!prune_remotes.is_empty());

        let result = prune_remotes.next().await;
        assert_eq!(result, Some(Uuid::from_u128(7473)));
        assert!(prune_remotes.is_empty());
        assert!(prune_remotes.next().await.is_none());
    }

    #[tokio::test]
    async fn multiple_ids() {
        let delay = tokio::time::sleep(Duration::ZERO);
        pin_mut!(delay);
        let mut prune_remotes = PruneRemotes::new(delay);

        let ids = vec![
            Uuid::from_u128(858383),
            Uuid::from_u128(3737645834),
            Uuid::from_u128(8474747422),
        ];

        assert!(prune_remotes.is_empty());
        let start = Instant::now();
        for id in &ids {
            prune_remotes.push(*id, TIMEOUT);
            assert!(!prune_remotes.is_empty());
        }

        let results = (&mut prune_remotes).collect::<Vec<_>>().await;
        let between = Instant::now().duration_since(start);
        assert!(prune_remotes.is_empty());
        assert_eq!(results, ids);
        assert!(between >= TIMEOUT);
    }
}
