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

use crate::model::MapAction;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct MapSender<K, V> {
    inner: mpsc::Sender<MapAction<K, V>>,
}

impl<K, V> MapSender<K, V> {
    pub async fn update(&self, key: K, value: V) -> Result<(), (K, V)> {
        self.inner
            .send(MapAction::Update { key, value })
            .await
            .map_err(|e| match e.0 {
                MapAction::Update { key, value } => (key, value),
                _ => unreachable!(),
            })
    }

    pub async fn remove(&self, key: K) -> Result<(), K> {
        self.inner
            .send(MapAction::Remove { key })
            .await
            .map_err(|e| match e.0 {
                MapAction::Remove { key } => key,
                _ => unreachable!(),
            })
    }

    pub async fn clear(&self) -> Result<(), ()> {
        self.inner.send(MapAction::Clear).await.map_err(|_| ())
    }

    pub async fn take(&self, n: u64) -> Result<(), ()> {
        self.inner.send(MapAction::Take { n }).await.map_err(|_| ())
    }

    pub async fn drop(&self, n: u64) -> Result<(), ()> {
        self.inner.send(MapAction::Drop { n }).await.map_err(|_| ())
    }
}
