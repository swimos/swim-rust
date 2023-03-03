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

use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::runtime::DownlinkRuntimeError;
use swim_utilities::trigger::promise;

pub type DownlinkOperationResult<T> = Result<T, DownlinkRuntimeError>;

/// A view over a stateful downlink.
#[derive(Debug, Clone)]
pub struct StatefulDownlinkView<T> {
    // This is intentionally its own field as to access it in the watch channel's receiver requires
    // accessing its internal mutex.
    pub current: Arc<T>,
    pub rx: watch::Receiver<Arc<T>>,
    pub tx: mpsc::Sender<T>,
    pub stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<T> StatefulDownlinkView<T> {
    /// Gets the most recent value of the downlink.
    pub fn get(&mut self) -> DownlinkOperationResult<Arc<T>> {
        if self.rx.has_changed()? {
            let inner = self.rx.borrow();
            self.current = inner.deref().clone();
        }
        Ok(self.current.clone())
    }

    /// Awaits for the value of the downlink to change and returns it.
    pub async fn await_changed(&mut self) -> DownlinkOperationResult<Arc<T>> {
        self.rx.changed().await?;
        let inner = self.rx.borrow();
        self.current = inner.deref().clone();
        Ok(self.current.clone())
    }

    /// Sets the value of the downlink to 'to'
    pub async fn set(&self, to: T) -> DownlinkOperationResult<()> {
        self.tx.send(to).await?;
        Ok(())
    }

    /// Attempts to set the value of the downlink to 'to'
    pub fn try_set(&self, to: T) -> DownlinkOperationResult<()> {
        self.tx.try_send(to)?;
        Ok(())
    }

    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.stop_rx.clone()
    }
}
