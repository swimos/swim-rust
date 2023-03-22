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

use std::{sync::Arc, time::Duration};

use parking_lot::{Condvar, Mutex};

#[derive(Debug)]
struct State<T> {
    data: Option<T>,
    sender_dropped: bool,
}

#[derive(Debug)]
struct Inner<T> {
    state: Mutex<State<T>>,
    cvar: Condvar,
}

impl<T> Default for Inner<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                data: None,
                sender_dropped: false,
            }),
            cvar: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        let mut guard = self.inner.state.lock();
        guard.data = Some(value);
        self.inner.cvar.notify_one();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut guard = self.inner.state.lock();
        guard.sender_dropped = true;
        self.inner.cvar.notify_one();
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug)]
pub enum ReceiveError {
    SenderDropped,
    TimedOut,
}

impl<T> Receiver<T> {
    pub fn recv(self, timeout: Duration) -> Result<T, ReceiveError> {
        let mut guard = self.inner.state.lock();
        if let Some(value) = guard.data.take() {
            Ok(value)
        } else if guard.sender_dropped {
            Err(ReceiveError::SenderDropped)
        } else if self.inner.cvar.wait_for(&mut guard, timeout).timed_out() {
            Err(ReceiveError::TimedOut)
        } else {
            guard.data.take().ok_or(ReceiveError::SenderDropped)
        }
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::default());
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}
