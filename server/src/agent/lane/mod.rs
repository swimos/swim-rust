// Copyright 2015-2020 SWIM.AI inc.
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

use form::FormDeserializeErr;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;

pub mod map;
pub mod strategy;
pub mod value;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InvalidForm(FormDeserializeErr);

impl Display for InvalidForm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lane form implementation is inconsistent: {}", self.0)
    }
}

impl Error for InvalidForm {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[pin_project]
pub struct BroadcastStream<T>(#[pin] broadcast::Receiver<T>);

impl<T: Clone> Stream for BroadcastStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projected = self.project();
        loop {
            match ready!(projected.0.as_mut().poll_next(cx)) {
                Some(Err(broadcast::RecvError::Closed)) => break Poll::Ready(None),
                Some(Err(broadcast::RecvError::Lagged(_))) => {}
                Some(Ok(t)) => break Poll::Ready(Some(t)),
                _ => break Poll::Ready(None),
            }
        }
    }
}
