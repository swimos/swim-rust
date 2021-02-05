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

use crate::agent::meta::metric::ObserverEvent;
use std::marker::PhantomData;
use tokio::sync::mpsc;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct SendError;

pub trait Surjection<I> {
    fn onto(&self, input: I) -> ObserverEvent;
}

impl<F, I> Surjection<I> for F
where
    F: Fn(I) -> ObserverEvent,
{
    fn onto(&self, input: I) -> ObserverEvent {
        (self)(input)
    }
}

pub struct TransformedSender<S, I>
where
    S: Surjection<I>,
{
    surjection: S,
    sender: mpsc::Sender<ObserverEvent>,
    _pd: PhantomData<I>,
}

impl<S, I> TransformedSender<S, I>
where
    S: Surjection<I>,
{
    pub fn new(surjection: S, sender: mpsc::Sender<ObserverEvent>) -> TransformedSender<S, I> {
        TransformedSender {
            surjection,
            sender,
            _pd: Default::default(),
        }
    }

    pub fn try_send(&self, message: I) -> Result<(), SendError> {
        let TransformedSender {
            surjection, sender, ..
        } = self;
        let msg = surjection.onto(message);

        sender.try_send(msg).map_err(|_| SendError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::meta::metric::uplink::{UplinkSurjection, UplinkUplinkProfile};
    use swim_common::warp::path::RelativePath;

    #[tokio::test]
    async fn test_err() {
        let path = RelativePath::new("/node", "/lane");
        let (tx, rx) = mpsc::channel(1);
        let sender = TransformedSender::new(UplinkSurjection(path.clone()), tx);
        let profile = UplinkUplinkProfile::default();

        drop(rx);

        assert_eq!(sender.try_send(profile), Err(SendError));
    }
}
