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

use crate::router::{Router, RouterEvent};
use futures::future::{ready, Ready};
use swim_common::warp::envelope::Envelope;
use swim_model::path::AbsolutePath;
use swim_utilities::future::request::request_future::RequestError;
use tokio::sync::mpsc;

/// A mock [`Router`] where connections produce no data and all outputs are silently dropped.
pub struct StubRouter {
    specific_tx: mpsc::Sender<Envelope>,
    general_tx: mpsc::Sender<(url::Url, Envelope)>,
    inner: Vec<mpsc::Sender<RouterEvent>>,
}

impl Router for StubRouter {
    type ConnectionFut =
        Ready<Result<(mpsc::Sender<Envelope>, mpsc::Receiver<RouterEvent>), RequestError>>;

    fn connection_for(&mut self, _target: &AbsolutePath) -> Self::ConnectionFut {
        let (tx, rx) = mpsc::channel(32);
        self.inner.push(tx);
        ready(Ok((self.specific_tx.clone(), rx)))
    }

    fn general_sink(&mut self) -> mpsc::Sender<(url::Url, Envelope)> {
        self.general_tx.clone()
    }
}

impl StubRouter {
    pub fn new(
        specific_tx: mpsc::Sender<Envelope>,
        general_tx: mpsc::Sender<(url::Url, Envelope)>,
    ) -> Self {
        StubRouter {
            specific_tx,
            general_tx,
            inner: vec![],
        }
    }
}
