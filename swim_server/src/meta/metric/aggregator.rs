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

use crate::meta::metric::{AggregatorError, AggregatorErrorKind, AggregatorKind};
use crate::meta::metric::{STOP_CLOSED, STOP_OK};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::StreamExt;
use futures::{select, Stream};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use utilities::sync::trigger;

const FWD_FAIL: &str = "Failed to forward metric";

pub trait Addressed {
    type Tag: Debug;

    fn address(&self) -> &Self::Tag;
}

pub trait MetricAggregator {
    const AGGREGATOR_KIND: AggregatorKind;

    type Input;
    type Output: Addressed;

    fn on_receive(&mut self, profile: Self::Input) -> BoxFuture<Result<Option<Self::Output>, ()>>;
}

pub struct AggregatorTask<C, S>
where
    C: MetricAggregator,
{
    node_id: String,
    stop_rx: trigger::Receiver,
    inner: C,
    input: S,
    output: Option<mpsc::Sender<C::Output>>,
}

impl<C, S> AggregatorTask<C, S>
where
    C: MetricAggregator,
    S: Stream<Item = C::Input> + Unpin,
{
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        inner: C,
        input: S,
        output: Option<mpsc::Sender<C::Output>>,
    ) -> AggregatorTask<C, S> {
        AggregatorTask {
            node_id,
            stop_rx,
            inner,
            input,
            output,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        let AggregatorTask {
            node_id,
            stop_rx,
            mut inner,
            input,
            output,
        } = self;

        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let error = loop {
            let event: Option<C::Input> = select! {
                _ = fused_trigger => {
                    event!(Level::WARN, %node_id, STOP_OK);
                    return Ok(());
                },
                metric = fused_metric_rx.next() => metric,
            };
            match event {
                None => {
                    event!(Level::WARN, %node_id, STOP_CLOSED);
                    break AggregatorErrorKind::AbnormalStop;
                }
                Some(profile) => {
                    if let Ok(Some(profile)) = inner.on_receive(profile).await {
                        if let Some(sender) = &output {
                            if let Err(e) = sender.try_send(profile) {
                                match e {
                                    TrySendError::Closed(e) => {
                                        break AggregatorErrorKind::ForwardChannelClosed;
                                    }
                                    TrySendError::Full(e) => {
                                        let address = e.address();
                                        event!(Level::DEBUG, ?address, FWD_FAIL);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        event!(Level::INFO, %error, %node_id, STOP_CLOSED);

        return Err(AggregatorError {
            aggregator: C::AGGREGATOR_KIND,
            error,
        });
    }
}
