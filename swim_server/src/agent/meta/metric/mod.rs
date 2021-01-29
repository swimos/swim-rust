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

use tokio::sync::{mpsc, oneshot};

use swim_common::warp::path::RelativePath;
use utilities::sync::trigger;

use crate::agent::meta::metric::config::MetricCollectorConfig;
use crate::agent::meta::metric::lane::LaneProfile;
use crate::agent::meta::metric::node::NodeProfile;
use crate::agent::meta::metric::sender::TransformedSender;
use crate::agent::meta::metric::task::{
    CollectorStopResult, CollectorTask, ProfileRequest, ProfileRequestErr,
};
use crate::agent::meta::metric::uplink::{UplinkObserver, UplinkProfile, UplinkSurjection};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

mod config;
mod lane;
mod node;
mod sender;
mod task;
mod uplink;

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Clone)]
pub enum Profile {
    Node(NodeProfile),
    Lane(LaneProfile),
    Uplink(UplinkProfile),
}

#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub enum MetricKind {
    Node,
    Lane(RelativePath),
    Uplink(RelativePath),
}

#[derive(PartialEq, Debug)]
pub enum ObserverEvent {
    Node(NodeProfile),
    Lane(RelativePath, LaneProfile),
    Uplink(RelativePath, UplinkProfile),
}

pub struct MetricCollector {
    config: MetricCollectorConfig,
    metric_tx: Sender<ObserverEvent>,
    _collector_task: JoinHandle<CollectorStopResult>,
    request_tx: mpsc::Sender<ProfileRequest>,
}

impl MetricCollector {
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        config: MetricCollectorConfig,
    ) -> MetricCollector {
        let (metric_tx, metric_rx) = mpsc::channel(config.buffer_size.get());
        let (collector, request_tx) =
            CollectorTask::new(node_id, stop_rx, config.buffer_size, metric_rx);
        let jh = tokio::spawn(collector.run());

        MetricCollector {
            config,
            metric_tx,
            _collector_task: jh,
            request_tx,
        }
    }

    pub async fn request_profile(&self, request: MetricKind) -> Result<Profile, ProfileRequestErr> {
        let (tx, rx) = oneshot::channel();
        let request = ProfileRequest::new(request, tx);

        self.request_tx.send(request).await?;

        rx.await
            .map_err(|e| ProfileRequestErr::ChannelError(e.to_string()))?
    }

    pub fn uplink_observer(&self, address: RelativePath) -> UplinkObserver {
        let MetricCollector {
            config, metric_tx, ..
        } = self;

        let sender = TransformedSender::new(UplinkSurjection(address.clone()), metric_tx.clone());
        UplinkObserver::new(sender, config.sample_rate)
    }
}
