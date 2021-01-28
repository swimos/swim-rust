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

mod config;
mod lane;
mod node;
mod uplink;

use std::collections::HashMap;

use crate::agent::meta::metric::config::MetricCollectorConfig;
use crate::agent::meta::metric::lane::{LaneEvent, LaneProfile};
use crate::agent::meta::metric::node::{NodeEvent, NodeProfile};
use crate::agent::meta::metric::uplink::{UplinkEvent, UplinkObserver, UplinkProfile};
use std::num::NonZeroUsize;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use utilities::sync::trigger;

type Observer<P> = mpsc::Receiver<P>;
pub type Sender<P> = mpsc::Sender<P>;

#[derive(Form, Clone, PartialEq, Hash, Eq)]
enum MetricKind {
    Node,
    Lane(String),
    Uplink(String),
}

pub enum ObserverKind {
    Node(Observer<NodeEvent>),
    Lane(Observer<LaneEvent>),
    Uplink(Observer<UplinkEvent>),
}

pub struct MetricCollector {
    observers: HashMap<MetricKind, ObserverKind>,
    stop_rx: trigger::Receiver,
    config: MetricCollectorConfig,
}

impl MetricCollector {
    pub fn new(stop_rx: trigger::Receiver, config: MetricCollectorConfig) -> MetricCollector {
        MetricCollector {
            observers: HashMap::default(),
            stop_rx,
            config,
        }
    }

    pub fn uplink_observer(&mut self, address: RelativePath) -> UplinkObserver {
        let (tx, rx) = mpsc::channel(self.config.buffer_size.get());
        let observer = UplinkObserver::new(tx);
        self.observers.insert(
            MetricKind::Uplink(address.to_string()),
            ObserverKind::Uplink(rx),
        );

        observer
    }
}
