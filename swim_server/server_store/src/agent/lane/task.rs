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

use crate::agent::lane::error::{LaneStoreErrorReport, StoreErrorHandler};
use crate::agent::lane::StoreIo;
use crate::agent::NodeStore;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use swim_store::EngineInfo;
use swim_utilities::trigger;

/// Aggregated node store errors.
#[derive(Debug)]
pub enum NodeStoreErrors {
    None,
    Failed {
        /// Details about the store that generated this error report.
        store_info: EngineInfo,
        /// A vector of lane errors and the time at which they were generated.
        errors: Vec<(String, LaneStoreErrorReport)>,
    },
}

impl NodeStoreErrors {
    #[cfg(test)]
    pub fn expect_err(self) -> Vec<(String, LaneStoreErrorReport)> {
        match self {
            NodeStoreErrors::None => {
                panic!("Expected errors")
            }
            NodeStoreErrors::Failed { errors, .. } => errors,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            NodeStoreErrors::None => true,
            NodeStoreErrors::Failed { errors, .. } => errors.is_empty(),
        }
    }

    pub fn failed(&self) -> bool {
        matches!(self, NodeStoreErrors::Failed { .. })
    }

    pub fn len(&self) -> usize {
        match self {
            NodeStoreErrors::None => 0,
            NodeStoreErrors::Failed { errors, .. } => errors.len(),
        }
    }

    pub fn push(&mut self, store_info: EngineInfo, lane_uri: String, report: LaneStoreErrorReport) {
        match self {
            NodeStoreErrors::None => {
                *self = NodeStoreErrors::Failed {
                    store_info,
                    errors: vec![(lane_uri, report)],
                }
            }
            NodeStoreErrors::Failed { errors, .. } => errors.push((lane_uri, report)),
        }
    }
}

impl Default for NodeStoreErrors {
    fn default() -> Self {
        NodeStoreErrors::None
    }
}

impl Display for NodeStoreErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStoreErrors::None => {
                writeln!(f, "Node store errors:")?;
                writeln!(f, "\tfailed: false")
            }
            NodeStoreErrors::Failed { store_info, errors } => {
                writeln!(f, "Node store errors:")?;
                writeln!(f, "\tfailed: true")?;
                writeln!(f, "\tDelegate store:")?;
                writeln!(f, "\t\tPath: `{}`", store_info.path)?;
                writeln!(f, "\t\tKind: `{}`", store_info.kind)?;
                writeln!(f, "\terror reports:")?;

                for (lane_uri, report) in errors {
                    writeln!(f, "\t\t lane URI: {}", lane_uri)?;
                    writeln!(f, "\t\t report: {}", report)?;
                }

                Ok(())
            }
        }
    }
}

/// Node store task manager which attaches lane store IO and aggregates any lane store IO errors.
pub struct NodeStoreTask<Store> {
    /// A stop trigger to run lane IO tasks until.
    stop_rx: trigger::Receiver,
    /// The node store which lane IO will be attached to.
    node_store: Store,
}

impl<Store> NodeStoreTask<Store> {
    pub fn new(stop_rx: trigger::Receiver, node_store: Store) -> Self {
        NodeStoreTask {
            stop_rx,
            node_store,
        }
    }
}

impl<Store: NodeStore> NodeStoreTask<Store> {
    /// Run the node store `tasks` and allow each lane to error `max_store_errors` times. Returns
    /// `Ok(empty error report)` if no tasks failed or `Err(error report)` with reports of any
    /// failed IO tasks.
    pub async fn run(
        self,
        tasks: HashMap<String, Box<dyn StoreIo>>,
        max_store_errors: usize,
    ) -> Result<NodeStoreErrors, NodeStoreErrors> {
        let NodeStoreTask {
            stop_rx,
            node_store,
        } = self;

        let pending = FuturesUnordered::new();

        for (lane_uri, io) in tasks {
            let store_error_handler = StoreErrorHandler::new(max_store_errors);

            let task = async move {
                let result = io.attach_boxed(store_error_handler).await;
                (lane_uri, result)
            };
            pending.push(task.boxed());
        }

        let tasks = pending.take_until(stop_rx).fuse();
        let info = node_store.engine_info();
        let report = tasks
            .fold(
                NodeStoreErrors::default(),
                |mut report, (lane_uri, result)| async {
                    if let Err(err) = result {
                        report.push(info.clone(), lane_uri, err);
                    }
                    report
                },
            )
            .await;

        if report.is_empty() {
            Ok(report)
        } else {
            Err(report)
        }
    }
}
