use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler};
use crate::agent::lane::store::StoreIo;
use crate::agent::store::NodeStore;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use utilities::sync::trigger;

type LaneIdentifiedResult = (String, Result<(), LaneStoreErrorReport>);

/// Aggregated node store errors.
#[derive(Debug, Default)]
pub struct NodeStoreErrors {
    /// Whether one of the lanes on this node failed.
    pub failed: bool,
    /// A vector of lane errors and the time at which they were generated.
    pub errors: Vec<(String, LaneStoreErrorReport)>,
}

impl Display for NodeStoreErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let NodeStoreErrors { failed, errors } = self;

        writeln!(f, "Node store errors:")?;
        writeln!(f, "\tfailed: {}", failed)?;
        writeln!(f, "\terror reports:")?;

        for (lane_uri, report) in errors {
            writeln!(f, "\t\t lane URI: {}", lane_uri)?;
            writeln!(f, "\t\t report: {}", report)?;
        }

        Ok(())
    }
}

/// Node store task manager which attaches lane store IO and aggregates any lane store IO errors.
pub struct NodeStoreTask<Store> {
    /// Running lane store IO tasks.
    pending: FuturesUnordered<BoxFuture<'static, LaneIdentifiedResult>>,
    /// A stop trigger to run lane IO tasks until.
    stop_rx: trigger::Receiver,
    /// The node store which lane IO will be attached to.
    node_store: Store,
}

impl<Store> NodeStoreTask<Store> {
    pub fn new(stop_rx: trigger::Receiver, node_store: Store) -> Self {
        NodeStoreTask {
            pending: FuturesUnordered::default(),
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
            pending,
            stop_rx,
            node_store,
        } = self;

        for (lane_uri, io) in tasks {
            let store_error_handler =
                StoreErrorHandler::new(max_store_errors, node_store.store_info());

            let task = async move {
                let result = io.attach_boxed(lane_uri.clone(), store_error_handler).await;
                (lane_uri.clone(), result)
            };
            pending.push(task.boxed());
        }

        let tasks = pending.take_until(stop_rx).fuse();
        let report = tasks
            .fold(
                NodeStoreErrors::default(),
                |mut report, (lane_uri, result)| async {
                    if let Err(err) = result {
                        report.failed = true;
                        report.errors.push((lane_uri, err));
                    }
                    report
                },
            )
            .await;

        if report.errors.is_empty() {
            Ok(report)
        } else {
            Err(report)
        }
    }
}
