use crate::agent::lane::store::error::{StoreErrorHandler, StoreErrorReport};
use crate::agent::StoreIo;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use store::NodeStore;
use utilities::sync::trigger;

type LaneIdentifiedResult = (String, Result<(), StoreErrorReport>);

#[derive(Debug, Default)]
pub struct LaneStoreErrors {
    pub failed: bool,
    pub errors: Vec<(String, StoreErrorReport)>,
}

impl Display for LaneStoreErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneStoreErrors { failed, errors } = self;

        writeln!(f, "Store errors:")?;
        writeln!(f, "\tfailed: {}", failed)?;
        writeln!(f, "\terror reports:")?;

        for (lane_uri, report) in errors {
            writeln!(f, "\t\t lane URI: {}", lane_uri)?;
            writeln!(f, "\t\t report: {}", report)?;
        }

        Ok(())
    }
}

pub struct LaneStoreTask<Store> {
    pending: FuturesUnordered<BoxFuture<'static, LaneIdentifiedResult>>,
    stop_rx: trigger::Receiver,
    node_store: Store,
}

impl<Store> LaneStoreTask<Store> {
    pub fn new(stop_rx: trigger::Receiver, node_store: Store) -> Self {
        LaneStoreTask {
            pending: FuturesUnordered::default(),
            stop_rx,
            node_store,
        }
    }
}

impl<Store: NodeStore> LaneStoreTask<Store> {
    pub async fn run(
        self,
        tasks: HashMap<String, Box<dyn StoreIo<Store>>>,
        max_store_errors: usize,
    ) -> Result<LaneStoreErrors, LaneStoreErrors> {
        let LaneStoreTask {
            pending,
            stop_rx,
            node_store,
        } = self;

        for (lane_uri, io) in tasks {
            let store_error_handler =
                StoreErrorHandler::new(max_store_errors, node_store.store_info());

            let node_store = node_store.clone();

            let task = async move {
                let result = io
                    .attach_boxed(node_store, lane_uri.clone(), store_error_handler)
                    .await;
                (lane_uri.clone(), result)
            };
            pending.push(task.boxed());
        }

        let tasks = pending.take_until(stop_rx).fuse();
        let report = tasks
            .fold(
                LaneStoreErrors::default(),
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
