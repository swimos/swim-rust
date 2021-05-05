use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler};
use crate::agent::lane::store::StoreIo;
use crate::agent::model::value::ValueLane;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use store::NodeStore;

/// Value lane store IO task.
///
/// This task loads the backing value lane with a value from the store if it exists and then
/// stores all of the events into the delegate store. If any errors are produced, the task will fail
/// after the store error handler's maximum allowed error policy has been reached.
///
/// # Type parameters:
/// `T` - the type of the value lane.
/// `Events` - events produced by the lane.
pub struct ValueLaneStoreIo<T, Events> {
    lane: ValueLane<T>,
    events: Events,
}

impl<T, Events> ValueLaneStoreIo<T, Events> {
    pub fn new(lane: ValueLane<T>, events: Events) -> ValueLaneStoreIo<T, Events> {
        ValueLaneStoreIo { lane, events }
    }
}

impl<Store, Events, T> StoreIo<Store> for ValueLaneStoreIo<T, Events>
where
    Store: NodeStore,
    Events: Stream<Item = Arc<T>> + Unpin + Send + Sync + 'static,
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    fn attach(
        self,
        store: Store,
        lane_uri: String,
        mut error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move {
            let ValueLaneStoreIo { lane, mut events } = self;
            let model = store.value_lane_store::<_, T>(lane_uri).await;

            match model.load() {
                Ok(Some(value)) => lane.store(value).await,
                Ok(None) => {}
                Err(e) => {
                    return Err(LaneStoreErrorReport::for_error(store.store_info(), e));
                }
            };

            while let Some(event) = events.next().await {
                match model.store(&event) {
                    Ok(()) => continue,
                    Err(e) => error_handler.on_error(e)?,
                }
            }
            Ok(())
        })
    }

    fn attach_boxed(
        self: Box<Self>,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(store, lane_uri, error_handler)
    }
}
