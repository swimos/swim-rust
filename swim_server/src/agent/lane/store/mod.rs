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

use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler};
use futures::future::BoxFuture;
use store::NodeStore;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub use tests::store_err_partial_eq;

pub mod error;
pub mod task;

/// Deferred lane store IO attachment task.
pub trait StoreIo<Store: NodeStore + Sized>: Send + 'static {
    /// Attempt to attach the lane event stream to the provided node store.
    ///
    /// # Arguments:
    /// `store`: the node store that can be used to request a value or map lane store.
    /// `lane_uri`: the URI of the lane.
    /// `error_handler`: a store error handler for reporting store error events to. The handler
    /// is already initialised with an error handling strategy.
    fn attach(
        self,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>>;

    fn attach_boxed(
        self: Box<Self>,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>>;
}

/// A transient lane store which completes immediately.
#[derive(Debug)]
pub struct LaneNoStore;
impl<Store> StoreIo<Store> for LaneNoStore
where
    Store: NodeStore,
{
    fn attach(
        self,
        _store: Store,
        _lane_uri: String,
        _error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move { Ok(()) })
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
