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

#[cfg(test)]
pub use tests::store_err_partial_eq;

#[cfg(test)]
mod tests;

pub mod error;
mod model;
pub mod task;

use crate::agent::lane::error::{LaneStoreErrorReport, StoreErrorHandler};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;

pub use model::*;

/// Deferred lane store IO attachment task.
pub trait StoreIo: Send + 'static {
    /// Attempt to attach the lane event stream to the provided node store.
    ///
    /// # Arguments:
    /// `store`: the node store that can be used to request a value or map lane store.
    /// `error_handler`: a store error handler for reporting store error events to. The handler
    /// is already initialised with an error handling strategy.
    fn attach(
        self,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>>;

    fn attach_boxed(
        self: Box<Self>,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>>;
}

/// A transient lane store which completes immediately.
#[derive(Debug)]
pub struct LaneNoStore;
impl StoreIo for LaneNoStore {
    fn attach(
        self,
        _error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        ready(Ok(())).boxed()
    }

    fn attach_boxed(
        self: Box<Self>,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(error_handler)
    }
}
