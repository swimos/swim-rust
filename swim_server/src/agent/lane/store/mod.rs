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

use crate::agent::lane::store::error::{StoreErrorHandler, StoreErrorReport};
use futures::future::BoxFuture;
use store::NodeStore;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub use tests::store_err_partial_eq;

pub mod error;
pub mod task;

pub trait StoreIo<Store: NodeStore + Sized>: Send + 'static {
    fn attach(
        self,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), StoreErrorReport>>;

    fn attach_boxed(
        self: Box<Self>,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), StoreErrorReport>>;
}

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
    ) -> BoxFuture<'static, Result<(), StoreErrorReport>> {
        Box::pin(async move { Ok(()) })
    }

    fn attach_boxed(
        self: Box<Self>,
        store: Store,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), StoreErrorReport>> {
        (*self).attach(store, lane_uri, error_handler)
    }
}
