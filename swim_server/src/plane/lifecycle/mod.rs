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

use crate::plane::context::PlaneContext;
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use std::fmt::Debug;

/// Custom lifecycle event handlers for a plane.
pub trait PlaneLifecycle: Debug + Send {
    /// Called concurrently when the plane starts.
    fn on_start<'a>(&'a mut self, _context: &'a mut dyn PlaneContext) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    /// Called after the plane has stopped.
    fn on_stop(&mut self) -> BoxFuture<()> {
        ready(()).boxed()
    }

    fn boxed(self) -> Box<dyn PlaneLifecycle>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
