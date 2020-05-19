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

use std::borrow::Borrow;
use std::cell::RefCell;

use futures::Future;
use tokio::task::JoinHandle;

// TODO: Client context writing doing properly
thread_local! {
    static CONTEXT: RefCell<Option<SwimContext>> = RefCell::new(None)
}

pub fn swim_context() -> Option<SwimContext> {
    CONTEXT.with(|ctx| *ctx.borrow())
}

#[derive(Clone, Copy)]
pub struct SwimContext {}

impl SwimContext {
    // TODO: Set properly
    pub fn build() -> SwimContext {
        SwimContext {}
    }

    // TODO: This needs to be a crate-only function. But it needs to be accesible from the macro
    pub fn enter() {
        CONTEXT.with(|ctx| {
            let _old = ctx.borrow_mut().replace(SwimContext::build());
        });
    }

    pub fn spawn<F>(&mut self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        tokio::spawn(future)
    }
}
