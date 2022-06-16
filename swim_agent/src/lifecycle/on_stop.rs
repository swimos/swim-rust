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

use swim_api::handlers::{NoHandler, FnHandler};

use crate::event_handler::{EventHandler, UnitHandler};

pub trait OnStop<'a, Context>: Send {
    type OnStopHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_stop(&'a self) -> Self::OnStopHandler;
}

impl<'a, Context> OnStop<'a, Context> for NoHandler {
    type OnStopHandler = UnitHandler;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        Default::default()
    }
}

impl<'a, Context, F, H> OnStop<'a, Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'static,
{
    type OnStopHandler = H;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        let FnHandler(f) = self;
        f()
    }
}
