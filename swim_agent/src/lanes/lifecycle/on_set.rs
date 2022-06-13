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

use swim_api::handlers::{NoHandler, FnMutHandler};

use crate::event_handler::{EventHandler, UnitHandler};

pub trait OnSet<'a, T, Context>: Send {

    type OnSetHandler: EventHandler<Context, Completion = ()> + Send + 'a;
    /// #Arguments
    /// * `existing` - The existing value, if it is defined.
    /// * `new_value` - The replacement value.
    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetHandler;
}

impl<'a, T, Context> OnSet<'a, T, Context> for NoHandler {
    type OnSetHandler = UnitHandler;

    fn on_set(&'a mut self, _existing: Option<&'a T>, _new_value: &'a T) -> Self::OnSetHandler {
        Default::default()
    }
}

impl<'a, T, Context, F, H> OnSet<'a, T, Context> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(Option<&'a T>, &'a T) -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'a,  {
    type OnSetHandler = H;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetHandler {
        let FnMutHandler(f) = self;
        f(existing, new_value)
    }
}