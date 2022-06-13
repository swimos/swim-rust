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

use crate::event_handler::{UnitHandler, EventHandler};

pub trait OnEvent<'a, T, Context>: Send {

    type OnEventHandler: EventHandler<Context, Completion = ()> + Send + 'a;
   
    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventHandler;
}

impl<'a, T, Context> OnEvent<'a, T, Context> for NoHandler {
    type OnEventHandler = UnitHandler;

    fn on_event(&'a mut self, _value: &'a T) -> Self::OnEventHandler {
        Default::default()
    }
}

impl<'a, T, Context, F, H> OnEvent<'a, T, Context> for FnMutHandler<F>
where
    T: 'static,
    F: FnMut(&'a T) -> H + Send,
    H: EventHandler<Context, Completion = ()> + Send + 'a,  {
    type OnEventHandler = H;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventHandler {
        let FnMutHandler(f) = self;
        f(value)
    }
}