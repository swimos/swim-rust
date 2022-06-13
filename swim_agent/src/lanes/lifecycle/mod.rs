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

use std::marker::PhantomData;

use swim_api::handlers::{NoHandler, FnMutHandler};

use self::{on_event::OnEvent, on_set::OnSet};

pub mod on_event;
pub mod on_set;

pub trait ValueLaneHandlers<'a, T, Context>:
    OnEvent<'a, T, Context> + OnSet<'a, T, Context>
{
}

pub trait ValueLaneLifecycle<T, Context>: for<'a> ValueLaneHandlers<'a, T, Context> {}

impl<L, T, Context> ValueLaneLifecycle<T, Context> for L
where
    L: for<'a> ValueLaneHandlers<'a, T, Context>,
{}

impl<'a, L, T, Context> ValueLaneHandlers<'a, T, Context> for L
where
    L: OnEvent<'a, T, Context> + OnSet<'a, T, Context>,
{}

pub struct BasicValueLaneLifecycle<Context, T, FEv, FSet> {
    _value_type: PhantomData<fn(Context, T)>,
    on_event: FEv,
    on_set: FSet,
}

pub fn for_value_lane<Context, T>() -> BasicValueLaneLifecycle<Context, T, NoHandler, NoHandler> {
    BasicValueLaneLifecycle { 
        _value_type: PhantomData, 
        on_event: NoHandler, 
        on_set: NoHandler 
    }
}

impl<Context, T, FEv, FSet> BasicValueLaneLifecycle<Context, T, FEv, FSet> {

    pub fn on_event<F>(
        self,
        f: F,
    ) -> BasicValueLaneLifecycle<Context, T, FnMutHandler<F>, FSet>
    where
        FnMutHandler<F>: for<'a> OnEvent<'a, T, Context>,
    {
        BasicValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: FnMutHandler(f),
            on_set: self.on_set,
        }
    }

    pub fn on_set<F>(
        self,
        f: F,
    ) -> BasicValueLaneLifecycle<Context, T, FEv, FnMutHandler<F>>
    where
        FnMutHandler<F>: for<'a> OnSet<'a, T, Context>,
    {
        BasicValueLaneLifecycle {
            _value_type: PhantomData,
            on_event: self.on_event,
            on_set: FnMutHandler(f),
        }
    }

}

impl<'a, T, FEv, FSet, Context> OnEvent<'a, T, Context> for BasicValueLaneLifecycle<Context, T, FEv, FSet>
where
    FSet: Send,
    FEv: OnEvent<'a, T, Context>,
{
    type OnEventHandler = FEv::OnEventHandler;

    fn on_event(&'a mut self, value: &'a T) -> Self::OnEventHandler {
        self.on_event.on_event(value)
    }
}

impl<'a, T, FEv, FSet, Context> OnSet<'a, T, Context> for BasicValueLaneLifecycle<Context, T, FEv, FSet>
where
    FEv: Send,
    FSet: OnSet<'a, T, Context>,
{
    type OnSetHandler = FSet::OnSetHandler;

    fn on_set(&'a mut self, existing: Option<&'a T>, new_value: &'a T) -> Self::OnSetHandler {
        self.on_set.on_set(existing, new_value)
    }
}

