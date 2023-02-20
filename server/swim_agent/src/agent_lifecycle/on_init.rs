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

use swim_api::handlers::{FnHandler, NoHandler};

use crate::{event_handler::ActionContext, meta::AgentMetadata};

pub trait OnInit<Context>: Send {
    /// Provides an opportunity for the lifecycle to perform any initial setup. This will be
    /// called immediately before the `on_start` event handler is executed.
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    );
}

pub trait OnInitShared<Context, Shared>: Send {
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    );
}

impl<Context> OnInit<Context> for NoHandler {
    fn initialize(
        &self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) {
    }
}

impl<Context, Shared> OnInitShared<Context, Shared> for NoHandler {
    fn initialize(
        &self,
        _shared: &Shared,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) {
    }
}

impl<Context, F> OnInit<Context> for FnHandler<F>
where
    F: Fn(&mut ActionContext<Context>, AgentMetadata, &Context) + Send,
{
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let FnHandler(f) = self;
        f(action_context, meta, context)
    }
}

impl<Context, Shared, F> OnInitShared<Context, Shared> for FnHandler<F>
where
    F: Fn(&Shared, &mut ActionContext<Context>, AgentMetadata, &Context) + Send,
{
    fn initialize(
        &self,
        shared: &Shared,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let FnHandler(f) = self;
        f(shared, action_context, meta, context)
    }
}
