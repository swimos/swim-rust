// Copyright 2015-2023 Swim Inc.
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

use serde::Serialize;

use wasm_ir::agent::GuestRuntimeEvent;
use wasm_ir::wpc::{EnvAccess, WasmProcedure};

use crate::prelude::lanes::handlers::HandlerContext;

pub struct AgentContext<A, H> {
    handler: HandlerContext<A>,
    host: H,
}

impl<A, H> AgentContext<A, H> {
    pub fn new(handler: HandlerContext<A>, host: H) -> AgentContext<A, H> {
        AgentContext { handler, host }
    }
}

impl<A, H> Clone for AgentContext<A, H>
where
    H: Clone,
{
    fn clone(&self) -> Self {
        let AgentContext { handler, host } = self;
        AgentContext {
            handler: handler.clone(),
            host: host.clone(),
        }
    }
}

impl<A, H> AgentContext<A, H> {
    pub fn handler(&self) -> &HandlerContext<A> {
        let AgentContext { handler, .. } = self;
        handler
    }

    pub fn dispatch<P, R>(&self, request: R) -> P::Response
    where
        H: EnvAccess,
        P: WasmProcedure<Request = R>,
        R: Into<GuestRuntimeEvent>,
    {
        let AgentContext { host, .. } = self;
        host.dispatch(request.into())
    }

    pub fn satisfy<P, R>(&self, response: R)
    where
        H: EnvAccess,
        P: WasmProcedure<Response = R>,
        R: Serialize + Into<GuestRuntimeEvent>,
    {
        let AgentContext { host, .. } = self;
        host.dispatch::<GuestRuntimeEvent, ()>(response.into()) //todo
    }
}
