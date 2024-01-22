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

pub use bincode;
pub use guest_derive::wasm_agent;

pub mod form {
    // pub use swim_form::*;
}

pub mod lanes {
    pub use crate::agent::lanes::*;
}

pub mod router {
    pub use crate::agent::{EitherRoute, EventRouter, ItemRoute};
}

pub mod ir {
    pub use bincode::*;
    pub use bytes::{BufMut, BytesMut};

    pub use wasm_ir::*;
}

pub mod host {
    pub use crate::runtime::host::*;
}

pub mod agent {
    pub use crate::agent::{
        wasm_agent, AgentContext, AgentModel, AgentSpecBuilder, Dispatcher, SwimAgent,
    };
}
