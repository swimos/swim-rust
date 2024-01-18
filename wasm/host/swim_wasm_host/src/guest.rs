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

use bytes::{BufMut, BytesMut};

use wasm_ir::requests::HostRequest;
use wasm_ir::AgentSpec;

use crate::error::WasmAgentError;
use crate::runtime::wasm::WasmError;
use crate::runtime::{WasmAgentPointer, WasmGuestRuntime};

pub async fn initialise_agent<R>(mut runtime: R) -> Result<InitialisedWasmAgent<R>, WasmAgentError>
where
    R: WasmGuestRuntime,
{
    let (ptr, spec) = runtime.init().await.unwrap();
    let spec = bincode::deserialize(&spec)?;

    Ok(InitialisedWasmAgent {
        spec,
        agent: WasmGuestAgent { ptr, runtime },
    })
}

pub struct InitialisedWasmAgent<R> {
    pub spec: AgentSpec,
    pub agent: WasmGuestAgent<R>,
}

pub struct WasmGuestAgent<R> {
    ptr: WasmAgentPointer,
    runtime: R,
}

impl<R> WasmGuestAgent<R> {
    pub async fn dispatch(&mut self, req: HostRequest) -> Result<(), WasmError>
    where
        R: WasmGuestRuntime,
    {
        let WasmGuestAgent { runtime, ptr } = self;

        let mut writer = BytesMut::new().writer();
        bincode::serialize_into(&mut writer, &req).expect("Failed to serialize host request");

        runtime.dispatch(*ptr, writer.into_inner()).await
    }

    pub fn read(&mut self) -> Result<Vec<u8>, WasmError>
    where
        R: WasmGuestRuntime,
    {
        let WasmGuestAgent { runtime, .. } = self;
        runtime.read()
    }

    pub fn stop(self)
    where
        R: WasmGuestRuntime,
    {
        let WasmGuestAgent { runtime, ptr } = self;
        runtime.stop(ptr);
    }
}
