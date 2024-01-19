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

use std::future::Future;

use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot};

use crate::runtime::wasm::WasmError;
use crate::GuestRuntimeEvent;

pub mod wasm;

#[derive(Copy, Clone, Debug)]
pub struct WasmAgentPointer(pub i32);

pub trait WasmGuestRuntimeFactory: Send + Sync + Clone + 'static {
    type AgentRuntime: WasmGuestRuntime;

    fn new_instance(
        &self,
        channel: mpsc::Sender<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    ) -> impl Future<Output = Result<Self::AgentRuntime, WasmError>> + Send + 'static;
}

pub trait WasmGuestRuntime: Send + 'static {
    fn init(
        &mut self,
    ) -> impl Future<Output = Result<(WasmAgentPointer, Vec<u8>), WasmError>> + Send;

    fn dispatch(
        &mut self,
        ptr: WasmAgentPointer,
        data: BytesMut,
    ) -> impl Future<Output = Result<(), WasmError>> + Send;

    fn stop(self, ptr: WasmAgentPointer) -> impl Future<Output = Result<(), WasmError>> + Send;
}
