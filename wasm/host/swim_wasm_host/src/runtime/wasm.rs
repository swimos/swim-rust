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
use std::sync::Arc;

use bytes::BytesMut;
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};
use wasmtime::{
    AsContextMut, Caller, Extern, Func, Instance, Memory, MemoryAccessError, Store, TypedFunc, Val,
};

use wasm_ir::agent::GuestRuntimeEvent;
use wasm_utils::{SharedMemory, WasmModule};

use crate::runtime::{WasmAgentPointer, WasmGuestRuntime, WasmGuestRuntimeFactory};
use crate::{ResolvedAccess, WasmAgentState};

mod vtable {
    pub const ALLOC_EXPORT: &str = "alloc";
    pub const MEMORY_EXPORT: &str = "memory";
    pub const HOST_CALL_FUNCTION: &'static str = "host_call";
    pub const COPY_SCHEMA_FUNCTION: &'static str = "copy_schema";
    pub const INIT_FUNCTION: &str = "init";
    pub const STOP_FUNCTION: &str = "stop";

    pub mod dispatch {
        pub const FUNCTION_NAME: &str = "dispatch";

        /// (agent pointer, data ptr, data len)
        pub type Params = (i32, i32, i32);
        pub type Returns = ();
    }
}

pub struct UninitialisedWasmAgent {
    runtime: WasmAgentRuntime,
}

#[derive(Copy, Clone)]
enum State {
    Uninitialised,
    Initialised,
}

pub struct WasmAgentRuntime {
    module: WasmModule<WasmAgentState>,
    store: Store<WasmAgentState>,
    instance: Instance,
    shared_memory: SharedMemory,
    dispatch_fn: TypedFunc<vtable::dispatch::Params, vtable::dispatch::Returns>,
    state: State,
    memory: Memory,
    alloc_fn: Func,
}

impl WasmAgentRuntime {
    async fn new(
        module: WasmModule<WasmAgentState>,
        channel: mpsc::Sender<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    ) -> Result<WasmAgentRuntime, WasmError> {
        let WasmModule {
            engine,
            module,
            mut linker,
        } = module;

        let shared_memory = SharedMemory::default();
        let resolved = Arc::new(ResolvedAccess::new());
        let mut store = Store::new(
            &engine,
            WasmAgentState::new(channel, shared_memory.clone(), resolved.clone()),
        );

        let guest_call_import = module
            .imports()
            .find(|import| import.name().eq(vtable::HOST_CALL_FUNCTION))
            .ok_or_else(|| WasmError::MissingImport(vtable::HOST_CALL_FUNCTION.to_string()))?;
        let copy_schema_import = module
            .imports()
            .find(|import| import.name().eq(vtable::COPY_SCHEMA_FUNCTION))
            .ok_or_else(|| WasmError::MissingImport(vtable::COPY_SCHEMA_FUNCTION.to_string()))?;

        linker.func_wrap2_async(
            copy_schema_import.module(),
            copy_schema_import.name(),
            move |mut caller: Caller<WasmAgentState>, ptr: i32, len: i32| {
                let state = caller.data();
                let cb_memory = state.shared_memory.clone();

                let task = async move {
                    match caller.get_export(vtable::MEMORY_EXPORT) {
                        Some(Extern::Memory(memory)) => {
                            unsafe { cb_memory.set(ptr, len, memory) }
                            Ok(())
                        }
                        _ => {
                            Err(WasmError::MissingExport(vtable::MEMORY_EXPORT.to_string()).into())
                        }
                    }
                };

                Box::new(task)
            },
        )?;

        linker.func_wrap2_async(
            guest_call_import.module(),
            guest_call_import.name(),
            move |mut caller: Caller<WasmAgentState>, ptr: i32, len: i32| {
                let state = caller.data();
                let channel = state.channel.clone();
                let (mem, alloc) = state.resolved.get();

                // We can't access the memory export until the instance has been created so we have
                // to look it up every time.
                let task = async move {
                    let mut bytes = vec![0u8; len as u32 as usize];
                    mem.read(caller.as_context_mut(), ptr as usize, &mut bytes)?;
                    let event: GuestRuntimeEvent = bincode::deserialize(bytes.as_ref())?;

                    let (tx, rx) = oneshot::channel();
                    channel.send((event, tx)).await?;
                    let response = rx.await?;

                    let results = &mut [Val::I32(0)];
                    alloc
                        .call_async(
                            caller.as_context_mut(),
                            &[Val::I32(response.len() as i32)],
                            results,
                        )
                        .await?;

                    let ptr = results[0].clone().i32().unwrap() as u32;
                    mem.write(caller.as_context_mut(), ptr as usize, response.as_ref())?;

                    Ok(ptr)
                };

                Box::new(task)
            },
        )?;

        let instance = linker.instantiate_async(&mut store, &module).await?;
        let dispatch_fn = instance
            .get_typed_func::<vtable::dispatch::Params, vtable::dispatch::Returns>(
                &mut store,
                vtable::dispatch::FUNCTION_NAME,
            )?;

        let memory = instance
            .get_memory(&mut store, vtable::MEMORY_EXPORT)
            .ok_or_else(|| wasmtime::Error::msg("Missing memory"))?;
        let alloc_fn = instance
            .get_func(&mut store, vtable::ALLOC_EXPORT)
            .ok_or_else(|| wasmtime::Error::msg("Missing alloc"))?;

        resolved.set(memory.clone(), alloc_fn.clone());

        Ok(WasmAgentRuntime {
            module: WasmModule {
                engine,
                module,
                linker,
            },
            store,
            instance,
            shared_memory,
            dispatch_fn,
            state: State::Uninitialised,
            memory,
            alloc_fn,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("Wasmtime error: {0}")]
    Runtime(#[from] wasmtime::Error),
    #[error("Module missing import: {0}")]
    MissingImport(String),
    #[error("Module missing export: {0}")]
    MissingExport(String),
    #[error("Memory access error: {0}")]
    Memory(#[from] MemoryAccessError),
    #[error("Malformatted intermediate representation: {0}")]
    Ir(#[from] bincode::Error),
}

impl WasmGuestRuntime for WasmAgentRuntime {
    async fn init(&mut self) -> Result<(WasmAgentPointer, Vec<u8>), WasmError> {
        match self.state {
            State::Uninitialised => {
                let func = {
                    self.instance
                        .get_func(&mut self.store, vtable::INIT_FUNCTION)
                        .ok_or_else(|| {
                            WasmError::MissingExport(vtable::INIT_FUNCTION.to_string())
                        })?
                };

                let mut agent_ptr = [Val::I32(0)];
                func.call_async(&mut self.store, &[], &mut agent_ptr)
                    .await?;

                let spec = unsafe { self.shared_memory.read(&mut self.store)? };

                self.state = State::Initialised;

                Ok((WasmAgentPointer(agent_ptr[0].i32().unwrap()), spec))
            }
            State::Initialised => {
                panic!("Agent already initialised")
            }
        }
    }

    fn dispatch(
        &mut self,
        agent_ptr: WasmAgentPointer,
        data: BytesMut,
    ) -> impl Future<Output = Result<(), WasmError>> + Send {
        async move {
            match self.state {
                State::Uninitialised => {
                    panic!("Agent not initialised")
                }
                State::Initialised => {
                    let len = data.len();
                    let data_ptr = unsafe {
                        self.shared_memory
                            .write(&mut self.store, &self.memory, &self.alloc_fn, data)
                            .await?
                    };

                    self.dispatch_fn
                        .call_async(&mut self.store, (agent_ptr.0, data_ptr as i32, len as i32))
                        .await?;

                    Ok(())
                }
            }
        }
    }

    async fn stop(mut self, ptr: WasmAgentPointer) -> Result<(), WasmError> {
        match self.state {
            State::Uninitialised => {
                panic!("Agent not initialised")
            }
            State::Initialised => {
                let func = {
                    self.instance
                        .get_func(&mut self.store, vtable::STOP_FUNCTION)
                        .ok_or_else(|| {
                            WasmError::MissingExport(vtable::STOP_FUNCTION.to_string())
                        })?
                };

                let mut agent_ptr = [Val::I32(ptr.0)];
                func.call_async(&mut self.store, &[], &mut agent_ptr)
                    .await?;
                Ok(())
            }
        }
    }
}

impl WasmGuestRuntimeFactory for WasmModule<WasmAgentState> {
    type AgentRuntime = WasmAgentRuntime;

    fn new_instance(
        &self,
        channel: mpsc::Sender<(GuestRuntimeEvent, oneshot::Sender<BytesMut>)>,
    ) -> impl Future<Output = Result<Self::AgentRuntime, WasmError>> + Send + 'static {
        WasmAgentRuntime::new(self.clone(), channel)
    }
}
