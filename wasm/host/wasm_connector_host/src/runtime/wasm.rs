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

use control_ir::ConnectorProperties;
use tokio::sync::mpsc;
use wasmtime::{
    AsContextMut, Caller, Engine, Extern, Func, Instance, Linker, Memory, Module, Store, TypedFunc,
};

use wasm_ir::connector::ConnectorMessage;
use wasm_utils::SharedMemory;

use crate::runtime::{WasmConnector, WasmConnectorFactory, WasmError};

mod vtable {
    pub const ALLOC_EXPORT: &str = "alloc";
    pub const MEMORY_EXPORT: &str = "memory";
    pub const HOST_CALL_FUNCTION: &'static str = "host_call";

    pub const INIT_PROPS_FUNCTION: &'static str = "init_context";

    pub const DISPATCH_FUNCTION: &str = "dispatch";

    /// (data ptr, data len, connector context ptr)
    pub type DispatchParams = (i32, i32, i32);
    pub type DispatchReturns = ();

    /// (data ptr, data len)
    pub type InitContextParams = (i32, i32);
    pub type InitContextReturns = i32;
}

impl WasmConnectorFactory for Engine {
    type Connector = WasmConnectorCallbackInstance;

    async fn new_connector(
        &self,
        bytes: Vec<u8>,
        channel: mpsc::Sender<ConnectorMessage>,
        properties: ConnectorProperties,
    ) -> Result<Self::Connector, WasmError> {
        WasmConnectorCallbackInstance::new(&self, bytes, channel, properties).await
    }
}

pub struct State {
    channel: mpsc::Sender<ConnectorMessage>,
    shared_memory: SharedMemory,
}

impl State {
    pub fn new(channel: mpsc::Sender<ConnectorMessage>, shared_memory: SharedMemory) -> State {
        State {
            channel,
            shared_memory,
        }
    }
}

pub struct WasmConnectorCallbackInstance {
    store: Store<State>,
    instance: Instance,
    shared_memory: SharedMemory,
    dispatch_fn: TypedFunc<vtable::DispatchParams, vtable::DispatchReturns>,
    memory: Memory,
    alloc_fn: Func,
    context_ptr: i32,
}

impl WasmConnectorCallbackInstance {
    async fn new(
        engine: &Engine,
        bytes: Vec<u8>,
        channel: mpsc::Sender<ConnectorMessage>,
        properties: ConnectorProperties,
    ) -> Result<WasmConnectorCallbackInstance, WasmError> {
        let mut linker = Linker::new(engine);
        let module = Module::new(engine, bytes)?;

        let mut shared_memory = SharedMemory::default();
        let mut store = Store::new(engine, State::new(channel, shared_memory.clone()));

        let guest_call_import = module
            .imports()
            .find(|import| import.name().eq(vtable::HOST_CALL_FUNCTION))
            .ok_or_else(|| WasmError::MissingExport(vtable::HOST_CALL_FUNCTION.to_string()))?;

        linker.func_wrap2_async(
            guest_call_import.module(),
            guest_call_import.name(),
            move |mut caller: Caller<State>, ptr: i32, len: i32| {
                let state = caller.data();
                let channel = state.channel.clone();
                let task = async move {
                    match caller.get_export(vtable::MEMORY_EXPORT) {
                        Some(Extern::Memory(memory)) => {
                            let mut bytes = vec![0u8; len as u32 as usize];
                            memory.read(caller.as_context_mut(), ptr as usize, &mut bytes)?;

                            let event: ConnectorMessage = bincode::deserialize(bytes.as_ref())?;
                            channel.send(event).await.expect("Runtime stopped");

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

        let instance = linker.instantiate_async(&mut store, &module).await?;
        let dispatch_fn = instance
            .get_typed_func::<vtable::DispatchParams, vtable::DispatchReturns>(
                &mut store,
                vtable::DISPATCH_FUNCTION,
            )?;

        let memory = instance
            .get_memory(&mut store, vtable::MEMORY_EXPORT)
            .ok_or_else(|| wasmtime::Error::msg("Missing memory"))?;
        let alloc_fn = instance
            .get_func(&mut store, vtable::ALLOC_EXPORT)
            .ok_or_else(|| wasmtime::Error::msg("Missing alloc"))?;

        let init_context_fn = instance
            .get_typed_func::<vtable::InitContextParams, vtable::InitContextReturns>(
                &mut store,
                vtable::INIT_PROPS_FUNCTION,
            )?;

        let properties_buf =
            bincode::serialize(&properties).expect("Failed to serialize connector context");
        let len = properties_buf.len();
        let data_ptr = unsafe {
            shared_memory
                .write(&mut store, &memory, &alloc_fn, properties_buf)
                .await?
        };

        let context_ptr = init_context_fn
            .call_async(&mut store, (data_ptr as i32, len as i32))
            .await?;

        Ok(WasmConnectorCallbackInstance {
            store,
            instance,
            shared_memory,
            dispatch_fn,
            memory,
            alloc_fn,
            context_ptr,
        })
    }
}

impl WasmConnector for WasmConnectorCallbackInstance {
    async fn dispatch(&mut self, data: &[u8]) -> Result<(), WasmError> {
        let len = data.len();
        let data_ptr = unsafe {
            self.shared_memory
                .write(&mut self.store, &self.memory, &self.alloc_fn, data)
                .await?
        };

        self.dispatch_fn
            .call_async(
                &mut self.store,
                (data_ptr as i32, len as i32, self.context_ptr),
            )
            .await?;

        Ok(())
    }
}
