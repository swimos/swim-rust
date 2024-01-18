use std::cell::UnsafeCell;
use std::path::Path;
use std::sync::Arc;
use wasmtime::{AsContextMut, Engine, Func, Linker, Memory, MemoryAccessError, Module, Val};

#[derive(Debug, Clone, Default)]
pub struct SharedMemory {
    inner: Arc<UnsafeCell<Option<SharedMemoryInner>>>,
}

impl SharedMemory {
    pub async unsafe fn write<A>(
        &mut self,
        store: &mut A,
        memory: &Memory,
        alloc_func: &Func,
        bytes: impl AsRef<[u8]>,
    ) -> Result<isize, wasmtime::Error>
    where
        A: AsContextMut,
        A::Data: Send,
    {
        let data = bytes.as_ref();
        let mut offset = [Val::I32(0)];

        alloc_func
            .call_async(&mut *store, &[Val::from(data.len() as i32)], &mut offset)
            .await?;

        let offset = *&offset[0].unwrap_i32() as isize;

        memory
            .data_ptr(store)
            .offset(offset)
            .copy_from(data.as_ptr(), data.len());

        Ok(offset)
    }

    pub unsafe fn set(&self, ptr: i32, len: i32, memory: Memory) {
        *self.inner.get() = Some(SharedMemoryInner { ptr, len, memory });
    }

    pub unsafe fn read(
        &mut self,
        store: &mut impl AsContextMut,
    ) -> Result<Vec<u8>, MemoryAccessError> {
        match (*self.inner.get()).take() {
            Some(SharedMemoryInner { ptr, len, memory }) => {
                let mut bytes = vec![0u8; len as u32 as usize];
                memory.read(store, ptr as usize, &mut bytes)?;

                Ok(bytes)
            }
            None => Ok(Vec::new()),
        }
    }
}

unsafe impl Send for SharedMemory {}

unsafe impl Sync for SharedMemory {}

#[derive(Debug)]
pub struct SharedMemoryInner {
    ptr: i32,
    len: i32,
    memory: Memory,
}

pub struct WasmModule<S> {
    pub engine: Engine,
    pub module: Module,
    pub linker: Linker<S>,
}

impl<S> Clone for WasmModule<S> {
    fn clone(&self) -> Self {
        WasmModule {
            engine: self.engine.clone(),
            module: self.module.clone(),
            linker: self.linker.clone(),
        }
    }
}

impl<S> WasmModule<S> {
    pub fn new(
        engine: &Engine,
        linker: Linker<S>,
        bytes: impl AsRef<[u8]>,
    ) -> Result<WasmModule<S>, wasmtime::Error> {
        let module = Module::new(engine, bytes)?;
        Ok(WasmModule {
            engine: engine.clone(),
            module,
            linker,
        })
    }

    pub fn from_file(
        engine: &Engine,
        linker: Linker<S>,
        file: impl AsRef<Path>,
    ) -> Result<WasmModule<S>, wasmtime::Error> {
        let module = Module::from_file(engine, file)?;
        Ok(WasmModule {
            engine: engine.clone(),
            module,
            linker,
        })
    }
}
