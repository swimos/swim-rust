use std::collections::HashMap;
use std::mem::{forget, ManuallyDrop};
use std::slice;

pub use bincode;

pub use guest_derive::connector;
pub use swim_form::Form;
use swim_recon::printer::print_recon_compact;
use wasm_ir::connector::ConnectorMessageRef;

pub struct ConnectorContext {
    state: *mut u8,
    state_len: usize,
    state_cap: usize,
    properties: HashMap<String, String>,
}

impl ConnectorContext {
    pub fn new(properties: HashMap<String, String>) -> ConnectorContext {
        let mut state = ManuallyDrop::new(Vec::new());

        let ptr = state.as_mut_ptr();
        let len = state.len();
        let cap = state.capacity();

        forget(state);

        ConnectorContext {
            state: ptr,
            state_len: len,
            state_cap: cap,
            properties,
        }
    }

    pub fn get_property(&self, key: &str) -> Option<String> {
        self.properties.get(key).cloned()
    }

    pub fn contains_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    pub fn replace_state(&mut self, mut buf: Vec<u8>) -> Vec<u8> {
        let ConnectorContext {
            state,
            state_len,
            state_cap,
            ..
        } = self;

        let old_state = unsafe { Vec::from_raw_parts(*state, *state_len, *state_cap) };

        let ptr = buf.as_mut_ptr();
        let len = buf.len();
        let cap = buf.capacity();

        forget(buf);

        *state = ptr;
        *state_len = len;
        *state_cap = cap;

        old_state
    }

    pub fn state(&self) -> &[u8] {
        let ConnectorContext {
            state, state_len, ..
        } = self;
        unsafe { slice::from_raw_parts(*state as *const u8, *state_len) }
    }

    pub fn state_mut(&mut self) -> &mut [u8] {
        let ConnectorContext {
            state, state_len, ..
        } = self;
        unsafe { slice::from_raw_parts_mut(*state, *state_len) }
    }

    pub fn send<T>(&self, node: &str, lane: &str, data: &T)
    where
        T: Form,
    {
        extern "C" {
            fn host_call(ptr: i32, len: i32);
        }

        let message = ConnectorMessageRef {
            node,
            lane,
            data: print_recon_compact(data).to_string(),
        };
        let mut buf = bincode::serialize(&message).expect("Serializing should be infallible");

        let ptr = buf.as_mut_ptr();
        let out_len = buf.len();
        forget(buf);

        unsafe {
            host_call(ptr as i32, out_len as i32);
        }
    }
}
