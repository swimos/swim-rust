pub use guest_derive::connector;
use std::collections::HashMap;
pub use swim_form::Form;

use std::mem::forget;
use swim_recon::printer::print_recon_compact;
use wasm_ir::connector::ConnectorMessageRef;

#[derive(Debug)]
pub struct ConnectorProperties(HashMap<String, String>);

impl From<HashMap<String, String>> for ConnectorProperties {
    fn from(value: HashMap<String, String>) -> Self {
        ConnectorProperties(value)
    }
}

impl ConnectorProperties {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

pub struct ConnectorContext;

impl ConnectorContext {
    pub fn send<T>(&self, node: &str, lane: &str, data: &T)
    where
        T: Form,
    {
        extern "C" {
            fn send_buf(ptr: i32, len: i32);
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
            send_buf(ptr as i32, out_len as i32);
        }
    }
}
