pub use guest_derive::connector;
use std::mem::forget;
use swim_form::Form;
use swim_recon::printer::print_recon_compact;
use wasm_ir::connector::ConnectorMessageRef;

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
