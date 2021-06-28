mod client;
use crate::extensions::Extension;
pub use client::exec_client_handshake;
use std::error::Error;

const WEBSOCKET_STR: &str = "websocket";
const UPGRADE_STR: &str = "upgrade";
const WEBSOCKET_VERSION_STR: &str = "13";
const WEBSOCKET_VERSION: u8 = 13;
const BAD_STATUS_CODE: &str = "Invalid status code";

const ACCEPT_KEY: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug)]
pub struct RequestError(pub(crate) Box<dyn Error + Send + Sync>);

pub type ProtocolRegistry = Registry<()>;
pub type ExtensionRegistry = Registry<Box<dyn Extension>>;

pub struct Registry<T> {
    registrants: Vec<(&'static str, T)>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Registry {
            registrants: Vec::default(),
        }
    }
}

impl<T> Registry<T> {
    pub fn contains(&self, needle: &'static str) -> bool {
        self.registrants
            .iter()
            .any(|(name, _value)| name == &needle)
    }
}
