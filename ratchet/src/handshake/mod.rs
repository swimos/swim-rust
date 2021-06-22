mod client;
pub use client::exec_client_handshake;
use std::error::Error;

const WEBSOCKET: &str = "websocket";
const UPGRADE: &str = "upgrade";
const WEBSOCKET_VERSION: u8 = 13;

pub struct RequestError(pub(crate) Box<dyn Error + Send + Sync>);
