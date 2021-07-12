use crate::extensions::{ExtHandshakeErr, Extension, ExtensionHandshake};
use crate::Request;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum WebsocketExtension {
    None,
    Deflate,
}

pub struct NoExtProxy;
impl ExtensionHandshake for NoExtProxy {
    type Extension = NoExt;

    fn apply_headers(&self, _request: &mut Request) {}

    fn negotiate(
        &self,
        _response: &httparse::Response,
    ) -> Result<Option<Self::Extension>, ExtHandshakeErr> {
        Ok(Some(NoExt))
    }
}

#[derive(Debug, Default)]
pub struct NoExt;
impl Extension for NoExt {
    fn encode(&mut self) {}

    fn decode(&mut self) {}
}
