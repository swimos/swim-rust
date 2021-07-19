use crate::extensions::{ExtHandshakeErr, Extension, ExtensionHandshake};
use crate::Request;
use httparse::Response;

pub struct DeflateHandshake;
impl ExtensionHandshake for DeflateHandshake {
    type Extension = Deflate;

    fn apply_headers(&self, _request: &mut Request) {
        todo!()
    }

    fn negotiate(&self, _response: &Response) -> Result<Option<Self::Extension>, ExtHandshakeErr> {
        Ok(Some(Deflate))
    }
}

#[derive(Debug)]
pub struct Deflate;
impl Extension for Deflate {
    fn encode(&mut self) {
        todo!()
    }

    fn decode(&mut self) {
        todo!()
    }
}
