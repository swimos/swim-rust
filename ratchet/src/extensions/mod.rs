use crate::errors::BoxError;
use crate::{Request, Response};
use std::fmt::Debug;

pub mod ext;

pub struct ExtHandshakeErr(pub(crate) BoxError);

pub trait Extension: Debug {
    fn encode(&mut self);

    fn decode(&mut self);
}

pub trait ExtensionHandshake {
    type Extension: Extension;

    fn apply_headers(&self, request: &mut Request);

    fn negotiate(&self, response: &httparse::Response) -> Result<Self::Extension, ExtHandshakeErr>;
}
