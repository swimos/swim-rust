use crate::errors::BoxError;
use crate::extensions::ext::NoExt;
use crate::Request;
use std::fmt::Debug;

pub mod ext;

// todo
pub struct ExtHandshakeErr(pub(crate) BoxError);

pub trait Extension: Debug {
    fn encode(&mut self);

    fn decode(&mut self);
}

pub trait ExtensionHandshake {
    type Extension: Extension;

    fn apply_headers(&self, request: &mut Request);

    fn negotiate(
        &self,
        response: &httparse::Response,
    ) -> Result<Option<Self::Extension>, ExtHandshakeErr>;
}

#[derive(Debug)]
pub enum NegotiatedExtension<E> {
    None(NoExt),
    Negotiated(E),
}

impl<E> Default for NegotiatedExtension<E> {
    fn default() -> Self {
        NegotiatedExtension::None(NoExt::default())
    }
}

impl<E> Extension for NegotiatedExtension<E>
where
    E: Extension,
{
    fn encode(&mut self) {
        match self {
            NegotiatedExtension::None(ext) => ext.encode(),
            NegotiatedExtension::Negotiated(ext) => ext.encode(),
        }
    }

    fn decode(&mut self) {
        match self {
            NegotiatedExtension::None(ext) => ext.decode(),
            NegotiatedExtension::Negotiated(ext) => ext.decode(),
        }
    }
}
