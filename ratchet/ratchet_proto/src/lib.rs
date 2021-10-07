use bytes::BytesMut;
use std::error::Error;

pub trait H1PartEncoder {
    type Error: Error;

    fn encode(self, into: &mut BytesMut) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        into.reserve(self.size_hint());
        self.encode_into(into)
    }

    fn encode_into(self, into: &mut BytesMut) -> Result<(), Self::Error>;

    fn size_hint(&self) -> usize;
}

pub trait H1PartDecoder: Sized {
    type Error: Error;

    fn decode(from: &mut BytesMut) -> Result<Self, Self::Error>;
}

pub trait H1Part: H1PartEncoder + H1PartDecoder {}
