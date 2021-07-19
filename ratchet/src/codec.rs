use crate::errors::Error;
use crate::protocol::Message;
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

pub struct Codec;

impl Encoder<Message> for Codec {
    type Error = Error;

    fn encode(&mut self, _item: Message, _dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!()
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}
