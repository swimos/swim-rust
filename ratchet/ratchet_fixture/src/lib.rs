pub mod duplex {
    use bytes::BytesMut;
    use ratchet::{Extension, NegotiatedExtension, Role, WebSocketConfig};
    use tokio::io::DuplexStream;

    pub type MockWebSocket<E> = ratchet::WebSocket<DuplexStream, E>;

    fn make_websocket<E>(stream: DuplexStream, role: Role, ext: E) -> MockWebSocket<E>
    where
        E: Extension,
    {
        ratchet::WebSocket::from_upgraded(
            WebSocketConfig::default(),
            stream,
            NegotiatedExtension::from(Some(ext)),
            BytesMut::new(),
            role,
        )
    }

    pub fn websocket_pair<E>(ext: E) -> (MockWebSocket<E>, MockWebSocket<E>)
    where
        E: Extension + Clone,
    {
        let (tx, rx) = tokio::io::duplex(256);
        (
            make_websocket(tx, Role::Client, ext.clone()),
            make_websocket(rx, Role::Server, ext),
        )
    }
}

pub mod ratchet_failing_ext {
    use bytes::BytesMut;
    use ratchet::{
        Extension, ExtensionDecoder, ExtensionEncoder, FrameHeader, RsvBits, SplittableExtension,
    };
    use std::error::Error;

    #[derive(Clone, Debug)]
    pub struct FailingExt<E>(pub E)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> Extension for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        fn bits(&self) -> RsvBits {
            RsvBits {
                rsv1: false,
                rsv2: false,
                rsv3: false,
            }
        }
    }

    impl<E> ExtensionEncoder for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn encode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            Err(self.0.clone())
        }
    }

    impl<E> ExtensionDecoder for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn decode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            Err(self.0.clone())
        }
    }

    impl<E> SplittableExtension for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type SplitEncoder = FailingExtEnc<E>;
        type SplitDecoder = FailingExtDec<E>;

        fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder) {
            let FailingExt(e) = self;
            (FailingExtEnc(e.clone()), FailingExtDec(e))
        }
    }

    pub struct FailingExtEnc<E>(pub E)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> ExtensionEncoder for FailingExtEnc<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn encode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            Err(self.0.clone())
        }
    }

    pub struct FailingExtDec<E>(pub E)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> ExtensionDecoder for FailingExtDec<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn decode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            Err(self.0.clone())
        }
    }
}
