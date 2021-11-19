// Copyright 2015-2021 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod duplex {
    use bytes::BytesMut;
    use ratchet::{Extension, NegotiatedExtension, Role, WebSocketConfig};
    use tokio::io::DuplexStream;

    pub type MockWebSocket<E> = ratchet::WebSocket<DuplexStream, E>;

    pub fn make_websocket<E>(stream: DuplexStream, role: Role, ext: E) -> MockWebSocket<E>
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

    pub fn websocket_pair<L, R>(left_ext: L, right_ext: R) -> (MockWebSocket<L>, MockWebSocket<R>)
    where
        L: Extension,
        R: Extension,
    {
        let (tx, rx) = tokio::io::duplex(1024);
        (
            make_websocket(tx, Role::Client, left_ext),
            make_websocket(rx, Role::Server, right_ext),
        )
    }

    pub async fn websocket_for<E>(role: Role, ext: E) -> (MockWebSocket<E>, DuplexStream)
    where
        E: Extension,
    {
        let (tx, rx) = tokio::io::duplex(4096);
        (
            ratchet::WebSocket::from_upgraded(
                WebSocketConfig::default(),
                tx,
                NegotiatedExtension::from(Some(ext)),
                BytesMut::new(),
                role,
            ),
            rx,
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
