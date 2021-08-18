// Copyright 2015-2021 SWIM.AI inc.
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

#[cfg(test)]
mod encoding {
    use crate::protocol::frame::{ControlCode, DataCode, Frame, FrameHeader, OpCode};
    use crate::protocol::HeaderFlags;
    use bytes::BytesMut;

    fn encode<A>(
        flags: HeaderFlags,
        opcode: OpCode,
        mut payload: A,
        mask: Option<u32>,
        expected: &[u8],
    ) where
        A: AsMut<[u8]>,
    {
        let mut bytes = BytesMut::new();

        let header = FrameHeader::new(opcode, flags, mask);
        Frame::new(header, payload.as_mut()).write_into(&mut bytes);

        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn short_payload() {
        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            &mut [1, 2, 3, 4],
            None,
            &[129, 4, 1, 2, 3, 4],
        );
    }

    #[test]
    fn medium_payload() {
        let mut payload = [0u8; 127].to_vec();
        let mut expected = [129u8, 126, 0, 127].to_vec();
        expected.extend(payload.clone());

        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            payload.as_mut_slice(),
            None,
            expected.as_slice(),
        );
    }

    #[test]
    fn large_payload() {
        let mut payload = [0u8; 65537].to_vec();
        let mut expected = [129u8, 127, 0, 0, 0, 0, 0, 1, 0, 1].to_vec();
        expected.extend(payload.clone());

        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            payload.as_mut_slice(),
            None,
            expected.as_slice(),
        );
    }

    #[test]
    fn large_masked_payload() {
        let mut payload = [0u8; 65537].to_vec();
        let mut expected = [129u8, 255, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0].to_vec();
        expected.extend(payload.clone());

        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            payload.as_mut_slice(),
            Some(0),
            expected.as_slice(),
        );
    }

    #[test]
    fn masked() {
        let mut text = "Bonsoir, Elliot".to_string().into_bytes();

        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            text,
            Some(0),
            &[
                129, 143, 0, 0, 0, 0, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105,
                111, 116,
            ],
        );
    }

    #[test]
    fn reserved() {
        encode(
            HeaderFlags::FIN | HeaderFlags::RESERVED,
            OpCode::DataCode(DataCode::Text),
            &mut [1, 2, 3, 4],
            None,
            &[241, 4, 1, 2, 3, 4],
        );
    }

    #[test]
    fn masked_text() {
        let mut payload = "Hello".to_string();
        let payload = unsafe { payload.as_bytes_mut() };

        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            payload,
            Some(2280436927_u32),
            &[129, 133, 135, 236, 180, 191, 207, 137, 216, 211, 232],
        )
    }
}

#[cfg(test)]
mod decode {
    use crate::codec::CodecFlags;
    use crate::errors::Error;
    use crate::handshake::ProtocolError;
    use crate::protocol::frame::{DataCode, Frame, FrameHeader, OpCode};
    use crate::protocol::HeaderFlags;
    use bytes::BytesMut;
    use std::iter::FromIterator;

    fn expect_protocol_error(
        result: Result<Option<(FrameHeader, usize, usize)>, Error>,
        error: ProtocolError,
    ) {
        match result {
            Err(e) => {
                let protocol_error = e
                    .downcast_ref::<ProtocolError>()
                    .expect("Expected a protocol error");
                assert_eq!(protocol_error, &error);
            }
            o => {
                panic!("Expected a protocol error. Got: `{:?}`", o)
            }
        }
    }

    #[test]
    fn header() {
        let mut bytes = BytesMut::from_iter(&[129, 4, 1, 2, 3, 4]);
        let (header, _header_len, _payload_len) =
            FrameHeader::read_from(&mut bytes, &CodecFlags::empty(), usize::MAX)
                .unwrap()
                .unwrap();
        assert_eq!(
            header,
            FrameHeader::new(OpCode::DataCode(DataCode::Text), HeaderFlags::FIN, None)
        );
    }

    #[test]
    fn rsv() {
        let mut bytes = BytesMut::from_iter(&[161, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, &CodecFlags::RSV1, usize::MAX);
        expect_protocol_error(r, ProtocolError::UnknownExtension);

        let mut bytes = BytesMut::from_iter(&[161, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(
            &mut bytes,
            &(CodecFlags::RSV1 | CodecFlags::RSV3),
            usize::MAX,
        );
        expect_protocol_error(r, ProtocolError::UnknownExtension);

        let mut bytes = BytesMut::from_iter(&[193, 4, 1, 2, 3, 4]);
        let result = FrameHeader::read_from(&mut bytes, &CodecFlags::RSV1, usize::MAX);
        let expected = FrameHeader::new(
            OpCode::DataCode(DataCode::Text),
            HeaderFlags::FIN | HeaderFlags::RSV_1,
            None,
        );
        assert!(matches!(result, Ok(Some((expected, _, _)))));
    }

    #[test]
    fn overflow() {
        let mut bytes = BytesMut::from_iter(&[129, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, &CodecFlags::empty(), 1);
        expect_protocol_error(r, ProtocolError::FrameOverflow);
    }

    #[test]
    fn fragmented_control() {
        let mut bytes = BytesMut::from_iter(&[8, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, &CodecFlags::empty(), 1);
        expect_protocol_error(r, ProtocolError::FragmentedControl);
    }

    #[test]
    fn unmasked() {
        let mut bytes = BytesMut::from_iter(&[1, 132, 0, 0, 0, 0, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, &CodecFlags::empty(), 1);
        expect_protocol_error(r, ProtocolError::MaskedFrame);
    }

    #[test]
    fn masked_err() {
        let mut bytes = BytesMut::from_iter(&[129, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, &CodecFlags::ROLE, 1);
        expect_protocol_error(r, ProtocolError::UnmaskedFrame);
    }
}
