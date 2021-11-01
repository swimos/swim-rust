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
    use crate::protocol::{apply_mask, DataCode, HeaderFlags};
    use crate::protocol::{FrameHeader, OpCode};
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
        let payload = payload.as_mut();

        FrameHeader::write_into(&mut bytes, opcode, flags, mask, payload.len());

        if let Some(mask) = mask {
            apply_mask(mask, payload);
        }

        bytes.extend_from_slice(payload);

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
        let text = "Bonsoir, Elliot".to_string().into_bytes();

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
            &[129, 133, 191, 180, 236, 135, 247, 209, 128, 235, 208],
        )
    }
}

#[cfg(test)]
mod decode {
    use crate::protocol::{DataCode, FrameHeader, HeaderFlags, OpCode};
    use crate::ProtocolError;
    use bytes::BytesMut;
    use either::Either;
    use std::iter::FromIterator;

    fn expect_protocol_error(
        result: Result<Either<(FrameHeader, usize, usize), usize>, ProtocolError>,
        error: ProtocolError,
    ) {
        match result {
            Err(e) => {
                assert_eq!(e, error)
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
            FrameHeader::read_from(&mut bytes, false, 0, usize::MAX)
                .unwrap()
                .unwrap_left();

        assert_eq!(
            header,
            FrameHeader {
                opcode: OpCode::DataCode(DataCode::Text),
                flags: HeaderFlags::FIN,
                mask: None
            }
        );
    }

    #[test]
    fn rsv() {
        let mut bytes = BytesMut::from_iter(&[161, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, false, 0, usize::MAX);
        expect_protocol_error(r, ProtocolError::UnknownExtension);

        let mut bytes = BytesMut::from_iter(&[161, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, false, 1 << 6 & 1 << 4, usize::MAX);
        expect_protocol_error(r, ProtocolError::UnknownExtension);

        let mut bytes = BytesMut::from_iter(&[193, 4, 1, 2, 3, 4]);
        let result = FrameHeader::read_from(&mut bytes, false, 1 << 6, usize::MAX);

        let _expected = FrameHeader {
            opcode: OpCode::DataCode(DataCode::Text),
            flags: HeaderFlags::FIN | HeaderFlags::RSV_1,
            mask: None,
        };
        assert!(matches!(result, Ok(Either::Left((_expected, _, _)))));
    }

    #[test]
    fn overflow() {
        let mut bytes = BytesMut::from_iter(&[129, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, false, 0, 1);
        expect_protocol_error(r, ProtocolError::FrameOverflow);
    }

    #[test]
    fn fragmented_control() {
        let mut bytes = BytesMut::from_iter(&[8, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, false, 0, usize::MAX);
        expect_protocol_error(r, ProtocolError::FragmentedControl);
    }

    #[test]
    fn unmasked() {
        let mut bytes = BytesMut::from_iter(&[1, 132, 0, 0, 0, 0, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, false, 0, usize::MAX);
        expect_protocol_error(r, ProtocolError::MaskedFrame);
    }

    #[test]
    fn masked_err() {
        let mut bytes = BytesMut::from_iter(&[129, 4, 1, 2, 3, 4]);
        let r = FrameHeader::read_from(&mut bytes, true, 0, usize::MAX);
        expect_protocol_error(r, ProtocolError::UnmaskedFrame);
    }
}
