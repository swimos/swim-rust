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
    use crate::protocol::frame::{write_into, DataCode, OpCode};
    use crate::protocol::HeaderFlags;
    use bytes::BytesMut;

    fn encode<A>(flags: HeaderFlags, opcode: OpCode, payload: A, mask: u32, expected: &[u8])
    where
        A: AsMut<[u8]>,
    {
        let mut bytes = BytesMut::new();
        write_into(&mut bytes, flags, opcode, payload, mask);

        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn short_payload() {
        encode(
            HeaderFlags::FIN,
            OpCode::DataCode(DataCode::Text),
            &mut [1, 2, 3, 4],
            1,
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
            0,
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
            0,
            expected.as_slice(),
        );
    }

    #[test]
    fn large_masked_payload() {
        let mut payload = [0u8; 65537].to_vec();
        let mut expected = [129u8, 255, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0].to_vec();
        expected.extend(payload.clone());

        encode(
            HeaderFlags::FIN | HeaderFlags::MASKED,
            OpCode::DataCode(DataCode::Text),
            payload.as_mut_slice(),
            0,
            expected.as_slice(),
        );
    }

    #[test]
    fn masked() {
        let mut text = "Bonsoir, Elliot".to_string().into_bytes();

        encode(
            HeaderFlags::FIN | HeaderFlags::MASKED,
            OpCode::DataCode(DataCode::Text),
            text,
            0,
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
            1,
            &[241, 4, 1, 2, 3, 4],
        );
    }
}
