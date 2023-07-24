// Copyright 2015-2023 Swim Inc.
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

use bytes::{Bytes, BytesMut};
use http::Uri;
use swim_model::http::{HttpRequest, Method, StandardHeaderName, Version};
use tokio_util::codec::{Decoder, Encoder};

use super::{HttpRequestMessage, HttpRequestMessageCodec};

fn round_trip_requests(msgs: Vec<HttpRequestMessage>) {
    let mut buffer = BytesMut::new();
    let mut codec = HttpRequestMessageCodec;

    for msg in msgs.clone() {
        assert!(codec.encode(msg.clone(), &mut buffer).is_ok());
    }

    let mut restored = vec![];

    for _ in 0..msgs.len() {
        let restored_msg = codec
            .decode_eof(&mut buffer)
            .expect("Decode failed.")
            .expect("Message incomplete.");
        restored.push(restored_msg);
    }

    assert_eq!(restored, msgs);
}

#[test]
fn request_encoding() {
    let uri = Uri::try_from("http://www.example.com:8080/path?query").unwrap();

    let headers = vec![
        (StandardHeaderName::MaxForwards, "6").into(),
        ("my_header", "some text").into(),
    ];
    let payload = Bytes::from(b"payload".as_ref());

    let request = HttpRequestMessage {
        request_id: 84823929,
        request: HttpRequest {
            method: Method::GET,
            version: Version::HTTP_1_1,
            uri,
            headers,
            payload,
        },
    };

    round_trip_requests(vec![request]);
}

#[test]
fn request_encoding_twice() {
    let uri1 = Uri::try_from("http://www.example.com:8080/path?query").unwrap();
    let uri2 = Uri::try_from("http://www.example.com:8080/path?other").unwrap();

    let headers1 = vec![
        (StandardHeaderName::MaxForwards, "6").into(),
        ("my_header", "some text").into(),
    ];
    let headers2 = vec![
        ("my_other_header", "some more text").into(),
        (StandardHeaderName::Cookie, "9384747BE*").into(),
        ("my_header", "some text").into(),
    ];
    let payload1 = Bytes::from(b"first".as_ref());
    let payload2 = Bytes::from(b"second".as_ref());

    let request1 = HttpRequestMessage {
        request_id: 84823929,
        request: HttpRequest {
            method: Method::GET,
            version: Version::HTTP_1_1,
            uri: uri1,
            headers: headers1,
            payload: payload1,
        },
    };

    let request2 = HttpRequestMessage {
        request_id: 284847848,
        request: HttpRequest {
            method: Method::PUT,
            version: Version::HTTP_1_1,
            uri: uri2,
            headers: headers2,
            payload: payload2,
        },
    };

    round_trip_requests(vec![request1, request2]);
}
