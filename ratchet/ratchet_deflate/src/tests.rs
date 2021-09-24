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

use crate::handshake::apply_headers;
use crate::DeflateConfig;
use http::header::SEC_WEBSOCKET_EXTENSIONS;
use http::HeaderMap;

fn test_headers(config: DeflateConfig, expected: &str) {
    let mut header_map = HeaderMap::new();
    apply_headers(&mut header_map, &config);

    match header_map.get(SEC_WEBSOCKET_EXTENSIONS) {
        Some(header) => {
            let value = header.to_str().expect("Malformatted header");
            assert_eq!(value, expected)
        }
        None => {
            panic!("Missing {} header", SEC_WEBSOCKET_EXTENSIONS)
        }
    }
}

#[test]
fn applies_headers() {
    test_headers(
        DeflateConfig {
            server_max_window_bits: 15,
            client_max_window_bits: 15,
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: false,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: 15,
            client_max_window_bits: 7,
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: false,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits=7; server_max_window_bits=15",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: 15,
            client_max_window_bits: 7,
            request_server_no_context_takeover: true,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits=7; server_max_window_bits=15; server_no_context_takeover; client_no_context_takeover",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: 15,
            client_max_window_bits: 15,
            request_server_no_context_takeover: true,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: 15,
            client_max_window_bits: 15,
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits; client_no_context_takeover",
    );
}
