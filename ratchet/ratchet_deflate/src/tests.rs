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

use crate::error::DeflateExtensionError;
use crate::handshake::{apply_headers, on_request, on_response, NegotiationErr};
use crate::{DeflateConfig, InitialisedDeflateConfig, WindowBits};
use flate2::Compression;
use http::header::SEC_WEBSOCKET_EXTENSIONS;
use http::HeaderMap;
use ratchet_ext::Header;

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
            server_max_window_bits: WindowBits::fifteen(),
            client_max_window_bits: WindowBits::fifteen(),
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: false,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: WindowBits::fifteen(),
            client_max_window_bits: WindowBits::eight(),
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: false,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits=8; server_max_window_bits=15",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: WindowBits::fifteen(),
            client_max_window_bits: WindowBits::eight(),
            request_server_no_context_takeover: true,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits=8; server_max_window_bits=15; server_no_context_takeover; client_no_context_takeover",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: WindowBits::fifteen(),
            client_max_window_bits: WindowBits::fifteen(),
            request_server_no_context_takeover: true,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
    );
    test_headers(
        DeflateConfig {
            server_max_window_bits: WindowBits::fifteen(),
            client_max_window_bits: WindowBits::fifteen(),
            request_server_no_context_takeover: false,
            request_client_no_context_takeover: true,
            accept_no_context_takeover: false,
            compression_level: Default::default(),
        },
        "permessage-deflate; client_max_window_bits; client_no_context_takeover",
    );
}

#[test]
fn request_negotiates_nothing() {
    match on_request(&[], &DeflateConfig::default()) {
        Err(NegotiationErr::Failed) => {}
        _ => panic!("Expected no extension"),
    }
}

fn request_test_valid_default(headers: &[Header]) {
    match on_request(headers, &DeflateConfig::default()) {
        Ok((config, header)) => {
            let value = header.to_str().expect("Malformatted header produced");
            assert_eq!(
                value,
                "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            );
            assert_eq!(
                config,
                InitialisedDeflateConfig {
                    server_max_window_bits: WindowBits::fifteen(),
                    client_max_window_bits: WindowBits::fifteen(),
                    compress_reset: true,
                    decompress_reset: true,
                    compression_level: Compression::fast()
                }
            )
        }
        e => panic!("Expected a valid config. Got: {:?}", e),
    }
}

#[test]
fn request_negotiates_default_spaces() {
    request_test_valid_default(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
        }]
    );
    request_test_valid_default(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate;         client_max_window_bits    ; server_no_context_takeover      ;     client_no_context_takeover",
        }]
    );
}

#[test]
fn request_negotiates_no_spaces() {
    request_test_valid_default(
        &[Header {
        name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
        value: b"permessage-deflate;client_max_window_bits;server_no_context_takeover;client_no_context_takeover",
        }]
    );
}

#[test]
fn request_unknown_header() {
    match on_request(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-bzip",
        }],
        &DeflateConfig::default(),
    ) {
        Err(NegotiationErr::Failed) => {}
        _ => panic!("Expected no extension"),
    }
}

#[test]
fn request_mixed_headers_with_unknown() {
    let headers = &[
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-bzip",
        },
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
        }
    ];

    request_test_valid_default(headers);
}

#[test]
fn request_mixed_headers_with_unnegotiable() {
    let headers = &[
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-bzip",
        },
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; client_max_window_bits=7; server_max_window_bits=8; server_no_context_takeover; client_no_context_takeover",
        },
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
        }
    ];

    request_test_valid_default(headers);
}

#[test]
fn request_truncated_headers() {
    request_test_valid_default(&[
        Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; client_max_window_bits=7; server_max_window_bits=8; server_no_context_takeover; client_no_context_takeover,                   permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",        }
    ])
}

#[test]
fn request_no_accept_no_context_takeover() {
    let header = Header {
        name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
        value: b"permessage-deflate; client_max_window_bits=7; server_max_window_bits=8; server_no_context_takeover; client_no_context_takeover,                   permessage-deflate; client_max_window_bits; server_no_context_takeover; client_no_context_takeover",
    };
    let config = DeflateConfig {
        server_max_window_bits: WindowBits::fifteen(),
        client_max_window_bits: WindowBits::fifteen(),
        request_server_no_context_takeover: true,
        request_client_no_context_takeover: true,
        accept_no_context_takeover: false,
        compression_level: Compression::fast(),
    };

    match on_request(&[header], &config) {
        Ok((config, header)) => {
            let value = header.to_str().expect("Malformatted header produced");
            assert_eq!(value, "permessage-deflate; client_no_context_takeover");
            assert_eq!(
                config,
                InitialisedDeflateConfig {
                    server_max_window_bits: WindowBits::fifteen(),
                    client_max_window_bits: WindowBits::fifteen(),
                    compress_reset: false,
                    decompress_reset: true,
                    compression_level: Compression::fast()
                }
            )
        }
        e => panic!("Expected a valid config. Got: {:?}", e),
    }
}

fn request_test_malformatted_default(headers: &[Header], expected: DeflateExtensionError) {
    match on_request(headers, &DeflateConfig::default()) {
        Err(NegotiationErr::Err(e)) => assert_eq!(e.to_string(), expected.to_string()),
        e => panic!("Expected: `{:?}`. Got: {:?}", expected, e),
    }
}

#[test]
fn request_malformatted_window_bits() {
    request_test_malformatted_default(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value:
                b"permessage-deflate; client_max_window_bits=2.71828; server_max_window_bits=3.14159",
        }],
        DeflateExtensionError::InvalidMaxWindowBits,
    );
    request_test_malformatted_default(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value:
                b"permessage-deflate; client_max_window_bits=666; server_max_window_bits=3.14159",
        }],
        DeflateExtensionError::InvalidMaxWindowBits,
    )
}

#[test]
fn request_unknown_parameter() {
    request_test_malformatted_default(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate; peer_max_window_bits",
        }],
        DeflateExtensionError::NegotiationError(
            "Unknown permessage-deflate parameter: peer_max_window_bits".to_string(),
        ),
    )
}

#[test]
fn response_no_ext() {
    match on_response(&[], &DeflateConfig::default()) {
        Err(NegotiationErr::Failed) => {}
        _ => panic!("Expected no extension"),
    }
}

#[test]
fn response_unknown_ext() {
    match on_response(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-bzip",
        }],
        &DeflateConfig::default(),
    ) {
        Err(NegotiationErr::Failed) => {}
        r => panic!("Expected no extension. Got: {:?}", r),
    }
}

#[test]
fn response_duplicate_param() {
    match on_response(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate;server_no_context_takeover;server_no_context_takeover",
        }],
        &DeflateConfig::default(),
    ) {
        Err(NegotiationErr::Err(DeflateExtensionError::NegotiationError(s)))
            if s.to_string()
                .eq("Duplicate permessage-deflate parameter: server_no_context_takeover") => {}
        r => panic!("Expected an error. Got: {:?}", r),
    }
}

#[test]
fn response_invalid_max_bits() {
    match on_response(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate;server_max_window_bits=666",
        }],
        &DeflateConfig::default(),
    ) {
        Err(NegotiationErr::Err(DeflateExtensionError::InvalidMaxWindowBits)) => {}
        r => panic!("Expected an error. Got: {:?}", r),
    }
}

#[test]
fn response_unknown_param() {
    match on_response(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate;invalid=param",
        }],
        &DeflateConfig::default(),
    ) {
        Err(NegotiationErr::Err(DeflateExtensionError::NegotiationError(s)))
            if s.to_string()
                .eq("Unknown permessage-deflate parameter: invalid=param") => {}
        r => panic!("Expected an error. Got: {:?}", r),
    }
}

#[test]
fn response_no_context_takeover() {
    let mut config = DeflateConfig::default();
    config.accept_no_context_takeover = false;

    match on_response(
        &[Header {
            name: SEC_WEBSOCKET_EXTENSIONS.as_str(),
            value: b"permessage-deflate;client_no_context_takeover",
        }],
        &config,
    ) {
        Err(NegotiationErr::Err(DeflateExtensionError::NegotiationError(s)))
            if s.to_string().eq("The client requires context takeover") => {}
        r => panic!("Expected an error. Got: {:?}", r),
    }
}
