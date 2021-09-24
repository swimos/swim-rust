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
use crate::{Deflate, DeflateConfig, InitialisedDeflateConfig, LZ77_MIN_WINDOW_SIZE};
use bytes::BytesMut;
use http::header::SEC_WEBSOCKET_EXTENSIONS;
use http::{HeaderMap, HeaderValue};
use ratchet_ext::Header;

/// The WebSocket Extension Identifier as per the IANA registry.
const EXT_IDENT: &str = "permessage-deflate";

pub fn apply_headers(header_map: &mut HeaderMap, config: &DeflateConfig) {
    let DeflateConfig {
        server_max_window_bits,
        client_max_window_bits,
        ..
    } = config;

    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(format!("{}; ", EXT_IDENT).as_bytes());
    // bytes.extend_from_slice(
    //     format!("server_max_window_bits={}; ", server_max_window_bits).as_bytes(),
    // );

    bytes.extend_from_slice(format!("client_max_window_bits").as_bytes());
    // bytes.extend_from_slice(b"client_no_context_takeover;");
    // bytes.extend_from_slice(b"server_no_context_takeover");

    // todo
    header_map.insert(
        SEC_WEBSOCKET_EXTENSIONS,
        HeaderValue::from_bytes(bytes.as_ref()).unwrap(),
    );
}

pub fn negotiate_client(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<Deflate>, DeflateExtensionError> {
    match on_response(headers, config)? {
        Some(initialised_config) => Ok(Some(Deflate::initialise_from(initialised_config))),
        None => Ok(None),
    }
}

pub fn negotiate_server(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<(Deflate, HeaderValue)>, DeflateExtensionError> {
    match on_request(headers, config)? {
        Some((initialised_config, header)) => {
            Ok(Some((Deflate::initialise_from(initialised_config), header)))
        }
        None => Ok(None),
    }
}

fn on_request(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<(InitialisedDeflateConfig, HeaderValue)>, DeflateExtensionError> {
    let header_iter = headers.iter().filter(|h| {
        h.name
            .eq_ignore_ascii_case(SEC_WEBSOCKET_EXTENSIONS.as_str())
    });

    for header in header_iter {
        let header_value = std::str::from_utf8(header.value)?;

        for part in header_value.split(',') {
            match validate_request_header(part, config)? {
                Some((initialised_config, header)) => {
                    return Ok(Some((initialised_config, header)))
                }
                None => continue,
            }
        }
    }

    Ok(None)
}

fn validate_request_header(
    header: &str,
    config: &DeflateConfig,
) -> Result<Option<(InitialisedDeflateConfig, HeaderValue)>, DeflateExtensionError> {
    let mut response_str = String::with_capacity(header.len());
    let mut param_iter = header.split(';');

    match param_iter.next() {
        Some(name) if name.trim().eq_ignore_ascii_case(EXT_IDENT) => {
            response_str.push_str(EXT_IDENT);
        }
        _ => {
            return Ok(None);
        }
    }

    let mut seen_server_takeover = false;
    let mut seen_client_takeover = false;
    let mut seen_server_max_bits = false;
    let mut seen_client_max_bits = false;
    let mut initialised_config = InitialisedDeflateConfig::from_config(config);

    for param in param_iter {
        match param.trim().to_lowercase().as_str() {
            n @ "server_no_context_takeover" => {
                check_param(n, &mut seen_server_takeover, || {
                    if config.accept_no_context_takeover {
                        initialised_config.compress_reset = true;
                        response_str.push_str("; server_no_context_takeover");
                    }
                    Ok(())
                })?;
            }
            n @ "client_no_context_takeover" => {
                check_param(n, &mut seen_client_takeover, || {
                    initialised_config.decompress_reset = true;
                    response_str.push_str("; client_no_context_takeover");
                    Ok(())
                })?;
            }
            param if param.starts_with("server_max_window_bits") => {
                check_param("server_max_window_bits", &mut seen_server_max_bits, || {
                    let mut window_param = param.split("=").skip(1);
                    match window_param.next() {
                        Some(window_param) => {
                            initialised_config.server_max_window_bits = parse_window_parameter(
                                window_param,
                                config.server_max_window_bits,
                            )?;
                            Ok(())
                        }
                        None => {
                            // If the client specifies 'server_max_window_bits' then a value must
                            // be provided.
                            Err(DeflateExtensionError::InvalidMaxWindowBits)
                        }
                    }
                })?;
            }
            param if param.starts_with("client_max_window_bits") => {
                check_param("client_max_window_bits", &mut seen_client_max_bits, || {
                    let mut window_param = param.split("=").skip(1);
                    if let Some(window_param) = window_param.next() {
                        // Absence of this parameter in an extension negotiation offer indicates
                        // that the client can receive messages compressed using an LZ77 sliding
                        // window of up to 32,768 bytes.
                        initialised_config.client_max_window_bits =
                            parse_window_parameter(window_param, config.client_max_window_bits)?;
                    }

                    response_str.push_str(&format!(
                        "; client_max_window_bits={}",
                        initialised_config.client_max_window_bits
                    ));
                    Ok(())
                })?;
            }
            p => {
                return Err(DeflateExtensionError::NegotiationError(
                    format!("Unknown permessage-deflate parameter: {}", p).into(),
                ))
            }
        }
    }

    Ok(Some((
        initialised_config,
        HeaderValue::from_str(response_str.as_str())?,
    )))
}

fn on_response(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<InitialisedDeflateConfig>, DeflateExtensionError> {
    let mut seen_extension_name = false;
    let mut seen_server_takeover = false;
    let mut seen_client_takeover = false;
    let mut seen_server_max_window_bits = false;
    let mut seen_client_max_window_bits = false;
    let mut enabled = false;
    let mut compress_reset = false;
    let mut decompress_reset = false;

    let mut server_max_window_bits = config.server_max_window_bits;
    let mut client_max_window_bits = config.client_max_window_bits;
    let accept_no_context_takeover = config.accept_no_context_takeover;

    let header_iter = headers.iter().filter(|h| {
        h.name
            .eq_ignore_ascii_case(SEC_WEBSOCKET_EXTENSIONS.as_str())
    });

    for header in header_iter {
        let header_value = std::str::from_utf8(header.value)?;

        for param in header_value.split(';') {
            match param.trim().to_lowercase().as_str() {
                EXT_IDENT => check_param(EXT_IDENT, &mut seen_extension_name, || {
                    enabled = true;
                    Ok(())
                })?,
                n @ "server_no_context_takeover" => {
                    check_param(n, &mut seen_server_takeover, || {
                        decompress_reset = true;
                        Ok(())
                    })?;
                }
                n @ "client_no_context_takeover" => {
                    check_param(n, &mut seen_client_takeover, || {
                        if accept_no_context_takeover {
                            compress_reset = true;
                            Ok(())
                        } else {
                            Err(DeflateExtensionError::NegotiationError(format!(
                                "The client requires context takeover."
                            )))
                        }
                    })?;
                }
                param if param.starts_with("server_max_window_bits") => {
                    check_param(
                        "server_max_window_bits",
                        &mut seen_server_max_window_bits,
                        || {
                            let mut window_param = param.split("=").skip(1);
                            match window_param.next() {
                                Some(window_param) => {
                                    server_max_window_bits = parse_window_parameter(
                                        window_param,
                                        server_max_window_bits,
                                    )?;
                                    Ok(())
                                }
                                None => Err(DeflateExtensionError::InvalidMaxWindowBits),
                            }
                        },
                    )?;
                }
                param if param.starts_with("client_max_window_bits") => {
                    check_param(
                        "client_max_window_bits",
                        &mut seen_client_max_window_bits,
                        || {
                            let mut window_param = param.split("=").skip(1);
                            if let Some(window_param) = window_param.next() {
                                client_max_window_bits =
                                    parse_window_parameter(window_param, client_max_window_bits)?;
                            }
                            Ok(())
                        },
                    )?;
                }
                p => {
                    return Err(DeflateExtensionError::NegotiationError(format!(
                        "Unknown permessage-deflate parameter: {}",
                        p
                    )));
                }
            }
        }
    }

    if enabled {
        Ok(Some(InitialisedDeflateConfig {
            server_max_window_bits,
            client_max_window_bits,
            compress_reset,
            decompress_reset,
            compression_level: config.compression_level,
        }))
    } else {
        Ok(None)
    }
}

fn check_param<F>(name: &str, seen: &mut bool, mut then: F) -> Result<(), DeflateExtensionError>
where
    F: FnMut() -> Result<(), DeflateExtensionError>,
{
    if *seen {
        Err(DeflateExtensionError::NegotiationError(format!(
            "Duplicate extension parameter: {}",
            name
        )))
    } else {
        then()?;
        *seen = true;
        Ok(())
    }
}

fn parse_window_parameter(
    window_param: &str,
    max_window_bits: u8,
) -> Result<u8, DeflateExtensionError> {
    let window_param = window_param.replace("\"", "");
    match window_param.trim().parse() {
        Ok(window_bits) => {
            if (LZ77_MIN_WINDOW_SIZE..=max_window_bits).contains(&window_bits) {
                Ok(window_bits)
            } else {
                Err(DeflateExtensionError::InvalidMaxWindowBits)
            }
        }
        Err(_) => Err(DeflateExtensionError::InvalidMaxWindowBits),
    }
}
