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
use http::header::SEC_WEBSOCKET_EXTENSIONS;
use ratchet_ext::Header;

/// The WebSocket Extension Identifier as per the IANA registry.
const EXT_IDENT: &str = "permessage-deflate";

pub fn negotiate_client(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Deflate, DeflateExtensionError> {
    match on_response(headers, config)? {
        Some(initialised_config) => Ok(Deflate::initialise_from(initialised_config)),
        None => Ok(Deflate::disabled()),
    }
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
