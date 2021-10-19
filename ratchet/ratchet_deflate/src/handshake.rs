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
use crate::{
    Deflate, DeflateConfig, InitialisedDeflateConfig, WindowBits, LZ77_MAX_WINDOW_SIZE,
    LZ77_MIN_WINDOW_SIZE,
};
use bytes::BytesMut;
use http::header::SEC_WEBSOCKET_EXTENSIONS;
use http::{HeaderMap, HeaderValue};
use ratchet_ext::Header;
use std::fmt::Write;
use std::str::Utf8Error;

/// The WebSocket Extension Identifier as per the IANA registry.
const EXT_IDENT: &str = "permessage-deflate";

const SERVER_MAX_BITS: &str = "server_max_window_bits";
const CLIENT_MAX_BITS: &str = "client_max_window_bits";
const SERVER_NO_TAKEOVER: &str = "server_no_context_takeover";
const CLIENT_NO_TAKEOVER: &str = "client_no_context_takeover";
const ERR_TAKEOVER: &str = "The client requires context takeover";
const UNKNOWN_PARAM: &str = "Unknown permessage-deflate parameter";
const DUPLICATE_PARAM: &str = "Duplicate permessage-deflate parameter";
const HEADER_ERR: &str = "Failed to produce header";

struct DeflateHeaderEncoder<'c>(&'c DeflateConfig);
impl<'c> DeflateHeaderEncoder<'c> {
    #[inline]
    fn encode(self, into: &mut BytesMut) {
        into.reserve(self.size_hint());
        self.encode_into(into)
    }

    #[inline]
    fn encode_into(self, into: &mut BytesMut) {
        let DeflateConfig {
            server_max_window_bits,
            client_max_window_bits,
            request_server_no_context_takeover,
            request_client_no_context_takeover,
            ..
        } = self.0;

        write(into, EXT_IDENT);
        write(into, "; ");

        if *client_max_window_bits < LZ77_MAX_WINDOW_SIZE {
            write(into, CLIENT_MAX_BITS);
            write(into, "=");
            write(into, client_max_window_bits.as_str());
            write(into, "; ");
            write(into, SERVER_MAX_BITS);
            write(into, "=");
            write(into, server_max_window_bits.as_str());
        } else {
            write(into, CLIENT_MAX_BITS);
        }

        if *request_server_no_context_takeover {
            write(into, "; server_no_context_takeover");
        }
        if *request_client_no_context_takeover {
            write(into, "; client_no_context_takeover");
        }
    }

    #[inline]
    fn size_hint(&self) -> usize {
        let DeflateConfig {
            client_max_window_bits,
            request_server_no_context_takeover,
            request_client_no_context_takeover,
            ..
        } = self.0;

        let mut len = EXT_IDENT.len();

        if *client_max_window_bits < LZ77_MAX_WINDOW_SIZE {
            // 4 for pairs & 2 for bits
            len += 4 + CLIENT_MAX_BITS.len() + SERVER_MAX_BITS.len() + 2;
        } else {
            len += CLIENT_MAX_BITS.len();
        }

        if *request_server_no_context_takeover {
            // 2 for colon and space
            len += SERVER_NO_TAKEOVER.len() + 2;
        }
        if *request_client_no_context_takeover {
            // 2 for colon and space
            len += CLIENT_NO_TAKEOVER.len() + 2;
        }

        len
    }
}

#[inline]
fn write(into: &mut BytesMut, data: &str) {
    if let Err(_) = into.write_str(data) {
        extend_and_write(into, data);
    }
}

#[cold]
#[inline(never)]
fn extend_and_write(into: &mut BytesMut, data: &str) {
    into.reserve(data.len());
    let _ = into.write_str(data);
}

pub fn apply_headers(header_map: &mut HeaderMap, config: &DeflateConfig) {
    let encoder = DeflateHeaderEncoder(config);
    let mut bytes = BytesMut::new();
    bytes.truncate(bytes.len());
    let _ = encoder.encode(&mut bytes);

    header_map.insert(
        SEC_WEBSOCKET_EXTENSIONS,
        HeaderValue::from_bytes(bytes.as_ref()).expect(HEADER_ERR),
    );
}

pub fn negotiate_client(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<Deflate>, DeflateExtensionError> {
    match on_response(headers, config) {
        Ok(initialised_config) => Ok(Some(Deflate::initialise_from(initialised_config, false))),
        Err(NegotiationErr::Failed) => Ok(None),
        Err(NegotiationErr::Err(e)) => Err(e),
    }
}

pub fn negotiate_server(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<Option<(Deflate, HeaderValue)>, DeflateExtensionError> {
    match on_request(headers, config) {
        Ok((initialised_config, header)) => Ok(Some((
            Deflate::initialise_from(initialised_config, true),
            header,
        ))),
        Err(NegotiationErr::Failed) => Ok(None),
        Err(NegotiationErr::Err(e)) => Err(e),
    }
}

pub fn on_request(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<(InitialisedDeflateConfig, HeaderValue), NegotiationErr> {
    let header_iter = headers.iter().filter(|h| {
        h.name
            .eq_ignore_ascii_case(SEC_WEBSOCKET_EXTENSIONS.as_str())
    });

    for header in header_iter {
        let header_value =
            std::str::from_utf8(header.value).map_err(|e| DeflateExtensionError::from(e))?;

        for part in header_value.split(',') {
            match validate_request_header(part, config) {
                Ok((initialised_config, header)) => return Ok((initialised_config, header)),
                Err(NegotiationErr::Failed) => continue,
                Err(NegotiationErr::Err(e)) => return Err(NegotiationErr::Err(e)),
            }
        }
    }

    Err(NegotiationErr::Failed)
}

fn validate_request_header(
    header: &str,
    config: &DeflateConfig,
) -> Result<(InitialisedDeflateConfig, HeaderValue), NegotiationErr> {
    let mut response_str = String::with_capacity(header.len());
    let mut param_iter = header.split(';');

    match param_iter.next() {
        Some(name) if name.trim().eq_ignore_ascii_case(EXT_IDENT) => {
            response_str.push_str(EXT_IDENT);
        }
        _ => {
            return Err(NegotiationErr::Failed);
        }
    }

    let mut seen_server_takeover = false;
    let mut seen_client_takeover = false;
    let mut seen_server_max_bits = false;
    let mut seen_client_max_bits = false;
    let mut initialised_config = InitialisedDeflateConfig::from_config(config);

    for param in param_iter {
        match param.trim().to_lowercase().as_str() {
            n if n == SERVER_NO_TAKEOVER => {
                check_param(n, &mut seen_server_takeover, || {
                    if config.accept_no_context_takeover {
                        initialised_config.compress_reset = true;
                        response_str.push_str("; server_no_context_takeover");
                    }
                    Ok(())
                })?;
            }
            n if n == CLIENT_NO_TAKEOVER => {
                check_param(n, &mut seen_client_takeover, || {
                    initialised_config.decompress_reset = true;
                    response_str.push_str("; client_no_context_takeover");
                    Ok(())
                })?;
            }
            param if param.starts_with(SERVER_MAX_BITS) => {
                check_param(SERVER_MAX_BITS, &mut seen_server_max_bits, || {
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
                            Err(DeflateExtensionError::InvalidMaxWindowBits.into())
                        }
                    }
                })?;
            }
            param if param.starts_with(CLIENT_MAX_BITS) => {
                check_param(CLIENT_MAX_BITS, &mut seen_client_max_bits, || {
                    let mut window_param = param.split("=").skip(1);
                    if let Some(window_param) = window_param.next() {
                        // Absence of this parameter in an extension negotiation offer indicates
                        // that the client can receive messages compressed using an LZ77 sliding
                        // window of up to 32,768 bytes.
                        initialised_config.client_max_window_bits =
                            parse_window_parameter(window_param, config.client_max_window_bits)?;
                        response_str.push_str(&format!(
                            "; {}={}",
                            CLIENT_MAX_BITS,
                            initialised_config.client_max_window_bits.as_str()
                        ));
                    }
                    Ok(())
                })?;
            }
            p => {
                return Err(DeflateExtensionError::NegotiationError(
                    format!("{}: {}", UNKNOWN_PARAM, p).into(),
                )
                .into())
            }
        }
    }

    Ok((
        initialised_config,
        HeaderValue::from_str(response_str.as_str()).map_err(|e| DeflateExtensionError::from(e))?,
    ))
}

#[derive(Debug)]
pub enum NegotiationErr {
    /// This is not an error but means that it was not possible to negotiate the extension and so it
    /// will not be used.
    Failed,
    /// An error was produced when negotiating per-message deflate.
    Err(DeflateExtensionError),
}

impl From<DeflateExtensionError> for NegotiationErr {
    fn from(e: DeflateExtensionError) -> Self {
        NegotiationErr::Err(e)
    }
}

impl From<Utf8Error> for NegotiationErr {
    fn from(e: Utf8Error) -> Self {
        NegotiationErr::Err(DeflateExtensionError::from(e))
    }
}

pub fn on_response(
    headers: &[Header],
    config: &DeflateConfig,
) -> Result<InitialisedDeflateConfig, NegotiationErr> {
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
        let mut param_iter = header_value.split(';');

        if let Some(param) = param_iter.next() {
            if param.trim().eq_ignore_ascii_case(EXT_IDENT) {
                check_param(EXT_IDENT, &mut seen_extension_name, || {
                    enabled = true;
                    Ok(())
                })?
            } else {
                return Err(NegotiationErr::Failed);
            }
        }

        for param in param_iter {
            match param.trim().to_lowercase().as_str() {
                n if n == SERVER_NO_TAKEOVER => {
                    check_param(n, &mut seen_server_takeover, || {
                        decompress_reset = true;
                        Ok(())
                    })?;
                }
                n if n == CLIENT_NO_TAKEOVER => {
                    check_param(n, &mut seen_client_takeover, || {
                        if accept_no_context_takeover {
                            compress_reset = true;
                            Ok(())
                        } else {
                            Err(DeflateExtensionError::NegotiationError(ERR_TAKEOVER.into()).into())
                        }
                    })?;
                }
                param if param.starts_with(SERVER_MAX_BITS) => {
                    check_param(SERVER_MAX_BITS, &mut seen_server_max_window_bits, || {
                        let mut window_param = param.split("=").skip(1);
                        match window_param.next() {
                            Some(window_param) => {
                                server_max_window_bits =
                                    parse_window_parameter(window_param, server_max_window_bits)?;
                                Ok(())
                            }
                            None => Err(DeflateExtensionError::InvalidMaxWindowBits.into()),
                        }
                    })?;
                }
                param if param.starts_with(CLIENT_MAX_BITS) => {
                    check_param(CLIENT_MAX_BITS, &mut seen_client_max_window_bits, || {
                        let mut window_param = param.split("=").skip(1);
                        if let Some(window_param) = window_param.next() {
                            client_max_window_bits =
                                parse_window_parameter(window_param, client_max_window_bits)?;
                        }
                        Ok(())
                    })?;
                }
                p => {
                    return Err(DeflateExtensionError::NegotiationError(format!(
                        "{}: {}",
                        UNKNOWN_PARAM, p
                    ))
                    .into());
                }
            }
        }
    }

    if enabled {
        Ok(InitialisedDeflateConfig {
            server_max_window_bits,
            client_max_window_bits,
            compress_reset,
            decompress_reset,
            compression_level: config.compression_level,
        })
    } else {
        Err(NegotiationErr::Failed)
    }
}

fn check_param<F>(name: &str, seen: &mut bool, mut then: F) -> Result<(), NegotiationErr>
where
    F: FnMut() -> Result<(), NegotiationErr>,
{
    if *seen {
        Err(NegotiationErr::Err(
            DeflateExtensionError::NegotiationError(format!("{}: {}", DUPLICATE_PARAM, name)),
        ))
    } else {
        then()?;
        *seen = true;
        Ok(())
    }
}

fn parse_window_parameter(
    window_param: &str,
    max_window_bits: WindowBits,
) -> Result<WindowBits, NegotiationErr> {
    let window_param = window_param.replace("\"", "");
    match window_param.trim().parse() {
        Ok(window_bits) => {
            if (LZ77_MIN_WINDOW_SIZE..=max_window_bits.0).contains(&window_bits) {
                Ok(WindowBits(window_bits))
            } else {
                Err(NegotiationErr::Failed)
            }
        }
        Err(_) => Err(DeflateExtensionError::InvalidMaxWindowBits.into()),
    }
}
