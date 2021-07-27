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

use crate::errors::{Error, ErrorKind, HttpError};
use crate::extensions::ExtensionHandshake;
use crate::handshake::client::Nonce;
use crate::handshake::{
    ProtocolRegistry, SubprotocolApplicator, UPGRADE_STR, WEBSOCKET_STR, WEBSOCKET_VERSION,
    WEBSOCKET_VERSION_STR,
};
use base64::encode_config_slice;
use bytes::BytesMut;
use http::header::{AsHeaderName, HeaderName, IntoHeaderName};
use http::{header, HeaderMap, HeaderValue, Method, Request};

pub fn encode_request(dst: &mut BytesMut, request: Request<()>, nonce_buffer: &mut Nonce) {
    let (parts, _body) = request.into_parts();

    dst.extend_from_slice(b"GET ");
    dst.extend_from_slice(parts.uri.to_string().as_bytes());
    dst.extend_from_slice(b" HTTP/1.1");
    dst.extend_from_slice(b"\r\nHost: ");
    // previously checked
    dst.extend_from_slice(parts.uri.host().unwrap().as_bytes());

    dst.extend_from_slice(format!("\r\n{}: websocket", header::UPGRADE).as_bytes());
    dst.extend_from_slice(format!("\r\n{}: Upgrade", header::CONNECTION).as_bytes());
    dst.extend_from_slice(format!("\r\n{}: ", header::SEC_WEBSOCKET_KEY).as_bytes());

    let nonce = rand::random::<[u8; 16]>();
    encode_config_slice(&nonce, base64::STANDARD, nonce_buffer);
    dst.extend_from_slice(nonce_buffer);

    write_header(dst, &parts.headers, header::ORIGIN);
    write_header(dst, &parts.headers, header::SEC_WEBSOCKET_PROTOCOL);
    write_header(dst, &parts.headers, header::SEC_WEBSOCKET_EXTENSIONS);

    dst.extend_from_slice(
        format!(
            "\r\n{}: {}",
            header::SEC_WEBSOCKET_VERSION,
            WEBSOCKET_VERSION
        )
        .as_bytes(),
    );
    dst.extend_from_slice(b"\r\n\r\n");
}

fn write_header(dst: &mut BytesMut, headers: &HeaderMap<HeaderValue>, name: HeaderName) {
    if let Some(value) = headers.get(&name) {
        dst.extend_from_slice(format!("\r\n{}: ", name).as_bytes());
        dst.extend_from_slice(value.as_bytes())
    }
}

// rfc6455 § 4.2.1
pub fn build_request<E>(
    request: &mut Request<()>,
    extension: &E,
    subprotocols: &ProtocolRegistry,
) -> Result<(), Error>
where
    E: ExtensionHandshake,
{
    if request.method() != Method::GET {
        return Err(Error::with_cause(ErrorKind::Http, HttpError::InvalidMethod));
    }

    validate_or_insert(
        request,
        header::CONNECTION,
        HeaderValue::from_static(UPGRADE_STR),
    )?;
    validate_or_insert(
        request,
        header::UPGRADE,
        HeaderValue::from_static(WEBSOCKET_STR),
    )?;
    validate_or_insert(
        request,
        header::SEC_WEBSOCKET_VERSION,
        HeaderValue::from_static(WEBSOCKET_VERSION_STR),
    )?;

    if let Some(_) = request.headers().get(header::SEC_WEBSOCKET_EXTENSIONS) {
        return Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::InvalidHeader(header::SEC_WEBSOCKET_EXTENSIONS),
        ));
    }

    extension.apply_headers(request);

    if let Some(_) = request.headers().get(header::SEC_WEBSOCKET_PROTOCOL) {
        return Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::InvalidHeader(header::SEC_WEBSOCKET_PROTOCOL),
        ));
    }

    subprotocols.apply_to(request)?;

    let option = request
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|head| head.to_str());
    match option {
        Some(Ok(version)) if version == WEBSOCKET_VERSION_STR => {}
        None => {
            request.headers_mut().insert(
                header::SEC_WEBSOCKET_VERSION,
                HeaderValue::from_static(WEBSOCKET_VERSION_STR),
            );
        }
        _ => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::InvalidHeader(header::SEC_WEBSOCKET_KEY),
            ))
        }
    }

    request.uri().host().ok_or(Error::with_cause(
        ErrorKind::Http,
        HttpError::MalformattedUri,
    ))?;

    Ok(())
}

fn validate_or_insert<A>(
    request: &mut Request<()>,
    header_name: A,
    expected: HeaderValue,
) -> Result<(), Error>
where
    A: AsHeaderName + IntoHeaderName + Clone,
{
    if let Some(header_value) = request.headers().get(header_name.clone()) {
        match header_value.to_str() {
            Ok(v) if v.to_ascii_lowercase().as_bytes().eq(expected.as_bytes()) => Ok(()),
            _ => Err(Error::new(ErrorKind::Http)),
        }
    } else {
        request.headers_mut().insert(header_name, expected);
        Ok(())
    }
}
