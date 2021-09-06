use crate::errors::{Error, ErrorKind, HttpError};
use crate::extensions::ExtensionHandshake;
use crate::handshake::client::Nonce;
use crate::handshake::{
    ProtocolRegistry, SubprotocolApplicator, UPGRADE_STR, WEBSOCKET_STR, WEBSOCKET_VERSION_STR,
};
use base64::encode_config_slice;
use bytes::{BufMut, BytesMut};
use http::header::{AsHeaderName, HeaderName, IntoHeaderName};
use http::request::Parts;
use http::{header, HeaderMap, HeaderValue, Method, Request, Uri, Version};

pub fn encode_request(dst: &mut BytesMut, request: ValidatedRequest, nonce_buffer: &mut Nonce) {
    let ValidatedRequest {
        version,
        headers,
        uri,
        host,
    } = request;

    let nonce = rand::random::<[u8; 16]>();
    encode_config_slice(&nonce, base64::STANDARD, nonce_buffer);

    let request = format!(
        "\
GET {path} {version:?}
Host: {host}
Connection: Upgrade
Upgrade: websocket
sec-websocket-version: 13
sec-websocket-key: ",
        version = version,
        path = uri.path(),
        host = host,
    );

    // 28 = request terminator + nonce buffer len
    let mut len = 28 + request.len();

    let origin = write_header(&headers, header::ORIGIN);
    let protocol = write_header(&headers, header::SEC_WEBSOCKET_PROTOCOL);
    let ext = write_header(&headers, header::SEC_WEBSOCKET_EXTENSIONS);

    if let Some((name, value)) = &origin {
        len += name.len() + value.len();
    }
    if let Some((name, value)) = &protocol {
        len += name.len() + value.len();
    }
    if let Some((name, value)) = &ext {
        len += name.len() + value.len();
    }

    dst.reserve(len);

    dst.put_slice(request.as_bytes());
    dst.put_slice(nonce_buffer);
    dst.put_slice(b"\r\n");

    if let Some((name, value)) = origin {
        dst.put_slice(name.as_bytes());
        dst.put_slice(value);
    }
    if let Some((name, value)) = protocol {
        dst.put_slice(name.as_bytes());
        dst.put_slice(value);
    }
    if let Some((name, value)) = ext {
        dst.put_slice(name.as_bytes());
        dst.put_slice(value);
    }

    dst.put_slice(b"\r\n\r\n");
}

fn write_header(headers: &HeaderMap<HeaderValue>, name: HeaderName) -> Option<(String, &[u8])> {
    if let Some(value) = headers.get(&name) {
        Some((format!("{}: ", name), value.as_bytes()))
    } else {
        None
    }
}

pub struct ValidatedRequest {
    version: Version,
    headers: HeaderMap,
    uri: Uri,
    host: String,
}

// rfc6455 § 4.2.1
pub fn build_request<E>(
    request: Request<()>,
    extension: &E,
    subprotocols: &ProtocolRegistry,
) -> Result<ValidatedRequest, Error>
where
    E: ExtensionHandshake,
{
    let (parts, _body) = request.into_parts();
    let Parts {
        method,
        uri,
        version,
        mut headers,
        ..
    } = parts;

    if method != Method::GET {
        return Err(Error::with_cause(ErrorKind::Http, HttpError::InvalidMethod));
    }

    if version < Version::HTTP_11 {
        return Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::HttpVersion(None),
        ));
    }

    validate_or_insert(
        &mut headers,
        header::CONNECTION,
        HeaderValue::from_static(UPGRADE_STR),
    )?;
    validate_or_insert(
        &mut headers,
        header::UPGRADE,
        HeaderValue::from_static(WEBSOCKET_STR),
    )?;
    validate_or_insert(
        &mut headers,
        header::SEC_WEBSOCKET_VERSION,
        HeaderValue::from_static(WEBSOCKET_VERSION_STR),
    )?;

    if let Some(_) = headers.get(header::SEC_WEBSOCKET_EXTENSIONS) {
        return Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::InvalidHeader(header::SEC_WEBSOCKET_EXTENSIONS),
        ));
    }

    extension.apply_headers(&mut headers);

    if let Some(_) = headers.get(header::SEC_WEBSOCKET_PROTOCOL) {
        return Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::InvalidHeader(header::SEC_WEBSOCKET_PROTOCOL),
        ));
    }

    subprotocols.apply_to(&mut headers)?;

    let option = headers
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|head| head.to_str());
    match option {
        Some(Ok(version)) if version == WEBSOCKET_VERSION_STR => {}
        None => {
            headers.insert(
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

    let host = uri
        .host()
        .ok_or(Error::with_cause(
            ErrorKind::Http,
            HttpError::MalformattedUri,
        ))?
        .to_string();

    Ok(ValidatedRequest {
        version,
        headers,
        uri,
        host,
    })
}

fn validate_or_insert<A>(
    headers: &mut HeaderMap,
    header_name: A,
    expected: HeaderValue,
) -> Result<(), Error>
where
    A: AsHeaderName + IntoHeaderName + Clone,
{
    if let Some(header_value) = headers.get(header_name.clone()) {
        match header_value.to_str() {
            Ok(v) if v.to_ascii_lowercase().as_bytes().eq(expected.as_bytes()) => Ok(()),
            _ => Err(Error::new(ErrorKind::Http)),
        }
    } else {
        headers.insert(header_name, expected);
        Ok(())
    }
}
