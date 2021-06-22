use crate::error::{ConnectionError, ConnectionErrorKind};
use crate::handshake::{UPGRADE, WEBSOCKET, WEBSOCKET_VERSION};
use crate::{RequestError, WebSocketConfig, WebSocketStream};
use base64::encode_config_slice;
use bytes::BytesMut;
use futures::{AsyncRead, AsyncWrite};
use http::header::{AsHeaderName, HeaderName};
use http::{header, HeaderMap, HeaderValue, Method, Request, Response};
use tokio::io::AsyncWriteExt;
use tokio_native_tls::TlsConnector;

type Nonce = [u8; 24];

pub async fn exec_client_handshake<S>(
    config: &WebSocketConfig,
    stream: &mut S,
    connector: Option<TlsConnector>,
    mut request: Request<()>,
) -> Result<(), ConnectionError>
where
    S: WebSocketStream,
{
    validate_request(&request)?;

    let mut nonce = [0; 24];
    let mut buffer = BytesMut::new();
    encode_request(&mut buffer, request, &mut nonce);

    let mut buffered = BufferedIo::new(stream, buffer);

    buffered.write().await?;
    let response = buffered.read().await?;

    validate_response(&response, nonce);

    Ok(())
}

fn validate_response(response: &Response<()>, nonce: Nonce) -> Result<(), ()> {
    Ok(())
}

struct BufferedIo<'s, S> {
    socket: &'s mut S,
    buffer: BytesMut,
}

impl<'s, S> BufferedIo<'s, S>
where
    S: WebSocketStream,
{
    fn new(socket: &'s mut S, buffer: BytesMut) -> BufferedIo<'s, S> {
        BufferedIo { socket, buffer }
    }

    async fn write(&mut self) -> Result<(), ConnectionError> {
        let BufferedIo { socket, buffer } = self;

        socket.write_all(&buffer).await?;
        socket.flush().await?;

        Ok(())
    }

    async fn read(&mut self) -> Result<Response<()>, ConnectionError> {
        unimplemented!()
    }
}

fn encode_request(dst: &mut BytesMut, request: Request<()>, nonce_buffer: &mut Nonce) {
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

    debug_assert!(nonce_buffer.is_empty());
    let nonce = rand::random::<[u8; 16]>();
    encode_config_slice(&nonce, base64::STANDARD, nonce_buffer);
    dst.extend_from_slice(nonce_buffer);

    write_header_pair(dst, &parts.headers, header::ORIGIN);
    write_header_pair(dst, &parts.headers, header::SEC_WEBSOCKET_PROTOCOL);
    write_header_pair(dst, &parts.headers, header::SEC_WEBSOCKET_EXTENSIONS);

    dst.extend_from_slice(
        format!(
            "\r\n{}: {}",
            header::SEC_WEBSOCKET_VERSION,
            WEBSOCKET_VERSION
        )
        .as_bytes(),
    );
}

fn write_header_pair(dst: &mut BytesMut, headers: &HeaderMap<HeaderValue>, name: HeaderName) {
    if let Some(value) = headers.get(&name) {
        dst.extend_from_slice(format!("\r\n{}: ", name).as_bytes());
        dst.extend_from_slice(value.as_bytes())
    }
}

// rfc6455 § 4.2.1
fn validate_request(request: &Request<()>) -> Result<(), ConnectionError> {
    if request.method() != Method::GET {
        return Err(ConnectionError::new(ConnectionErrorKind::Request));
    }

    header_valid(request, header::CONNECTION, UPGRADE)?;
    header_valid(request, header::UPGRADE, WEBSOCKET)?;
    header_valid(
        request,
        header::SEC_WEBSOCKET_VERSION,
        WEBSOCKET_VERSION.to_string(),
    )?;
    // this is set by the library and should not have been provided
    if request.headers().get(header::SEC_WEBSOCKET_KEY).is_some() {
        return Err(ConnectionError::new(ConnectionErrorKind::Request));
    }

    request
        .uri()
        .host()
        .ok_or(ConnectionError::new(ConnectionErrorKind::Request))?;

    Ok(())
}

fn header_valid<A, E>(
    request: &Request<()>,
    header_name: A,
    expected: E,
) -> Result<(), ConnectionError>
where
    A: AsHeaderName,
    E: AsRef<str>,
{
    if let Some(header_value) = request.headers().get(header_name) {
        match header_value.to_str() {
            Ok(v) if v.to_ascii_lowercase().contains(expected.as_ref()) => Ok(()),
            _ => Err(ConnectionError::new(ConnectionErrorKind::Request)),
        }
    } else {
        Err(ConnectionError::new(ConnectionErrorKind::Request))
    }
}
