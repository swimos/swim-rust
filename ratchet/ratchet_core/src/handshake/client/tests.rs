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

use crate::errors::{Error, HttpError};
use crate::ext::NoExt;
use crate::fixture::mock;
use crate::handshake::client::{ClientHandshake, HandshakeResult};
use crate::handshake::{ProtocolError, ProtocolRegistry, ACCEPT_KEY, UPGRADE_STR, WEBSOCKET_STR};
use crate::{ErrorKind, NoExtProvider, TryIntoRequest};
use bytes::BytesMut;
use futures::future::join;
use futures::FutureExt;
use http::header::HeaderName;
use http::{header, HeaderMap, HeaderValue, Request, Response, StatusCode, Version};
use httparse::{Header, Status};
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, ExtensionProvider, FrameHeader,
    ReunitableExtension, RsvBits, SplittableExtension,
};
use sha1::{Digest, Sha1};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;

const TEST_URL: &str = "ws://127.0.0.1:9001/test";

#[tokio::test]
async fn handshake_sends_valid_request() {
    let request = TEST_URL.try_into_request().unwrap();
    let (mut peer, mut stream) = mock();
    let mut buf = BytesMut::new();
    let mut machine = ClientHandshake::new(
        &mut stream,
        ProtocolRegistry::new(vec!["warp"]),
        &NoExtProvider,
        &mut buf,
    );
    machine.encode(request).unwrap();
    machine.buffered.write().await.unwrap();

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);

    let mut buf = BytesMut::with_capacity(1024);
    peer.read_buf(&mut buf).await.unwrap();

    assert!(matches!(request.parse(&mut buf), Ok(Status::Complete(_))));

    assert_eq!(request.version, Some(1));
    assert_eq!(request.method, Some("GET"));
    assert_eq!(request.path, Some("/test"));
    assert_header(
        request.headers,
        header::SEC_WEBSOCKET_PROTOCOL.as_str(),
        "warp",
    );
}

fn assert_header(headers: &mut [Header<'_>], name: &str, expected: &str) {
    let mut filter = headers.iter().filter(|h| h.name == name);
    let header = filter.next().expect("Missing header");

    assert_eq!(header.value, expected.as_bytes());
    assert!(filter.next().is_none());
}

#[tokio::test]
async fn handshake_invalid_requests() {
    async fn test(request: Request<()>) {
        let (mut peer, mut stream) = mock();
        let mut buf = BytesMut::new();
        let mut machine = ClientHandshake::new(
            &mut stream,
            ProtocolRegistry::default(),
            &NoExtProvider,
            &mut buf,
        );
        machine
            .encode(request)
            .expect_err("Expected encoding to fail");
        machine.buffered.write().await.expect("Unexpected IO error");

        assert!(peer.read(&mut [0; 8]).now_or_never().is_none());
    }

    test(Request::put(TEST_URL).body(()).unwrap()).await;

    test(
        Request::get(TEST_URL)
            .header(header::SEC_WEBSOCKET_VERSION, "12")
            .body(())
            .unwrap(),
    )
    .await;

    test(
        Request::get(TEST_URL)
            .header(header::SEC_WEBSOCKET_KEY, "donut")
            .body(())
            .unwrap(),
    )
    .await;

    test(
        Request::get(TEST_URL)
            .header(header::UPGRADE, "MSG")
            .body(())
            .unwrap(),
    )
    .await;

    test(
        Request::get(TEST_URL)
            .header(header::SEC_WEBSOCKET_EXTENSIONS, "deflate")
            .body(())
            .unwrap(),
    )
    .await;
}

struct Trigger(Arc<Notify>);
impl Trigger {
    fn trigger() -> (Trigger, Trigger) {
        let inner = Arc::new(Notify::new());
        (Trigger(inner.clone()), Trigger(inner))
    }

    fn notify(&self) {
        self.0.notify_one();
    }

    async fn notified(&self) {
        self.0.notified().await;
    }
}

async fn expect_server_error(response: Response<()>, expected_error: HttpError) {
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = Trigger::trigger();
    let (server_tx, server_rx) = Trigger::trigger();

    let client_task = async move {
        let mut buf = BytesMut::new();
        let mut machine = ClientHandshake::new(
            &mut stream,
            ProtocolRegistry::default(),
            &NoExtProvider,
            &mut buf,
        );
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        client_tx.notify();
        server_rx.notified().await;

        let handshake_result = machine.read().await;

        const ERR: &str = "Expected an error";

        handshake_result
            .err()
            .map(|e| {
                let error = e.downcast_ref::<HttpError>().expect(ERR);
                assert_eq!(error, &expected_error);
            })
            .expect(ERR);
    };

    let server_task = async move {
        client_rx.notified().await;
        server.write_response(response).await.unwrap();
        server_tx.notify();
    };

    let _result = join(client_task, server_task).await;
}

#[tokio::test]
async fn missing_sec_websocket_accept() {
    let response = Response::builder()
        .version(Version::HTTP_11)
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, WEBSOCKET_STR)
        .header(header::CONNECTION, UPGRADE_STR)
        .body(())
        .unwrap();

    expect_server_error(
        response,
        HttpError::MissingHeader(header::SEC_WEBSOCKET_ACCEPT),
    )
    .await;
}

#[tokio::test]
async fn incorrect_sec_websocket_accept() {
    let response = Response::builder()
        .version(Version::HTTP_11)
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, WEBSOCKET_STR)
        .header(header::CONNECTION, UPGRADE_STR)
        .header(header::SEC_WEBSOCKET_ACCEPT, "🔥")
        .body(())
        .unwrap();

    expect_server_error(response, HttpError::KeyMismatch).await;
}

#[tokio::test]
async fn bad_status_code() {
    let response = Response::builder()
        .version(Version::HTTP_11)
        .status(StatusCode::IM_A_TEAPOT)
        .header(header::UPGRADE, WEBSOCKET_STR)
        .header(header::CONNECTION, UPGRADE_STR)
        .body(())
        .unwrap();

    expect_server_error(response, HttpError::Status(StatusCode::IM_A_TEAPOT)).await;
}

#[tokio::test]
async fn incorrect_version() {
    let response = Response::builder()
        .version(Version::HTTP_10)
        .body(())
        .unwrap();

    expect_server_error(response, HttpError::HttpVersion(Some(0))).await;
}

#[tokio::test]
async fn ok_nonce() {
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = Trigger::trigger();
    let (server_tx, server_rx) = Trigger::trigger();

    let client_task = async move {
        let mut buf = BytesMut::new();
        let mut machine = ClientHandshake::new(
            &mut stream,
            ProtocolRegistry::default(),
            &NoExtProvider,
            &mut buf,
        );
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        client_tx.notify();
        server_rx.notified().await;
        assert!(machine.read().await.is_ok());
    };

    let server_task = async move {
        client_rx.notified().await;

        let request = server
            .read_request()
            .await
            .expect("No server response received");

        let (parts, _body) = request.into_parts();

        let key = expect_header(&parts.headers, header::SEC_WEBSOCKET_KEY);

        let mut digest = Sha1::new();
        Digest::update(&mut digest, key);
        Digest::update(&mut digest, ACCEPT_KEY);

        let sec_websocket_accept = base64::encode(&digest.finalize());

        let response = Response::builder()
            .version(Version::HTTP_11)
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::UPGRADE, WEBSOCKET_STR)
            .header(header::CONNECTION, UPGRADE_STR)
            .header(header::SEC_WEBSOCKET_ACCEPT, sec_websocket_accept)
            .body(())
            .unwrap();

        server.write_response(response).await.unwrap();

        server_tx.notify();
    };

    let _result = join(client_task, server_task).await;
}

fn expect_header(headers: &HeaderMap, name: HeaderName) -> &HeaderValue {
    let err = format!("Missing header: {}", name);
    headers.get(name).expect(err.as_str())
}

#[tokio::test]
async fn redirection() {
    let redirected_to = "somewhere";

    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = Trigger::trigger();
    let (server_tx, server_rx) = Trigger::trigger();

    let client_task = async move {
        let mut buf = BytesMut::new();
        let mut machine = ClientHandshake::new(
            &mut stream,
            ProtocolRegistry::default(),
            &NoExtProvider,
            &mut buf,
        );
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        client_tx.notify();
        server_rx.notified().await;

        match machine.read().await {
            Ok(r) => {
                panic!("Expected an error got: {:?}", r)
            }
            Err(e) => {
                let r = e
                    .downcast_ref::<HttpError>()
                    .expect("Expected a HTTP error");

                assert_eq!(r, &HttpError::Redirected(redirected_to.to_string()))
            }
        }
    };

    let server_task = async move {
        client_rx.notified().await;

        let response = Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .version(Version::HTTP_11)
            .header(header::LOCATION, redirected_to)
            .body(())
            .unwrap();

        server.write_response(response).await.unwrap();

        server_tx.notify();
    };

    let _result = join(client_task, server_task).await;
}

async fn subprotocol_test<I, F>(registry: I, response_protocol: Option<String>, match_fn: F)
where
    I: IntoIterator<Item = &'static str>,
    F: Fn(Result<HandshakeResult<NoExt>, Error>),
{
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = Trigger::trigger();
    let (server_tx, server_rx) = Trigger::trigger();

    let client_task = async move {
        let mut buf = BytesMut::new();

        let mut machine = ClientHandshake::new(
            &mut stream,
            ProtocolRegistry::new(registry),
            &NoExtProvider,
            &mut buf,
        );
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        client_tx.notify();
        server_rx.notified().await;
        match_fn(machine.read().await);
    };

    let server_task = async move {
        client_rx.notified().await;

        let request = server
            .read_request()
            .await
            .expect("No server response received");

        let (parts, _body) = request.into_parts();

        let key = expect_header(&parts.headers, header::SEC_WEBSOCKET_KEY);

        let mut digest = Sha1::new();
        Digest::update(&mut digest, key);
        Digest::update(&mut digest, ACCEPT_KEY);

        let sec_websocket_accept = base64::encode(&digest.finalize());

        let mut response = Response::builder()
            .version(Version::HTTP_11)
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::UPGRADE, WEBSOCKET_STR)
            .header(header::CONNECTION, UPGRADE_STR)
            .header(header::SEC_WEBSOCKET_ACCEPT, sec_websocket_accept);

        if let Some(protocol) = response_protocol {
            response = response.header(header::SEC_WEBSOCKET_PROTOCOL, protocol);
        };

        let response = response.body(()).unwrap();
        server.write_response(response).await.unwrap();
        server_tx.notify();
    };

    let _result = join(client_task, server_task).await;
}

#[tokio::test]
async fn selects_valid_subprotocol() {
    subprotocol_test(vec!["warp", "warps"], Some("warp".to_string()), |r| {
        assert_eq!(r.unwrap().subprotocol, Some("warp".to_string()));
    })
    .await;
}

#[tokio::test]
async fn invalid_subprotocol() {
    subprotocol_test(vec!["warp", "warps"], Some("warpy".to_string()), |r| {
        let err = r.unwrap_err();
        let protocol_error = err
            .downcast_ref::<ProtocolError>()
            .expect("Expected a protocol error");
        assert_eq!(protocol_error, &ProtocolError::UnknownProtocol);
    })
    .await;
}

#[tokio::test]
async fn disjoint_protocols() {
    subprotocol_test(vec!["warp", "warps"], None, |r| {
        assert_eq!(r.unwrap().subprotocol, None);
    })
    .await;
}

#[derive(thiserror::Error, Debug)]
#[error("Extension error")]
struct ExtHandshakeErr;

impl From<ExtHandshakeErr> for Error {
    fn from(e: ExtHandshakeErr) -> Self {
        Error::with_cause(ErrorKind::Extension, e)
    }
}

struct MockExtensionProxy<R>(&'static [(HeaderName, &'static str)], R)
where
    R: for<'h> Fn(&'h [Header]) -> Result<Option<MockExtension>, ExtHandshakeErr>;

impl<R> ExtensionProvider for MockExtensionProxy<R>
where
    R: for<'h> Fn(&'h [Header]) -> Result<Option<MockExtension>, ExtHandshakeErr>,
{
    type Extension = MockExtension;
    type Error = ExtHandshakeErr;

    fn apply_headers(&self, header_map: &mut HeaderMap) {
        for (name, value) in self.0 {
            header_map.insert(name, HeaderValue::from_static(value));
        }
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        (self.1)(headers)
    }

    fn negotiate_server(
        &self,
        _headers: &[Header],
    ) -> Result<Option<(Self::Extension, HeaderValue)>, ExtHandshakeErr> {
        panic!("Unexpected server-side extension negotiation")
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct MockExtension(bool);

impl ExtensionEncoder for MockExtension {
    type Error = Infallible;

    fn encode(
        &mut self,
        _payload: &mut BytesMut,
        _header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ExtensionDecoder for MockExtension {
    type Error = Infallible;

    fn decode(
        &mut self,
        _payload: &mut BytesMut,
        _header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Extension for MockExtension {
    fn bits(&self) -> RsvBits {
        RsvBits {
            rsv1: false,
            rsv2: false,
            rsv3: false,
        }
    }
}

impl ReunitableExtension for MockExtension {
    fn reunite(encoder: Self::SplitEncoder, _decoder: Self::SplitDecoder) -> Self {
        encoder
    }
}

impl SplittableExtension for MockExtension {
    type SplitEncoder = Self;
    type SplitDecoder = Self;

    fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder) {
        (self, self)
    }
}

async fn extension_test<E, F, R>(ext: E, response_fn: F, result_fn: R)
where
    E: ExtensionProvider,
    F: Fn(&mut Response<()>),
    R: Fn(Result<HandshakeResult<E::Extension>, Error>),
{
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = Trigger::trigger();
    let (server_tx, server_rx) = Trigger::trigger();

    let client_task = async move {
        let mut buf = BytesMut::new();
        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::default(), &ext, &mut buf);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        client_tx.notify();
        server_rx.notified().await;

        let read_result = machine.read().await;
        result_fn(read_result);
    };

    let server_task = async move {
        client_rx.notified().await;

        let request = server
            .read_request()
            .await
            .expect("No server response received");

        let (parts, _body) = request.into_parts();

        let key = expect_header(&parts.headers, header::SEC_WEBSOCKET_KEY);

        let mut digest = Sha1::new();
        Digest::update(&mut digest, key);
        Digest::update(&mut digest, ACCEPT_KEY);

        let sec_websocket_accept = base64::encode(&digest.finalize());

        let mut response = Response::builder()
            .version(Version::HTTP_11)
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::UPGRADE, WEBSOCKET_STR)
            .header(header::CONNECTION, UPGRADE_STR)
            .header(header::SEC_WEBSOCKET_ACCEPT, sec_websocket_accept)
            .body(())
            .unwrap();

        response_fn(&mut response);

        server.write_response(response).await.unwrap();

        server_tx.notify();
    };

    let _result = join(client_task, server_task).await;
}

#[tokio::test]
async fn negotiates_extension() {
    const EXT: &str = "test_extension; something=1, something_else=2";
    const HEADERS: &'static [(HeaderName, &'static str)] =
        &[(header::SEC_WEBSOCKET_EXTENSIONS, EXT)];

    let extension_proxy = MockExtensionProxy(&HEADERS, |headers| {
        let ext = headers
            .iter()
            .filter(|h| {
                h.name
                    .eq_ignore_ascii_case(header::SEC_WEBSOCKET_EXTENSIONS.as_str())
            })
            .next();
        match ext {
            Some(header) => {
                let value = String::from_utf8(header.value.to_vec())
                    .expect("Server returned invalid UTF-8");
                if value == EXT {
                    Ok(Some(MockExtension(true)))
                } else {
                    panic!(
                        "Server returned an invalid sec-websocket-extensions header: `{:?}`",
                        value
                    );
                }
            }
            None => {
                panic!("Server sec-websocket-extensions header missing")
            }
        }
    });

    extension_test(
        extension_proxy,
        |r| {
            r.headers_mut().insert(
                header::SEC_WEBSOCKET_EXTENSIONS,
                HeaderValue::from_static(EXT),
            );
        },
        |result| match result {
            Ok(handshake_result) => assert!(handshake_result.extension.take().unwrap().0),
            Err(e) => {
                panic!("Expected a valid upgrade: {:?}", e)
            }
        },
    )
    .await;
}

#[tokio::test]
async fn negotiates_no_extension() {
    const HEADERS: &'static [(HeaderName, &'static str)] = &[];
    let extension_proxy = MockExtensionProxy(&HEADERS, |_| Ok(Some(MockExtension(false))));

    extension_test(
        extension_proxy,
        |_| {},
        |result| match result {
            Ok(handshake_result) => assert!(!handshake_result.extension.take().unwrap().0),
            Err(e) => {
                panic!("Expected a valid upgrade: {:?}", e)
            }
        },
    )
    .await;
}
