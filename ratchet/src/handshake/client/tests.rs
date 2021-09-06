use crate::errors::{Error, HttpError};
use crate::extensions::ext::{NoExt, NoExtProxy};
use crate::extensions::{ExtHandshakeErr, Extension, ExtensionHandshake, NegotiatedExtension};
use crate::fixture::mock;
use crate::handshake::client::{ClientHandshake, HandshakeResult};
use crate::handshake::{ProtocolError, ProtocolRegistry, ACCEPT_KEY, UPGRADE_STR, WEBSOCKET_STR};
use crate::TryIntoRequest;
use bytes::BytesMut;
use futures::future::join;
use futures::FutureExt;
use http::header::HeaderName;
use http::{header, HeaderMap, HeaderValue, Request, Response, StatusCode, Version};
use httparse::{Header, Status};
use sha1::{Digest, Sha1};
use tokio::io::AsyncReadExt;
use utilities::sync::trigger;

const TEST_URL: &str = "ws://127.0.0.1:9001/test";

// todo: some parts in this test file can be refactored once the server handshake has been done

#[tokio::test]
async fn handshake_sends_valid_request() {
    let request = TEST_URL.try_into_request().unwrap();
    let (mut peer, mut stream) = mock();

    let mut machine =
        ClientHandshake::new(&mut stream, ProtocolRegistry::new(vec!["warp"]), NoExtProxy);
    machine.encode(request).unwrap();
    machine.buffered.write().await.unwrap();

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);

    let mut buf = BytesMut::with_capacity(1024);
    peer.read_buf(&mut buf).await.unwrap();

    assert!(matches!(request.parse(&mut buf), Ok(Status::Complete(_))));

    assert_eq!(request.version, Some(1));
    assert_eq!(request.method, Some("GET"));
    assert_eq!(request.path, Some(TEST_URL));
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

        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::default(), NoExtProxy);
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

async fn expect_server_error(response: Response<()>, expected_error: HttpError) {
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::default(), NoExtProxy);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        assert!(client_tx.trigger());
        assert!(server_rx.await.is_ok());

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
        assert!(client_rx.await.is_ok());
        server.write_response(response).await.unwrap();
        assert!(server_tx.trigger());
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

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::default(), NoExtProxy);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        assert!(client_tx.trigger());
        assert!(server_rx.await.is_ok());
        assert!(machine.read().await.is_ok());
    };

    let server_task = async move {
        assert!(client_rx.await.is_ok());

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

        assert!(server_tx.trigger());
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

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::default(), NoExtProxy);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        assert!(client_tx.trigger());
        assert!(server_rx.await.is_ok());

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
        assert!(client_rx.await.is_ok());

        let response = Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .version(Version::HTTP_11)
            .header(header::LOCATION, redirected_to)
            .body(())
            .unwrap();

        server.write_response(response).await.unwrap();

        assert!(server_tx.trigger());
    };

    let _result = join(client_task, server_task).await;
}

async fn subprotocol_test<I, F>(registry: I, response_protocol: Option<String>, match_fn: F)
where
    I: IntoIterator<Item = &'static str>,
    F: Fn(Result<HandshakeResult<NoExt>, Error>),
{
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine =
            ClientHandshake::new(&mut stream, ProtocolRegistry::new(registry), NoExtProxy);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        assert!(client_tx.trigger());
        assert!(server_rx.await.is_ok());
        match_fn(machine.read().await);
    };

    let server_task = async move {
        assert!(client_rx.await.is_ok());

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
        assert!(server_tx.trigger());
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

struct MockExtensionProxy<R>(&'static [(HeaderName, &'static str)], R)
where
    R: for<'h> Fn(&'h httparse::Response) -> Result<Option<MockExtension>, ExtHandshakeErr>;

impl<R> ExtensionHandshake for MockExtensionProxy<R>
where
    R: for<'h> Fn(&'h httparse::Response) -> Result<Option<MockExtension>, ExtHandshakeErr>,
{
    type Extension = MockExtension;

    fn apply_headers(&self, header_map: &mut HeaderMap) {
        for (name, value) in self.0 {
            header_map.insert(name, HeaderValue::from_static(value));
        }
    }

    fn negotiate(
        &self,
        response: &httparse::Response,
    ) -> Result<Option<Self::Extension>, ExtHandshakeErr> {
        (self.1)(response)
    }
}

#[derive(Debug, PartialEq)]
struct MockExtension;
impl Extension for MockExtension {
    fn encode(&mut self) {
        panic!("Unexpected encode invocation")
    }

    fn decode(&mut self) {
        panic!("Unexpected decode invocation")
    }
}

async fn extension_test<E, F, R>(ext: E, response_fn: F, result_fn: R)
where
    E: ExtensionHandshake,
    F: Fn(&mut Response<()>),
    R: Fn(Result<HandshakeResult<E::Extension>, Error>),
{
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine = ClientHandshake::new(&mut stream, ProtocolRegistry::default(), ext);
        machine
            .encode(Request::get(TEST_URL).body(()).unwrap())
            .unwrap();
        machine.write().await.unwrap();
        machine.clear_buffer();

        assert!(client_tx.trigger());
        assert!(server_rx.await.is_ok());

        let read_result = machine.read().await;
        result_fn(read_result);
    };

    let server_task = async move {
        assert!(client_rx.await.is_ok());

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

        assert!(server_tx.trigger());
    };

    let _result = join(client_task, server_task).await;
}

#[tokio::test]
async fn negotiates_extension() {
    const EXT: &str = "test_extension; something=1, something_else=2";
    const HEADERS: &'static [(HeaderName, &'static str)] =
        &[(header::SEC_WEBSOCKET_EXTENSIONS, EXT)];

    let extension_proxy = MockExtensionProxy(&HEADERS, |response| {
        let ext = response
            .headers
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
                    Ok(Some(MockExtension))
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
            Ok(handshake_result) => {
                if let NegotiatedExtension::None(_) = handshake_result.extension {
                    panic!("No extension negotiated")
                }
            }
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
    let extension_proxy = MockExtensionProxy(&HEADERS, |_| Ok(None));

    extension_test(
        extension_proxy,
        |_| {},
        |result| match result {
            Ok(handshake_result) => {
                if let NegotiatedExtension::Negotiated(_) = handshake_result.extension {
                    panic!("Unexpected extension negotiated")
                }
            }
            Err(e) => {
                panic!("Expected a valid upgrade: {:?}", e)
            }
        },
    )
    .await;
}
