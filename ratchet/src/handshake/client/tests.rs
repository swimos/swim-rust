use crate::errors::{Error, ErrorKind, HttpError};
use crate::fixture::{mock, MockPeer};
use crate::handshake::client::HandshakeMachine;
use crate::handshake::{UPGRADE_STR, WEBSOCKET_STR, WEBSOCKET_VERSION, WEBSOCKET_VERSION_STR};
use crate::TryIntoRequest;
use futures::future::join;
use http::{header, Request, Response, StatusCode, Version};
use httparse::{Header, Status};
use std::any::Any;
use std::error::Error as StdError;
use utilities::sync::trigger;

const TEST_URL: &str = "ws://127.0.0.1:9001/test";

#[tokio::test]
async fn handshake_sends_valid_request() {
    let request = TEST_URL.try_into_request().unwrap();
    let (peer, mut stream) = mock();

    let mut machine = HandshakeMachine::new(&mut stream, Vec::new(), Vec::new());
    machine.encode(request).expect("");
    machine.buffered.write().await.expect("");

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);

    let mut guard = peer.rx_buf.lock().unwrap();
    let mut rx_buf = &mut (*guard);
    let buf_len = rx_buf.len();

    assert_eq!(request.parse(&mut rx_buf), Ok(Status::Complete(buf_len)));
    assert_eq!(request.version, Some(1));
    assert_eq!(request.method, Some("GET"));
    assert_eq!(request.path, Some(TEST_URL));
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
        let (peer, mut stream) = mock();

        let mut machine = HandshakeMachine::new(&mut stream, Vec::new(), Vec::new());
        machine
            .encode(request)
            .expect_err("Expected encoding to fail");
        machine.buffered.write().await.expect("Unexpected IO error");

        let mut guard = peer.rx_buf.lock().unwrap();
        let rx_buf = &mut (*guard);
        assert!(rx_buf.is_empty());
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
}

fn expect_error<E>(actual: Result<(), Error>, expected: E)
where
    E: StdError + PartialEq<E> + Any,
{
    const ERR: &str = "Expected an error";

    actual
        .err()
        .map(|e| {
            let error = e.downcast_ref::<E>().expect(ERR);
            assert!(error.eq(&expected))
        })
        .expect(ERR);
}

async fn expect_server_error(response: Response<()>, expected_error: HttpError) {
    let (mut server, mut stream) = mock();

    let (client_tx, client_rx) = trigger::trigger();
    let (server_tx, server_rx) = trigger::trigger();

    let client_task = async move {
        let mut machine = HandshakeMachine::new(&mut stream, Vec::new(), Vec::new());
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

        let request = server.read_request();

        server.write_response(response);
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
async fn redirection() {}
