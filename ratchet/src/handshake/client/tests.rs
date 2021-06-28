use crate::fixture::mock;
use crate::handshake::client::HandshakeMachine;
use crate::TryIntoRequest;
use http::{header, Request};
use httparse::{Header, Status};

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
            .header(header::SEC_WEBSOCKET_KEY, "31231321321")
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
