use ratchet::{NoExtProxy, ProtocolRegistry, WebSocketConfig, WebSocketResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
    let mut incoming = TcpListenerStream::new(listener);

    while let Some(socket) = incoming.next().await {
        let socket = socket.unwrap();

        let upgrader = ratchet::accept(
            socket,
            WebSocketConfig::default(),
            NoExtProxy,
            ProtocolRegistry::default(),
        )
        .await
        .unwrap();

        upgrader
            .reject(WebSocketResponse::new(500).unwrap())
            .await
            .unwrap();
    }
}
