use ratchet::{client, NoExtProxy, ProtocolRegistry, TryIntoRequest, WebSocketConfig};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:9001").await.unwrap();
    stream.set_nodelay(true).unwrap();

    client(
        WebSocketConfig::default(),
        stream,
        "ws://127.0.0.1/hello".try_into_request().unwrap(),
        NoExtProxy,
        ProtocolRegistry::default(),
    )
    .await
    .unwrap();
}
