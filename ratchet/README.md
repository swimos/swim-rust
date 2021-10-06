<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br>

# Ratchet
Ratchet is a fast, robust, lightweight and fully asynchronous implementation of [RFC6455](https://datatracker.ietf.org/doc/html/rfc6455) (The WebSocket protocol). Complete with an optional implementation of [RFC7692](https://datatracker.ietf.org/doc/html/rfc7692) (Compression Extensions For WebSocket).

Ratchet is fast, robust and lightweight and the WebSocket is capable of being split into its sender and receiver halves.

# Features
- Implement your own extensions using [ratchet_ext](/ratchet_ext).
- Per-message deflate with [ratchet_deflate](/ratchet_deflate) or enable with the `deflate`
  feature.
- Split WebSocket with the `split` feature.

# Testing
Ratchet is fully tested and passes every Autobahn test for both client and server modes.

# Examples
## Client
```rust
#[tokio::main]
async fn main() -> Result<(), Error> {
  let stream = TcpStream::connect("127.0.0.1:9001").await?;
  
  let upgraded = subscribe(
    WebSocketConfig::default(),
    stream,
    "ws://127.0.0.1/hello".try_into_request()?,
  )
  .await?;

  let UpgradedClient{ socket, subprotocol }=upgraded;
  let mut buf = BytesMut::new();

  loop {
    match websocket.read(&mut buf).await? {
      Message::Text => {
        websocket.write(&mut buf, PayloadType::Text).await?;
        buf.clear();
      }
      Message::Binary => {
        websocket.write(&mut buf, PayloadType::Binary).await?;
        buf.clear();
      }
      Message::Ping | Message::Pong => {
        // Ping messages are transparently handled by Ratchet
      }
      Message::Close(_) => break Ok(()),
    }
  }
}
```

## Server
```rust
#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:9001").await?;
    let mut incoming = TcpListenerStream::new(listener);

    while let Some(socket) = incoming.next().await {
        let socket = socket?;

        // An upgrader contains information about what the peer has requested.
        let mut upgrader = ratchet::accept_with(
            socket,
            WebSocketConfig::default(),
            NoExtProvider,
            ProtocolRegistry::default(),
        )
        .await?;

        // You could opt to reject the connection
        // upgrader.reject(WebSocketResponse::new(404)?).await?;
        // continue;
      
        // Or you could reject the connection with headers
        // upgrader.reject(WebSocketResponse::with_headers(404, headers)?).await;
        // continue;

        let UpgradedServer {
            request,
            mut websocket,
            subprotocol,
        } = upgrader.upgrade().await?;
        
        let mut buf = BytesMut::new();

        loop {
            match websocket.read(&mut buf).await? {
                Message::Text => {
                    websocket.write(&mut buf, PayloadType::Text).await?;
                    buf.clear();
                }
                Message::Binary => {
                    websocket.write(&mut buf, PayloadType::Binary).await?;
                    buf.clear();
                }
                Message::Ping | Message::Pong => {
                  // Ping messages are transparently handled by Ratchet
                }
                Message::Close(_) => break,
            }
        }
    }
    
    Ok(())
}
```
## Deflate
```rust
  let mut websocket = ratchet::accept_with(
      socket,
      WebSocketConfig::default(),
      DeflateProvider,
      ProtocolRegistry::default(),
  )
  .await?;
```

## Split
```rust
// A split operation will only fail if the WebSocket is already closed.
let (mut sender, mut receiver) = websocket.split()?;
    
loop {
    match receiver.read(&mut buf).await? {
        Message::Text => {
            sender.write(&mut buf, PayloadType::Text).await?;
            buf.clear();
        }
        Message::Binary => {
            sender.write(&mut buf, PayloadType::Binary).await?;
            buf.clear();
        }
        Message::Ping | Message::Pong => {}
        Message::Close(_) => break Ok(()),
    }
}
```
# Planned features
- `futures-rs` `Sink` and `Stream` implementations.
- `tokio` `AsyncRead` and `AsyncWrite` implementations.

# License
Ratchet is licensed under the [Apache License 2.0](LICENSE)