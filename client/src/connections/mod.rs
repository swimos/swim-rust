use std::collections::HashMap;

use async_trait::async_trait;
use futures::stream::ErrInto;
use futures::{Sink, StreamExt};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use std::error::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, tungstenite, WebSocketStream};
use url;

#[cfg(test)]
mod tests;

struct ConnectionPoolMessage {
    host: String,
    message: String,
}

struct ConnectionPool {
    tx: mpsc::Sender<ConnectionPoolMessage>,
    send_handler: JoinHandle<Result<(), ConnectionError>>,
    receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl ConnectionPool {
    fn new<T: ConnectionProducer + Send + std::marker::Sync + 'static>(
        buffer_size: usize,
        router_tx: mpsc::Sender<Message>,
        connection_producer: T,
    ) -> ConnectionPool {
        let (tx, rx) = mpsc::channel(buffer_size);
        let (connections_tx, connections_rx) = mpsc::channel(buffer_size);

        let send_handler = tokio::spawn(ConnectionPool::send_messages(
            rx,
            connections_tx,
            connection_producer,
            buffer_size,
        ));

        let receive_handler =
            tokio::spawn(ConnectionPool::receive_messages(connections_rx, router_tx));

        ConnectionPool {
            tx,
            send_handler,
            receive_handler,
        }
    }

    async fn send_messages<T: ConnectionProducer + Send + std::marker::Sync + 'static>(
        mut rx: mpsc::Receiver<ConnectionPoolMessage>,
        connections_tx: mpsc::Sender<Message>,
        connection_producer: T,
        buffer_size: usize,
    ) -> Result<(), ConnectionError> {
        let mut connections = HashMap::new();
        loop {
            let connection_pool_message = rx
                .try_recv()
                .map_err(|_| ConnectionError::SendMessageError)?;

            if !&connections.contains_key(&connection_pool_message.host) {
                let pool_tx = connections_tx.clone();

                let connection = connection_producer
                    .create_connection(&connection_pool_message.host, buffer_size, pool_tx)
                    .await?;

                connections.insert(connection_pool_message.host.to_string(), connection);
            }

            let connection = connections
                .get_mut(&connection_pool_message.host)
                .ok_or(ConnectionError::ConnectError)?;

            connection
                .send_message(&connection_pool_message.message)
                .await?;
        }
    }

    async fn receive_messages(
        rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<Message>,
    ) -> Result<(), ConnectionError> {
        rx.for_each(|response| {
            async {
                // TODO print statement is for debugging only
                println!("{:?}", response);
                tx.clone()
                    .send(response)
                    .await
                    .map_err(|_| ConnectionError::SendMessageError);
            }
        })
        .await;
        Ok(())
    }

    fn send_message(&mut self, host: &str, message: &str) -> Result<(), ConnectionError> {
        self.tx
            .try_send(ConnectionPoolMessage {
                host: host.to_string(),
                message: message.to_string(),
            })
            .map_err(|_| ConnectionError::SendMessageError)
    }
}

#[async_trait]
trait ConnectionProducer {
    type T: Connection + Send + 'static;

    async fn create_connection(
        &self,
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
    ) -> Result<Self::T, ConnectionError>;
}

struct SwimConnectionProducer {}

#[async_trait]
impl ConnectionProducer for SwimConnectionProducer {
    type T = SwimConnection;

    async fn create_connection(
        &self,
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
    ) -> Result<Self::T, ConnectionError> {
        SwimConnection::new(host, buffer_size, pool_tx, SwimWebsocketProducer {}).await
    }
}

#[async_trait]
trait Connection: Sized {
    async fn send_messages<S>(
        write_stream: S,
        rx: mpsc::Receiver<Message>,
    ) -> Result<(), ConnectionError>
    where
        S: Sink<Message> + Send + 'static,
    {
        rx.map(Ok)
            .forward(write_stream)
            .await
            .map_err(|_| ConnectionError::SendMessageError)?;
        Ok(())
    }

    async fn receive_messages<S>(
        pool_tx: mpsc::Sender<Message>,
        read_stream: S,
    ) -> Result<(), ConnectionError>
    where
        S: TryStreamExt<Ok = Message, Error = ConnectionError> + Send + 'static,
    {
        read_stream
            .try_for_each(|response| {
                async {
                    pool_tx
                        .clone()
                        .send(response)
                        .await
                        .map_err(|_| ConnectionError::SendMessageError)
                }
            })
            .await?;

        Ok(())
    }

    async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError>;
}

struct SwimConnection {
    tx: mpsc::Sender<Message>,
    send_handler: JoinHandle<Result<(), ConnectionError>>,
    receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl SwimConnection {
    async fn new<T: WebsocketProducer + Send + std::marker::Sync + 'static>(
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
        websocket_producer: T,
    ) -> Result<SwimConnection, ConnectionError> {
        let url = url::Url::parse(&host)?;
        let (tx, rx) = mpsc::channel(buffer_size);

        let ws_stream = websocket_producer.connect(url).await?;
        let (write_stream, read_stream) = ws_stream.split();

        let receive = SwimConnection::receive_messages(pool_tx, read_stream);
        let send = SwimConnection::send_messages(write_stream, rx);

        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        Ok(SwimConnection {
            tx,
            send_handler,
            receive_handler,
        })
    }
}

#[async_trait]
impl Connection for SwimConnection {
    //noinspection ALL
    async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx.send(Message::text(message)).await?;
        Ok(())
    }
}

#[async_trait]
trait WebsocketProducer {
    type T: Stream<Item = Result<Message, ConnectionError>>
        + Sink<Message>
        + std::marker::Send
        + 'static;

    async fn connect(self, url: url::Url) -> Result<Self::T, ConnectionError>;
}

struct SwimWebsocketProducer {}

#[async_trait]
impl WebsocketProducer for SwimWebsocketProducer {
    type T = ErrInto<WebSocketStream<TcpStream>, ConnectionError>;

    async fn connect(self, url: url::Url) -> Result<Self::T, ConnectionError> {
        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|_| ConnectionError::ConnectError)?;
        let ws_stream = ws_stream.err_into::<ConnectionError>();
        Ok(ws_stream)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionError {
    ParseError,
    ConnectError,
    SendMessageError,
}

impl From<url::ParseError> for ConnectionError {
    fn from(_: url::ParseError) -> Self {
        ConnectionError::ParseError
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<tungstenite::error::Error> for ConnectionError {
    fn from(_: tungstenite::error::Error) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<tokio::sync::mpsc::error::SendError<Message>> for ConnectionError {
    fn from(_: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        ConnectionError::SendMessageError
    }
}

impl From<mpsc::error::TrySendError<ConnectionPoolMessage>> for ConnectionError {
    fn from(_: mpsc::error::TrySendError<ConnectionPoolMessage>) -> Self {
        ConnectionError::SendMessageError
    }
}
