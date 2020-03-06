use std::collections::HashMap;
use std::future::Future;

use futures::future::FutureExt;
use futures::{Sink, StreamExt};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use url;

#[cfg(test)]
mod tests;

struct ConnectionPoolMessage {
    host: String,
    message: String,
}

struct ConnectionPool {
    connections: HashMap<String, ConnectionHandler>,
    rx: mpsc::Receiver<ConnectionPoolMessage>,
    connections_rx: mpsc::Receiver<Message>,
    connections_tx: mpsc::Sender<Message>,
}

impl ConnectionPool {
    fn new(buffer_size: usize) -> (ConnectionPool, ConnectionPoolHandler) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let (connections_tx, connections_rx) = mpsc::channel(buffer_size);
        (
            ConnectionPool {
                connections: HashMap::new(),
                rx,
                connections_rx,
                connections_tx,
            },
            ConnectionPoolHandler { tx },
        )
    }

    fn open(self, client: &Client) -> JoinHandle<Result<(), ConnectionError>> {
        let handle_messages = self.handle_messages();
        client.schedule_task(handle_messages)
    }

    async fn handle_messages(mut self) -> Result<(), ConnectionError> {
        loop {
            let response = self.rx.recv().await;
            match response {
                Some(connection_pool_message) => {
                    if !self.connections.contains_key(&connection_pool_message.host) {
                        self.open_connection(&connection_pool_message.host).await?;
                    }

                    let handler = self
                        .connections
                        .get_mut(&connection_pool_message.host)
                        .ok_or(ConnectionError::ConnectError)?;

                    handler
                        .send_message(&connection_pool_message.message)
                        .await?;
                }
                None => (),
            }
        }
    }

    async fn open_connection(&mut self, host: &str) -> Result<ConnectionHandler, ConnectionError> {
        // Todo buffer size is hardcoded
        let (connection, connection_handler) =
            Connection::new(host, 5, self.connections_tx.clone())?;
        connection.open().await?;
        self.connections
            .insert(host.to_string(), connection_handler);

        Ok(self
            .connections
            .get(host)
            .ok_or(ConnectionError::ConnectError)?
            .clone())
    }

    async fn receive_message(self, host: &str, message: Message) {
        println!("Host: {:?}", host);
        println!("Message: {:?}", message.to_text().unwrap());
        // TODO this will call the `receive_message` of the Router
    }
}

struct ConnectionPoolHandler {
    tx: mpsc::Sender<ConnectionPoolMessage>,
}

impl ConnectionPoolHandler {
    fn send_message(&mut self, host: &str, message: &str) -> Result<(), ConnectionError> {
        self.tx.try_send(ConnectionPoolMessage {
            host: host.to_string(),
            message: message.to_string(),
        })?;
        Ok(())
    }
}

struct Connection {
    url: url::Url,
    rx: mpsc::Receiver<Message>,
    pool_tx: mpsc::Sender<Message>,
}

impl Connection {
    fn new(
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
    ) -> Result<(Connection, ConnectionHandler), ConnectionError> {
        let url = url::Url::parse(&host)?;
        let (tx, rx) = mpsc::channel(buffer_size);

        Ok((Connection { url, rx, pool_tx }, ConnectionHandler { tx }))
    }

    async fn open(
        self,
    ) -> Result<
        (
            JoinHandle<Result<(), ConnectionError>>,
            JoinHandle<Result<(), ConnectionError>>,
        ),
        ConnectionError,
    > {
        let (ws_stream, _) = connect_async(&self.url).await?;
        let ws_stream = ws_stream.map_err(|_| ConnectionError::SendMessageError);
        Ok(self.start(ws_stream))
    }

    fn start<S>(
        self,
        ws_stream: S,
    ) -> (
        JoinHandle<Result<(), ConnectionError>>,
        JoinHandle<Result<(), ConnectionError>>,
    )
    where
        S: Stream<Item = Result<Message, ConnectionError>>
            + Sink<Message>
            + std::marker::Send
            + 'static,
    {
        let (write_stream, read_stream) = ws_stream.split();

        let receive = Connection::receive_messages(self.pool_tx.clone(), read_stream);
        let send = self.send_message(write_stream);

        let send_handle = tokio::spawn(send);
        let receive_handle = tokio::spawn(receive);

        (send_handle, receive_handle)
    }

    async fn send_message<S>(self, write_stream: S) -> Result<(), ConnectionError>
    where
        S: Sink<Message> + std::marker::Send + 'static,
    {
        self.rx
            .map(Ok)
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
        S: TryStreamExt<Ok = Message, Error = ConnectionError>,
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
}

#[derive(Debug, Clone)]
struct ConnectionHandler {
    tx: mpsc::Sender<Message>,
}

impl ConnectionHandler {
    async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx.send(Message::text(message)).await?;
        Ok(())
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

impl From<TrySendError<ConnectionPoolMessage>> for ConnectionError {
    fn from(_: TrySendError<ConnectionPoolMessage>) -> Self {
        ConnectionError::SendMessageError
    }
}

#[derive(Debug, Clone)]
pub enum ClientError {
    RuntimeError,
}

impl From<std::io::Error> for ClientError {
    fn from(_: std::io::Error) -> Self {
        ClientError::RuntimeError
    }
}

struct Client {
    rt: tokio::runtime::Runtime,
}

impl Client {
    fn new() -> Result<Client, ClientError> {
        let rt = tokio::runtime::Runtime::new()?;
        Ok(Client { rt })
    }

    fn schedule_task<F>(
        &self,
        task: impl Future<Output = Result<F, ConnectionError>> + Send + 'static,
    ) -> JoinHandle<Result<F, ConnectionError>>
    where
        F: Send + 'static,
    {
        self.rt.spawn(
            async move { task.await }.inspect(|response| match response {
                Err(e) => println!("{:?}", e),
                Ok(_) => (),
            }),
        )
    }
}
