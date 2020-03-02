use std::collections::HashMap;
use std::future::Future;

use futures::future::FutureExt;
use futures::StreamExt;
use futures_util::stream::SplitStream;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, WebSocketStream};
use url;

#[cfg(test)]
mod tests;

type ConnectionPoolMessage = (String, String);

struct ConnectionPool {
    connections: HashMap<String, ConnectionHandler>,
    rx: mpsc::Receiver<ConnectionPoolMessage>,
}

impl ConnectionPool {
    fn new(buffer_size: usize) -> (ConnectionPool, ConnectionPoolHandler) {
        let (tx, rx) = mpsc::channel(buffer_size);
        return (
            ConnectionPool {
                connections: HashMap::new(),
                rx,
            },
            ConnectionPoolHandler { tx },
        );
    }

    fn open(self, client: &Client) -> Result<(), ConnectionError> {
        let handle_messages = self.handle_messages();

        client.schedule_task(handle_messages);
        return Ok(());
    }

    async fn handle_messages(mut self) -> Result<(), ConnectionError> {
        loop {
            let response = self.rx.recv().await;
            match response {
                Some((host, message)) => {
                    let handler = self.get_connection(&host).await?;
                    handler.send_message(&message).await?;
                }
                None => (),
            }
        }
    }

    async fn get_connection(
        &mut self,
        host: &str,
    ) -> Result<&mut ConnectionHandler, ConnectionError> {
        if !self.connections.contains_key(host) {
            // Todo buffer size is hardcoded
            let (connection, connection_handler) = Connection::new(host, 5)?;
            connection.open().await?;
            self.connections
                .insert(host.to_string(), connection_handler);
        }
        return Ok(self
            .connections
            .get_mut(host)
            .ok_or(ConnectionError::ConnectError)?);
    }

    async fn receive_message(host: &str, message: Message) {
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
        self.tx.try_send((host.to_string(), message.to_string()))?;
        return Ok(());
    }
}

struct Connection {
    url: url::Url,
    rx: mpsc::Receiver<Message>,
}

impl Connection {
    fn new(
        host: &str,
        buffer_size: usize,
    ) -> Result<(Connection, ConnectionHandler), ConnectionError> {
        let url = url::Url::parse(&host)?;
        let (tx, rx) = mpsc::channel(buffer_size);

        return Ok((Connection { url, rx }, ConnectionHandler { tx }));
    }

    async fn open(self) -> Result<(), ConnectionError> {
        let (ws_stream, _) = connect_async(&self.url).await?;
        let (write_stream, read_stream) = ws_stream.split();

        let receive = Connection::receive_messages(read_stream, self.url.to_string().to_owned());
        let send = self.rx.map(Ok).forward(write_stream);

        tokio::spawn(send);
        tokio::spawn(receive);
        return Ok(());
    }

    async fn receive_messages(read_stream: SplitStream<WebSocketStream<TcpStream>>, host: String) {
        read_stream
            .for_each(|response| {
                async {
                    if let Ok(message) = response {
                        ConnectionPool::receive_message(&host, message).await;
                    }
                }
            })
            .await;
    }
}

#[derive(Debug, Clone)]
struct ConnectionHandler {
    tx: mpsc::Sender<Message>,
}

impl ConnectionHandler {
    async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx.send(Message::text(message)).await?;
        return Ok(());
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionError {
    ParseError,
    ConnectError,
    SendMessageError,
}

#[derive(Debug, Clone)]
pub enum ClientError {
    RuntimeError,
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

impl From<std::io::Error> for ClientError {
    fn from(_: std::io::Error) -> Self {
        ClientError::RuntimeError
    }
}

impl From<TrySendError<ConnectionPoolMessage>> for ConnectionError {
    fn from(_: TrySendError<ConnectionPoolMessage>) -> Self {
        ConnectionError::SendMessageError
    }
}

struct Client {
    rt: tokio::runtime::Runtime,
}

impl Client {
    fn new() -> Result<Client, ClientError> {
        let rt = tokio::runtime::Runtime::new()?;
        return Ok(Client { rt });
    }

    fn schedule_task<F>(
        &self,
        task: impl Future<Output = Result<F, ConnectionError>> + Send + 'static,
    ) where
        F: Send + 'static,
    {
        &self.rt.spawn(
            async move { task.await }.inspect(|response| match response {
                Err(e) => println!("{:?}", e),
                Ok(_) => (),
            }),
        );
    }
}
