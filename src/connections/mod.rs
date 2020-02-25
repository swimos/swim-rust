use futures_util::stream::SplitStream;
use futures::StreamExt;
use tokio_tungstenite::{connect_async, WebSocketStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use url;

#[cfg(test)]
mod tests;

struct Connection {
    url: url::Url,
    rx: mpsc::Receiver<Message>,
}

impl Connection {
    fn new(host: &str, buffer_size: usize) -> Result<(Connection, mpsc::Sender<Message>), ConnectionError> {
        let url = url::Url::parse(&host)?;
        let (tx, rx) = mpsc::channel(buffer_size);

        return Ok((Connection { url, rx }, tx));
    }

    async fn open(self) -> Result<(), ConnectionError> {
        let (ws_stream, _) = tokio::spawn(connect_async(self.url)).await??;
        let (write_stream, read_stream) = ws_stream.split();

        let receive = Connection::receive_messages(read_stream);
        let send = self.rx.map(Ok).forward(write_stream);

        tokio::spawn(send);
        tokio::spawn(receive);
        return Ok(());
    }

    async fn receive_messages(mut read_stream: SplitStream<WebSocketStream<TcpStream>>) {
        while let Some(message) = read_stream.next().await {
            if let Ok(m) = message {
                // TODO Add a real callback
                println!("{}", m);
            }
        }
    }

    async fn send_message(mut tx: mpsc::Sender<Message>, message: String) -> Result<(), ConnectionError> {
        tx.send(Message::text(message)).await?;
        return Ok(());
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionError
{
    ParseError,
    ConnectError,
    SendMessageError,

}

impl std::error::Error for ConnectionError {}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConnectionError::ParseError => write!(f, "Parse error!"),
            ConnectionError::ConnectError => write!(f, "Connect error!"),
            ConnectionError::SendMessageError => write!(f, "Send message error!")
        }
    }
}

impl From<url::ParseError, > for ConnectionError {
    fn from(_: url::ParseError) -> Self {
        ConnectionError::ParseError
    }
}

impl From<tokio::task::JoinError, > for ConnectionError {
    fn from(_: tokio::task::JoinError) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<tungstenite::error::Error, > for ConnectionError {
    fn from(_: tungstenite::error::Error) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<tokio::sync::mpsc::error::SendError<Message>, > for ConnectionError {
    fn from(_: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        ConnectionError::SendMessageError
    }
}