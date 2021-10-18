// Copyright 2015-2021 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::future::BoxFuture;
use futures::FutureExt;
use futures::StreamExt;
use ratchet::{NoExt, WebSocket};
use ratchet_fixture::duplex::MockWebSocket;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use url::Url;

use crate::configuration::router::ConnectionPoolParams;
use crate::connections::factory::async_factory::AsyncFactory;
use crate::connections::factory::HostConfig;
use crate::connections::tests::failing_ext::{FailOn, FailingExt};
use crate::connections::{ConnectionPool, SwimConnPool, SwimConnection};
use futures::future::join;
use swim_runtime::error::{CapacityError, CapacityErrorKind, ConnectionError, ProtocolError};
use swim_runtime::ws::{
    into_stream, CompressionSwitcherProvider, Protocol, WebsocketFactory, WsMessage,
};
use tokio::sync::Mutex;

#[tokio::test]
async fn test_connection_pool_send_single_message_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let text = "Hello";

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(vec![], writer_tx);

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message(text.to_string().into())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Hello".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";

    let (writer_tx, mut writer_rx) = mpsc::channel(5);

    let test_data = TestData::new(vec![], writer_tx);

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(host_url.clone(), false)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_message(first_text.to_string().into())
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text.to_string().into())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("First_Text".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Second_Text".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_multiple_connections() {
    // Given
    let first_host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3//").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";
    let third_text = "Third_Text";

    let (first_writer_tx, mut first_writer_rx) = mpsc::channel(5);

    let (second_writer_tx, mut second_writer_rx) = mpsc::channel(5);

    let (third_writer_tx, mut third_writer_rx) = mpsc::channel(5);

    let test_data = vec![
        TestData::new(vec![], first_writer_tx),
        TestData::new(vec![], second_writer_tx),
        TestData::new(vec![], third_writer_tx),
    ];

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new_multiple(test_data).await,
    );

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(first_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(second_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (mut third_connection_sender, _third_connection_receiver) = connection_pool
        .request_connection(third_host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_message(first_text.to_string().into())
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text.to_string().into())
        .await
        .unwrap();
    third_connection_sender
        .send_message(third_text.to_string().into())
        .await
        .unwrap();

    // Then
    assert_eq!(
        first_writer_rx.recv().await.unwrap(),
        WsMessage::Text("First_Text".to_string())
    );
    assert_eq!(
        second_writer_rx.recv().await.unwrap(),
        WsMessage::Text("Second_Text".to_string())
    );
    assert_eq!(
        third_writer_rx.recv().await.unwrap(),
        WsMessage::Text("Third_Text".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let mut items = Vec::new();
    items.push("new_message".to_string());
    let (writer_tx, _writer_rx) = mpsc::channel(5);

    let test_data = TestData::new(items, writer_tx);
    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();
    assert_eq!(pool_message, WsMessage::Text("new_message".to_string()));
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let mut items = Vec::new();
    items.push("first_message".to_string());
    items.push("second_message".to_string());
    items.push("third_message".to_string());
    let (writer_tx, _writer_rx) = mpsc::channel(5);

    let test_data = TestData::new(items, writer_tx);
    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    let mut connection_receiver = connection_receiver.unwrap();

    // Then
    let first_pool_message = connection_receiver.recv().await.unwrap();
    let second_pool_message = connection_receiver.recv().await.unwrap();
    let third_pool_message = connection_receiver.recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        WsMessage::Text("first_message".to_string())
    );
    assert_eq!(
        second_pool_message,
        WsMessage::Text("second_message".to_string())
    );
    assert_eq!(
        third_pool_message,
        WsMessage::Text("third_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_multiple_connections() {
    // Given
    let mut first_items = Vec::new();
    let mut second_items = Vec::new();
    let mut third_items = Vec::new();

    first_items.push("first_message".to_string());
    second_items.push("second_message".to_string());
    third_items.push("third_message".to_string());

    let first_host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3//").unwrap();

    let (first_writer_tx, _first_writer_rx) = mpsc::channel(5);
    let (second_writer_tx, _second_writer_rx) = mpsc::channel(5);
    let (third_writer_tx, _third_writer_rx) = mpsc::channel(5);

    let test_data = vec![
        TestData::new(first_items, first_writer_tx),
        TestData::new(second_items, second_writer_tx),
        TestData::new(third_items, third_writer_tx),
    ];

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new_multiple(test_data).await,
    );

    // When
    let (_first_sender, mut first_receiver) = connection_pool
        .request_connection(first_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (_second_sender, mut second_receiver) = connection_pool
        .request_connection(second_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (_third_sender, mut third_receiver) = connection_pool
        .request_connection(third_host_url, false)
        .await
        .unwrap()
        .unwrap();

    // Then
    let first_pool_message = first_receiver.take().unwrap().recv().await.unwrap();
    let second_pool_message = second_receiver.take().unwrap().recv().await.unwrap();
    let third_pool_message = third_receiver.take().unwrap().recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        WsMessage::Text("first_message".to_string())
    );
    assert_eq!(
        second_pool_message,
        WsMessage::Text("second_message".to_string())
    );
    assert_eq!(
        third_pool_message,
        WsMessage::Text("third_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let mut items = Vec::new();
    items.push("recv_baz".to_string());
    let (writer_tx, mut writer_rx) = mpsc::channel(5);

    let test_data = TestData::new(items, writer_tx);

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    let (mut connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message("send_bar".to_string().into())
        .await
        .unwrap();
    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();

    assert_eq!(pool_message, WsMessage::Text("recv_baz".to_string()));

    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("send_bar".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_connection_error() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new_multiple(vec![]).await,
    );

    // When

    let connection = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap();

    // Then
    assert!(connection.is_err());
}

#[tokio::test]
async fn test_connection_pool_connection_error_send_message() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let text = "Test_message";

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let test_data = vec![None, Some(TestData::new(vec![], writer_tx))];

    let mut connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new_multiple_with_errs(test_data).await,
    );

    // When
    let first_connection = connection_pool
        .request_connection(host_url.clone(), false)
        .await
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(host_url.clone(), false)
        .await
        .unwrap()
        .unwrap();

    second_connection_sender
        .send_message(text.to_string().into())
        .await
        .unwrap();

    // Then
    assert!(first_connection.is_err());
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Test_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_close() {
    // Given
    let (writer_tx, mut writer_rx) = mpsc::channel(5);

    let test_data = TestData::new(vec![], writer_tx);

    let connection_pool = SwimConnPool::new(
        ConnectionPoolParams::default(),
        TestConnectionFactory::new(test_data).await,
    );

    // When
    assert!(connection_pool.close().await.is_ok());

    // Then
    assert!(writer_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
    let buffer_size = 5;
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(vec![], writer_tx);

    let mut factory = TestConnectionFactory::new(test_data).await;

    let connection = SwimConnection::new(host, buffer_size, &mut factory)
        .await
        .unwrap();

    // When
    connection.tx.send("foo".to_string().into()).await.unwrap();
    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("foo".to_string())
    );
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
    let buffer_size = 5;
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(vec![], writer_tx);

    let mut factory = TestConnectionFactory::new(test_data).await;

    let mut connection = SwimConnection::new(host, buffer_size, &mut factory)
        .await
        .unwrap();

    let connection_sender = &mut connection.tx;
    // When
    connection_sender
        .send("foo".to_string().into())
        .await
        .unwrap();
    connection_sender
        .send("bar".to_string().into())
        .await
        .unwrap();
    connection_sender
        .send("baz".to_string().into())
        .await
        .unwrap();
    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("foo".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("bar".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("baz".to_string())
    );
}

#[tokio::test]
async fn test_connection_send_and_receive_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push("message_received".to_string());
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(items, writer_tx);

    let mut factory = TestConnectionFactory::new(test_data).await;

    let mut connection = SwimConnection::new(host, buffer_size, &mut factory)
        .await
        .unwrap();

    // When
    connection
        .tx
        .send("message_sent".to_string().into())
        .await
        .unwrap();
    // Then
    let pool_message = connection.rx.take().unwrap().recv().await.unwrap();
    assert_eq!(
        pool_message,
        WsMessage::Text("message_received".to_string())
    );

    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("message_sent".to_string())
    );
}

#[tokio::test]
async fn test_connection_receive_message_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
    let buffer_size = 5;

    let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(vec!["".to_string()], writer_tx).fail_on_input();

    let mut factory = TestConnectionFactory::new(test_data).await;

    let connection = SwimConnection::new(host, buffer_size, &mut factory)
        .await
        .unwrap();

    // When
    let result = connection._receive_handle.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap(),
        ratchet::Error::with_cause(
            ratchet::ErrorKind::Extension,
            ConnectionError::Protocol(ProtocolError::websocket(None))
        )
        .into()
    );
}

#[tokio::test]
async fn test_new_connection_send_message_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
    let buffer_size = 5;

    let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);

    let test_data = TestData::new(vec!["bbbbbbbbb".to_string()], writer_tx).fail_on_output();

    let mut factory = TestConnectionFactory::new(test_data).await;

    let connection = SwimConnection::new(host, buffer_size, &mut factory)
        .await
        .unwrap();

    connection
        .tx
        .send(WsMessage::Text("boom!".to_string()))
        .await
        .unwrap();

    // When
    let result = connection._send_handle.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap(),
        ratchet::Error::with_cause(
            ratchet::ErrorKind::Extension,
            ConnectionError::Protocol(ProtocolError::websocket(None))
        )
        .into()
    );
}

pub mod failing_ext {
    use bytes::BytesMut;
    use ratchet::{
        Extension, ExtensionDecoder, ExtensionEncoder, FrameHeader, RsvBits, SplittableExtension,
    };
    use std::error::Error;

    #[derive(Debug, Clone)]
    pub enum FailOn<E> {
        Read(E),
        Write(E),
        Rw(E),
        Nothing,
    }

    #[derive(Clone, Debug)]
    pub struct FailingExt<E>(pub FailOn<E>)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> Extension for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        fn bits(&self) -> RsvBits {
            RsvBits {
                rsv1: false,
                rsv2: false,
                rsv3: false,
            }
        }
    }

    impl<E> ExtensionEncoder for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn encode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            match &self.0 {
                FailOn::Write(e) => Err(e.clone()),
                FailOn::Rw(e) => Err(e.clone()),
                _ => Ok(()),
            }
        }
    }

    impl<E> ExtensionDecoder for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn decode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            match &self.0 {
                FailOn::Read(e) => Err(e.clone()),
                FailOn::Rw(e) => Err(e.clone()),
                _ => Ok(()),
            }
        }
    }

    impl<E> SplittableExtension for FailingExt<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type SplitEncoder = FailingExtEnc<E>;
        type SplitDecoder = FailingExtDec<E>;

        fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder) {
            let FailingExt(e) = self;
            (FailingExtEnc(e.clone()), FailingExtDec(e))
        }
    }

    pub struct FailingExtEnc<E>(pub FailOn<E>)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> ExtensionEncoder for FailingExtEnc<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn encode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            match &self.0 {
                FailOn::Write(e) => Err(e.clone()),
                FailOn::Rw(e) => Err(e.clone()),
                _ => Ok(()),
            }
        }
    }

    pub struct FailingExtDec<E>(pub FailOn<E>)
    where
        E: Error + Clone + Send + Sync + 'static;

    impl<E> ExtensionDecoder for FailingExtDec<E>
    where
        E: Error + Clone + Send + Sync + 'static,
    {
        type Error = E;

        fn decode(
            &mut self,
            _payload: &mut BytesMut,
            _header: &mut FrameHeader,
        ) -> Result<(), Self::Error> {
            match &self.0 {
                FailOn::Read(e) => Err(e.clone()),
                FailOn::Rw(e) => Err(e.clone()),
                _ => Ok(()),
            }
        }
    }
}

struct TestConnectionFactory {
    inner: AsyncFactory<DuplexStream, FailingExt<ConnectionError>>,
}

impl TestConnectionFactory {
    async fn new(test_data: TestData) -> Self {
        let shared_data = Arc::new(TestFixture::new(TestDataRepr::Single(test_data)));
        let inner = AsyncFactory::new(5, move |url, _config| {
            let shared_data = shared_data.clone();
            async { shared_data.open_conn(url).await }
        })
        .await;
        TestConnectionFactory { inner }
    }

    async fn new_multiple(test_data: Vec<TestData>) -> Self {
        TestConnectionFactory::new_multiple_with_errs(test_data.into_iter().map(Some).collect())
            .await
    }

    async fn new_multiple_with_errs(test_data: Vec<Option<TestData>>) -> Self {
        let shared_data = Arc::new(TestFixture::new(TestDataRepr::Multiple(Mutex::new(
            MultipleTestData::new(test_data),
        ))));

        let inner = AsyncFactory::new(5, move |url, _config| {
            let shared_data = shared_data.clone();
            async { shared_data.open_conn(url).await }
        })
        .await;
        TestConnectionFactory { inner }
    }
}

impl WebsocketFactory for TestConnectionFactory {
    type Sock = DuplexStream;
    type Ext = FailingExt<ConnectionError>;

    fn connect(
        &mut self,
        url: Url,
    ) -> BoxFuture<Result<WebSocket<Self::Sock, Self::Ext>, ConnectionError>> {
        self.inner
            .connect_using(
                url,
                HostConfig {
                    protocol: Protocol::PlainText,
                    compression_level: CompressionSwitcherProvider::Off,
                },
            )
            .boxed()
    }
}

struct TestFixture {
    repr: TestDataRepr,
}

impl TestFixture {
    pub fn new(repr: TestDataRepr) -> Self {
        TestFixture { repr }
    }

    async fn open_conn(
        self: Arc<Self>,
        url: url::Url,
    ) -> Result<MockWebSocket<FailingExt<ConnectionError>>, ConnectionError> {
        let TestFixture { repr } = self.as_ref();

        match repr {
            TestDataRepr::Single(data) => {
                let (local, auto) = data.open_conn(url).await?;

                tokio::spawn(async move {
                    if let Err(e) = auto.run().await {
                        panic!("{:?}", e);
                    }
                });

                Ok(local)
            }
            TestDataRepr::Multiple(data) => {
                let data = &mut *(data.lock().await);
                let (local, auto) = data.open_conn(url).await?;

                tokio::spawn(async move {
                    if let Err(e) = auto.run().await {
                        panic!("{:?}", e);
                    }
                });

                Ok(local)
            }
        }
    }
}

enum TestDataRepr {
    Single(TestData),
    Multiple(Mutex<MultipleTestData>),
}

struct MultipleTestData {
    connections: VecDeque<Option<TestData>>,
}

impl MultipleTestData {
    fn new(data: Vec<Option<TestData>>) -> Self {
        MultipleTestData {
            connections: data.into(),
        }
    }

    async fn open_conn(
        &mut self,
        _url: url::Url,
    ) -> Result<(MockWebSocket<FailingExt<ConnectionError>>, AutoWebSocket), ConnectionError> {
        match self.connections.pop_front() {
            Some(Some(data)) => {
                let TestData {
                    inputs,
                    outputs,
                    fail_on,
                } = data;

                let (local, peer) =
                    ratchet_fixture::duplex::websocket_pair(FailingExt(fail_on.clone()), NoExt);
                let auto = AutoWebSocket {
                    inputs: inputs.clone().into(),
                    inner: peer,
                    outputs,
                };

                Ok((local, auto))
            }
            _ => Err(ConnectionError::Capacity(CapacityError::new(
                CapacityErrorKind::Ambiguous,
                None,
            ))),
        }
    }
}

struct AutoWebSocket {
    inputs: VecDeque<String>,
    inner: MockWebSocket<NoExt>,
    outputs: mpsc::Sender<WsMessage>,
}

impl AutoWebSocket {
    async fn run(self) -> Result<(), ConnectionError> {
        let AutoWebSocket {
            mut inputs,
            inner,
            outputs,
        } = self;

        let (mut tx, rx) = inner.split()?;

        let send_task = async move {
            while let Some(msg) = inputs.pop_front() {
                match tx.write_text(msg).await {
                    Ok(()) => continue,
                    Err(e) => return Err(ConnectionError::from(e)),
                }
            }

            Ok(())
        };

        let receive_task = async move {
            let receive_stream = into_stream(rx);
            pin_utils::pin_mut!(receive_stream);

            while let Some(message) = receive_stream.next().await {
                assert!(outputs
                    .send(message.expect("Malformatted message"))
                    .await
                    .is_ok());
            }
        };

        let task_result = join(send_task, receive_task).await;
        assert_eq!(task_result.0, Ok(()));

        Ok(())
    }
}

struct TestData {
    inputs: Vec<String>,
    outputs: mpsc::Sender<WsMessage>,
    fail_on: FailOn<ConnectionError>,
}

impl TestData {
    fn new(inputs: Vec<String>, outputs: mpsc::Sender<WsMessage>) -> Self {
        TestData {
            inputs,
            outputs,
            fail_on: FailOn::Nothing,
        }
    }

    async fn open_conn(
        &self,
        _url: url::Url,
    ) -> Result<(MockWebSocket<FailingExt<ConnectionError>>, AutoWebSocket), ConnectionError> {
        let (local, peer) =
            ratchet_fixture::duplex::websocket_pair(FailingExt(self.fail_on.clone()), NoExt);
        let auto = AutoWebSocket {
            inputs: self.inputs.clone().into(),
            inner: peer,
            outputs: self.outputs.clone(),
        };

        Ok((local, auto))
    }

    fn fail_on_input(self) -> Self {
        let TestData {
            inputs,
            outputs,
            fail_on,
        } = self;

        let fail_on = match fail_on {
            FailOn::Read(on) => FailOn::Rw(on),
            FailOn::Write(on) => FailOn::Write(on),
            FailOn::Rw(on) => FailOn::Rw(on),
            FailOn::Nothing => {
                FailOn::Read(ConnectionError::Protocol(ProtocolError::websocket(None)))
            }
        };

        TestData {
            inputs,
            outputs,
            fail_on,
        }
    }

    fn fail_on_output(self) -> Self {
        let TestData {
            inputs,
            outputs,
            fail_on,
        } = self;

        let fail_on = match fail_on {
            FailOn::Read(on) => FailOn::Read(on),
            FailOn::Write(on) => FailOn::Rw(on),
            FailOn::Rw(on) => FailOn::Rw(on),
            FailOn::Nothing => {
                FailOn::Write(ConnectionError::Protocol(ProtocolError::websocket(None)))
            }
        };

        TestData {
            inputs,
            outputs,
            fail_on,
        }
    }
}
//
// struct EmptyFailingIo<E>(FailOn<E>);
// impl<E> AsyncRead for EmptyFailingIo<E> {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         _cx: &mut Context<'_>,
//         _buf: &mut ReadBuf<'_>,
//     ) -> Poll<std::io::Result<()>> {
//         match &self.0 {
//             FailOn::Read(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.clone()))),
//             FailOn::Rw(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.clone()))),
//             _ => Poll::Ready(Ok(())),
//         }
//     }
// }
//
// impl<E> AsyncWrite for EmptyFailingIo<E> {
//     fn poll_write(
//         self: Pin<&mut Self>,
//         _cx: &mut Context<'_>,
//         _buf: &[u8],
//     ) -> Poll<Result<usize, IoError>> {
//         match &self.0 {
//             FailOn::Read(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.clone()))),
//             FailOn::Rw(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.clone()))),
//             _ => Poll::Ready(Ok(0)),
//         }
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
//         Poll::Ready(Ok(()))
//     }
//
//     fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
//         Poll::Ready(Ok(()))
//     }
// }
