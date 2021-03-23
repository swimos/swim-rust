// // Copyright 2015-2021 SWIM.AI inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
// use futures::task::{Context, Poll};
// use futures::Sink;
// use futures_util::stream::Stream;
// use std::pin::Pin;
// use std::sync::atomic::{AtomicUsize, Ordering};
// use std::sync::Arc;
// use tokio::sync::mpsc;
// use url::Url;
//
// use super::*;
// use swim_common::routing::ws::Protocol;
// use swim_common::routing::ws::{ConnFuture, WebsocketFactory};
// use swim_common::routing::{
//     CapacityError, CapacityErrorKind, CloseErrorKind, ProtocolError, ProtocolErrorKind,
// };
// use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
//
// #[tokio::test]
// async fn test_connection_pool_send_single_message_single_connection() {
//     // Given
//     let buffer_size = 5;
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let text = "Hello";
//
//     let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(vec![], writer_tx);
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     let (mut connection_sender, _connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // When
//     connection_sender
//         .send_message(text.to_string().into())
//         .await
//         .unwrap();
//
//     // Then
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("Hello".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_send_multiple_messages_single_connection() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let first_text = "First_Text";
//     let second_text = "Second_Text";
//
//     let (writer_tx, mut writer_rx) = mpsc::channel(5);
//
//     let test_data = TestData::new(vec![], writer_tx);
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     let (mut first_connection_sender, _first_connection_receiver) = connection_pool
//         .request_connection(host_url.clone(), false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let (mut second_connection_sender, _second_connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // When
//     first_connection_sender
//         .send_message(first_text.to_string().into())
//         .await
//         .unwrap();
//     second_connection_sender
//         .send_message(second_text.to_string().into())
//         .await
//         .unwrap();
//
//     // Then
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("First_Text".to_string())
//     );
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("Second_Text".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_send_multiple_messages_multiple_connections() {
//     // Given
//     let first_host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_host_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//     let third_host_url = url::Url::parse("ws://127.0.0.3//").unwrap();
//     let first_text = "First_Text";
//     let second_text = "Second_Text";
//     let third_text = "Third_Text";
//
//     let (first_writer_tx, mut first_writer_rx) = mpsc::channel(5);
//
//     let (second_writer_tx, mut second_writer_rx) = mpsc::channel(5);
//
//     let (third_writer_tx, mut third_writer_rx) = mpsc::channel(5);
//
//     let test_data = vec![
//         TestData::new(vec![], first_writer_tx),
//         TestData::new(vec![], second_writer_tx),
//         TestData::new(vec![], third_writer_tx),
//     ];
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new_multiple(test_data).await,
//     );
//
//     let (mut first_connection_sender, _first_connection_receiver) = connection_pool
//         .request_connection(first_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let (mut second_connection_sender, _second_connection_receiver) = connection_pool
//         .request_connection(second_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let (mut third_connection_sender, _third_connection_receiver) = connection_pool
//         .request_connection(third_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // When
//     first_connection_sender
//         .send_message(first_text.to_string().into())
//         .await
//         .unwrap();
//     second_connection_sender
//         .send_message(second_text.to_string().into())
//         .await
//         .unwrap();
//     third_connection_sender
//         .send_message(third_text.to_string().into())
//         .await
//         .unwrap();
//
//     // Then
//     assert_eq!(
//         first_writer_rx.recv().await.unwrap(),
//         WsMessage::Text("First_Text".to_string())
//     );
//     assert_eq!(
//         second_writer_rx.recv().await.unwrap(),
//         WsMessage::Text("Second_Text".to_string())
//     );
//     assert_eq!(
//         third_writer_rx.recv().await.unwrap(),
//         WsMessage::Text("Third_Text".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_receive_single_message_single_connection() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let mut items = Vec::new();
//     items.push("new_message".to_string().into());
//     let (writer_tx, _writer_rx) = mpsc::channel(5);
//
//     let test_data = TestData::new(items, writer_tx);
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     // When
//     let (_connection_sender, connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // Then
//     let pool_message = connection_receiver.unwrap().recv().await.unwrap();
//     assert_eq!(pool_message, WsMessage::Text("new_message".to_string()));
// }
//
// #[tokio::test]
// async fn test_connection_pool_receive_multiple_messages_single_connection() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let mut items = Vec::new();
//     items.push("first_message".to_string().into());
//     items.push("second_message".to_string().into());
//     items.push("third_message".to_string().into());
//     let (writer_tx, _writer_rx) = mpsc::channel(5);
//
//     let test_data = TestData::new(items, writer_tx);
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     // When
//     let (_connection_sender, connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let mut connection_receiver = connection_receiver.unwrap();
//
//     // Then
//     let first_pool_message = connection_receiver.recv().await.unwrap();
//     let second_pool_message = connection_receiver.recv().await.unwrap();
//     let third_pool_message = connection_receiver.recv().await.unwrap();
//
//     assert_eq!(
//         first_pool_message,
//         WsMessage::Text("first_message".to_string())
//     );
//     assert_eq!(
//         second_pool_message,
//         WsMessage::Text("second_message".to_string())
//     );
//     assert_eq!(
//         third_pool_message,
//         WsMessage::Text("third_message".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_receive_multiple_messages_multiple_connections() {
//     // Given
//     let mut first_items = Vec::new();
//     let mut second_items = Vec::new();
//     let mut third_items = Vec::new();
//
//     first_items.push("first_message".to_string().into());
//     second_items.push("second_message".to_string().into());
//     third_items.push("third_message".to_string().into());
//
//     let first_host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_host_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//     let third_host_url = url::Url::parse("ws://127.0.0.3//").unwrap();
//
//     let (first_writer_tx, _first_writer_rx) = mpsc::channel(5);
//     let (second_writer_tx, _second_writer_rx) = mpsc::channel(5);
//     let (third_writer_tx, _third_writer_rx) = mpsc::channel(5);
//
//     let test_data = vec![
//         TestData::new(first_items, first_writer_tx),
//         TestData::new(second_items, second_writer_tx),
//         TestData::new(third_items, third_writer_tx),
//     ];
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new_multiple(test_data).await,
//     );
//
//     // When
//     let (_first_sender, mut first_receiver) = connection_pool
//         .request_connection(first_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let (_second_sender, mut second_receiver) = connection_pool
//         .request_connection(second_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     let (_third_sender, mut third_receiver) = connection_pool
//         .request_connection(third_host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // Then
//     let first_pool_message = first_receiver.take().unwrap().recv().await.unwrap();
//     let second_pool_message = second_receiver.take().unwrap().recv().await.unwrap();
//     let third_pool_message = third_receiver.take().unwrap().recv().await.unwrap();
//
//     assert_eq!(
//         first_pool_message,
//         WsMessage::Text("first_message".to_string())
//     );
//     assert_eq!(
//         second_pool_message,
//         WsMessage::Text("second_message".to_string())
//     );
//     assert_eq!(
//         third_pool_message,
//         WsMessage::Text("third_message".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_send_and_receive_messages() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let mut items = Vec::new();
//     items.push("recv_baz".to_string().into());
//     let (writer_tx, mut writer_rx) = mpsc::channel(5);
//
//     let test_data = TestData::new(items, writer_tx);
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     let (mut connection_sender, connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     // When
//     connection_sender
//         .send_message("send_bar".to_string().into())
//         .await
//         .unwrap();
//     // Then
//     let pool_message = connection_receiver.unwrap().recv().await.unwrap();
//
//     assert_eq!(pool_message, WsMessage::Text("recv_baz".to_string()));
//
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("send_bar".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_connection_error() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new_multiple(vec![]).await,
//     );
//
//     // When
//
//     let connection = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap();
//
//     // Then
//     assert!(connection.is_err());
// }
//
// #[tokio::test]
// async fn test_connection_pool_connection_error_send_message() {
//     // Given
//     let host_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let text = "Test_message";
//     let (writer_tx, mut writer_rx) = mpsc::channel(5);
//
//     let test_data = vec![None, Some(TestData::new(vec![], writer_tx))];
//
//     let mut connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new_multiple_with_errs(test_data).await,
//     );
//
//     // When
//     let first_connection = connection_pool
//         .request_connection(host_url.clone(), false)
//         .await
//         .unwrap();
//
//     let (mut second_connection_sender, _second_connection_receiver) = connection_pool
//         .request_connection(host_url, false)
//         .await
//         .unwrap()
//         .unwrap();
//
//     second_connection_sender
//         .send_message(text.to_string().into())
//         .await
//         .unwrap();
//
//     // Then
//     assert!(first_connection.is_err());
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("Test_message".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_pool_close() {
//     // Given
//     let (writer_tx, mut writer_rx) = mpsc::channel(5);
//
//     let test_data = TestData::new(vec![], writer_tx);
//
//     let connection_pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TestConnectionFactory::new(test_data).await,
//     );
//
//     // When
//     assert!(connection_pool.close().await.is_ok());
//
//     // Then
//     assert!(writer_rx.recv().await.is_none());
// }
//
// #[tokio::test]
// async fn test_connection_send_single_message() {
//     // Given
//     let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
//     let buffer_size = 5;
//     let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(vec![], writer_tx);
//
//     let mut factory = TestConnectionFactory::new(test_data).await;
//
//     let connection = ClientConnection::new(host, buffer_size, &mut factory)
//         .await
//         .unwrap();
//
//     // When
//     connection.tx.send("foo".to_string().into()).await.unwrap();
//     // Then
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("foo".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_send_multiple_messages() {
//     // Given
//     let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
//     let buffer_size = 5;
//     let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(vec![], writer_tx);
//
//     let mut factory = TestConnectionFactory::new(test_data).await;
//
//     let mut connection = ClientConnection::new(host, buffer_size, &mut factory)
//         .await
//         .unwrap();
//
//     let connection_sender = &mut connection.tx;
//     // When
//     connection_sender
//         .send("foo".to_string().into())
//         .await
//         .unwrap();
//     connection_sender
//         .send("bar".to_string().into())
//         .await
//         .unwrap();
//     connection_sender
//         .send("baz".to_string().into())
//         .await
//         .unwrap();
//     // Then
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("foo".to_string())
//     );
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("bar".to_string())
//     );
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("baz".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_send_and_receive_messages() {
//     // Given
//     let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
//     let buffer_size = 5;
//     let mut items = Vec::new();
//     items.push("message_received".to_string().into());
//     let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(items, writer_tx);
//
//     let mut factory = TestConnectionFactory::new(test_data).await;
//
//     let mut connection = ClientConnection::new(host, buffer_size, &mut factory)
//         .await
//         .unwrap();
//
//     // When
//     connection
//         .tx
//         .send("message_sent".to_string().into())
//         .await
//         .unwrap();
//     // Then
//     let pool_message = connection.rx.take().unwrap().recv().await.unwrap();
//     assert_eq!(
//         pool_message,
//         WsMessage::Text("message_received".to_string())
//     );
//
//     assert_eq!(
//         writer_rx.recv().await.unwrap(),
//         WsMessage::Text("message_sent".to_string())
//     );
// }
//
// #[tokio::test]
// async fn test_connection_receive_message_error() {
//     // Given
//     let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
//     let buffer_size = 5;
//
//     let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(vec![], writer_tx).fail_on_input();
//
//     let mut factory = TestConnectionFactory::new(test_data).await;
//
//     let connection = ClientConnection::new(host, buffer_size, &mut factory)
//         .await
//         .unwrap();
//     // When
//     let result = connection._receive_handle.await.unwrap();
//     // Then
//     assert!(result.is_err());
//     assert_eq!(
//         result.err().unwrap(),
//         ConnectionError::Closed(CloseError::unexpected())
//     );
// }
//
// #[tokio::test]
// async fn test_new_connection_send_message_error() {
//     // Given
//     let host = url::Url::parse("ws://127.0.0.1:9999/").unwrap();
//     let buffer_size = 5;
//
//     let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);
//
//     let test_data = TestData::new(vec![], writer_tx).fail_on_output();
//
//     let mut factory = TestConnectionFactory::new(test_data).await;
//
//     let connection = ClientConnection::new(host, buffer_size, &mut factory)
//         .await
//         .unwrap();
//     // When
//     let result = connection._send_handle.await.unwrap();
//     // Then
//     assert!(result.is_err());
//     assert_eq!(
//         result.err().unwrap(),
//         ConnectionError::Closed(CloseError::unexpected())
//     );
// }
//
// #[derive(Clone)]
// struct TestReadStream {
//     items: Vec<WsMessage>,
//     error: bool,
// }
//
// impl Stream for TestReadStream {
//     type Item = Result<WsMessage, ConnectionError>;
//
//     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         if self.error {
//             Poll::Ready(Some(Err(ConnectionError::Protocol(ProtocolError::new(
//                 ProtocolErrorKind::WebSocket,
//                 None,
//             )))))
//         } else {
//             let message = self.items.drain(0..1).next();
//             Poll::Ready(Some(message.ok_or(ConnectionError::Closed(
//                 CloseError::new(CloseErrorKind::Normal, None),
//             ))))
//         }
//     }
// }
//
// #[derive(Clone)]
// struct TestWriteStream {
//     tx: mpsc::Sender<WsMessage>,
//     error: bool,
// }
//
// impl Sink<WsMessage> for TestWriteStream {
//     type Error = ();
//
//     fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         if self.error {
//             Poll::Ready(Err(()))
//         } else {
//             Poll::Ready(Ok(()))
//         }
//     }
//
//     fn start_send(self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
//         if self.error {
//             Err(())
//         } else {
//             self.tx.try_send(item).unwrap();
//             Ok(())
//         }
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         if self.error {
//             Poll::Ready(Err(()))
//         } else {
//             Poll::Ready(Ok(()))
//         }
//     }
//
//     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         if self.error {
//             Poll::Ready(Err(()))
//         } else {
//             Poll::Ready(Ok(()))
//         }
//     }
// }
//
// struct TestConnectionFactory {
//     inner: AsyncFactory<TestWriteStream, TestReadStream>,
// }
//
// impl TestConnectionFactory {
//     async fn new(test_data: TestData) -> Self {
//         let shared_data = Arc::new(test_data);
//         let inner = AsyncFactory::new(5, move |url, _config| {
//             let shared_data = shared_data.clone();
//             async { shared_data.open_conn(url).await }
//         })
//         .await;
//         TestConnectionFactory { inner }
//     }
//
//     async fn new_multiple(test_data: Vec<TestData>) -> Self {
//         TestConnectionFactory::new_multiple_with_errs(test_data.into_iter().map(Some).collect())
//             .await
//     }
//
//     async fn new_multiple_with_errs(test_data: Vec<Option<TestData>>) -> Self {
//         let shared_data = Arc::new(MultipleTestData::new(test_data));
//         let inner = AsyncFactory::new(5, move |url, _config| {
//             let shared_data = shared_data.clone();
//             async { shared_data.open_conn(url).await }
//         })
//         .await;
//         TestConnectionFactory { inner }
//     }
// }
//
// impl WebsocketFactory for TestConnectionFactory {
//     type WsStream = TestReadStream;
//     type WsSink = TestWriteStream;
//
//     fn connect(&mut self, url: Url) -> ConnFuture<Self::WsSink, Self::WsStream> {
//         self.inner
//             .connect_using(
//                 url,
//                 HostConfig {
//                     protocol: Protocol::PlainText,
//                     compression_level: WsCompression::None(None),
//                 },
//             )
//             .boxed()
//     }
// }
//
// struct TestData {
//     inputs: Vec<WsMessage>,
//     outputs: mpsc::Sender<WsMessage>,
//     input_error: bool,
//     output_error: bool,
// }
//
// struct MultipleTestData {
//     connections: Vec<Option<TestData>>,
//     n: AtomicUsize,
// }
//
// impl MultipleTestData {
//     fn new(data: Vec<Option<TestData>>) -> Self {
//         MultipleTestData {
//             connections: data,
//             n: AtomicUsize::new(0),
//         }
//     }
//
//     async fn open_conn(
//         self: Arc<Self>,
//         _url: url::Url,
//     ) -> Result<(TestWriteStream, TestReadStream), ConnectionError> {
//         let i = self.n.fetch_add(1, Ordering::AcqRel);
//         if i >= self.connections.len() {
//             Err(ConnectionError::Capacity(CapacityError::new(
//                 CapacityErrorKind::Ambiguous,
//                 None,
//             )))
//         } else {
//             let maybe_conn = &self.connections[i];
//             match maybe_conn {
//                 Some(conn) => {
//                     let data = conn.inputs.clone();
//                     let sender = conn.outputs.clone();
//                     let output = TestWriteStream {
//                         tx: sender,
//                         error: conn.output_error,
//                     };
//                     let input = TestReadStream {
//                         items: data,
//                         error: conn.input_error,
//                     };
//                     Ok((output, input))
//                 }
//                 _ => Err(ConnectionError::Capacity(CapacityError::new(
//                     CapacityErrorKind::Ambiguous,
//                     None,
//                 ))),
//             }
//         }
//     }
// }
//
// impl TestData {
//     fn new(inputs: Vec<WsMessage>, outputs: mpsc::Sender<WsMessage>) -> Self {
//         TestData {
//             inputs,
//             outputs,
//             input_error: false,
//             output_error: false,
//         }
//     }
//
//     async fn open_conn(
//         self: Arc<Self>,
//         _url: url::Url,
//     ) -> Result<(TestWriteStream, TestReadStream), ConnectionError> {
//         let data = self.inputs.clone();
//         let sender = self.outputs.clone();
//         let output = TestWriteStream {
//             tx: sender,
//             error: self.output_error,
//         };
//         let input = TestReadStream {
//             items: data,
//             error: self.input_error,
//         };
//         Ok((output, input))
//     }
//
//     fn fail_on_input(mut self) -> Self {
//         self.input_error = true;
//         self
//     }
//
//     fn fail_on_output(mut self) -> Self {
//         self.output_error = true;
//         self
//     }
// }
