// // Copyright 2015-2020 SWIM.AI inc.
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
// use futures::future::ErrInto as FutErrInto;
// use futures::sink::SinkErrInto;
// use futures::stream::{SplitSink, SplitStream};
// use futures::task::{Context, Poll};
// use futures::{Future, Sink, Stream, StreamExt, TryFutureExt};
// use futures_util::SinkExt;
// use pin_project::*;
// use tokio::sync::{mpsc, oneshot};
// use url::Url;
// use wasm_bindgen::__rt::core::pin::Pin;
// use wasm_bindgen_futures::spawn_local;
// use ws_stream_wasm::*;
//
// use crate::request::request_future::{RequestError, RequestFuture, SendAndAwait, Sequenced};
// use crate::request::Request;
// use crate::wasm::errors::FlattenErrors;
//
// #[derive(Debug)]
// pub enum ConnectionError {
//     ConnectError,
// }
//
// #[pin_project]
// #[derive(Debug)]
// pub struct WasmWsSink {
//     #[pin]
//     inner: SinkErrInto<SplitSink<WsStream, WsMessage>, WsMessage, ConnectionError>,
// }
//
// impl From<RequestError> for ConnectionError {
//     fn from(_: RequestError) -> Self {
//         ConnectionError::ConnectError
//     }
// }
//
// impl Sink<String> for WasmWsSink {
//     type Error = ConnectionError;
//
//     fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.project().inner.poll_ready(cx)
//     }
//
//     fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
//         self.project().inner.start_send(WsMessage::Text(item))
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.project().inner.poll_flush(cx)
//     }
//
//     fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.project().inner.poll_close(cx)
//     }
// }
//
// #[pin_project]
// #[derive(Debug)]
// pub struct WasmWsStream {
//     #[pin]
//     inner: SplitStream<WsStream>,
// }
//
// impl Stream for WasmWsStream {
//     type Item = Result<String, ConnectionError>;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         match self.project().inner.poll_next(cx) {
//             Poll::Pending => Poll::Pending,
//             Poll::Ready(Some(WsMessage::Text(s))) => Poll::Ready(Some(Ok(s))),
//             Poll::Ready(Some(WsMessage::Binary(_))) => unimplemented!(),
//             Poll::Ready(None) => Poll::Ready(None),
//         }
//     }
// }
//
// pub struct WasmWsFactory {
//     pub sender: mpsc::Sender<ConnReq>,
// }
//
// pub struct ConnReq {
//     request: Request<Result<(WasmWsSink, WasmWsStream), ConnectionError>>,
//     url: url::Url,
// }
//
// impl From<WsErr> for ConnectionError {
//     fn from(_: WsErr) -> Self {
//         ConnectionError::ConnectError
//     }
// }
//
// impl WasmWsFactory {
//     pub fn new(buffer_size: usize) -> Self {
//         let (tx, rx) = mpsc::channel(buffer_size);
//         let _task = spawn_local(Self::factory_task(rx));
//
//         WasmWsFactory { sender: tx }
//     }
//
//     async fn factory_task(mut receiver: mpsc::Receiver<ConnReq>) {
//         while let Some(ConnReq { request, url }) = receiver.next().await {
//             let (_ws, wsio) = WsMeta::connect(url, None)
//                 .await
//                 .map_err(|_| ConnectionError::ConnectError)
//                 .unwrap();
//
//             let (sink, stream) = wsio.split();
//             let res = (
//                 WasmWsSink {
//                     inner: sink.sink_err_into(),
//                 },
//                 WasmWsStream { inner: stream },
//             );
//
//             let _ = request.send(Ok(res));
//         }
//     }
// }
//
// pub type ConnectionFuture =
//     SendAndAwait<ConnReq, Result<(WasmWsSink, WasmWsStream), ConnectionError>>;
//
// impl WebsocketFactory for WasmWsFactory {
//     type WsStream = WasmWsStream;
//     type WsSink = WasmWsSink;
//     type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture, ConnectionError>>;
//
//     fn connect(&mut self, url: Url) -> Self::ConnectFut {
//         let (tx, rx) = oneshot::channel();
//         let req = ConnReq {
//             request: Request::new(tx),
//             url,
//         };
//
//         let req_fut = RequestFuture::new(self.sender.clone(), req);
//
//         FlattenErrors::new(TryFutureExt::err_into::<ConnectionError>(Sequenced::new(
//             req_fut, rx,
//         )))
//     }
// }
//
// pub trait WebsocketFactory: Send + Sync {
//     /// Type of the stream of incoming messages.
//     type WsStream: Stream<Item = Result<String, ConnectionError>> + Unpin + Send + 'static;
//
//     /// Type of the sink for outgoing messages.
//     type WsSink: Sink<String> + Unpin + Send + 'static;
//
//     type ConnectFut: Future<Output = Result<(Self::WsSink, Self::WsStream), ConnectionError>>
//         + Send
//         + 'static;
//
//     /// Open a connection to the provided remote URL.
//     fn connect(&mut self, url: url::Url) -> Self::ConnectFut;
// }
//
// pub mod errors {
//     use futures::task::{Context, Poll};
//     use futures::Future;
//     use tokio::macros::support::Pin;
//
//     pub struct FlattenErrors<F> {
//         inner: F,
//     }
//
//     impl<F: Unpin> Unpin for FlattenErrors<F> {}
//
//     impl<F> FlattenErrors<F> {
//         pub fn new(inner: F) -> Self {
//             FlattenErrors { inner }
//         }
//     }
//
//     impl<F, T, E> Future for FlattenErrors<F>
//     where
//         F: Future<Output = Result<Result<T, E>, E>> + Unpin,
//     {
//         type Output = Result<T, E>;
//
//         fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//             let f = &mut self.get_mut().inner;
//             Pin::new(f).poll(cx).map(|r| r.and_then(|r2| r2))
//         }
//     }
// }
