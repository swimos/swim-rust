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
// use either::Either;
// use futures::future::BoxFuture;
// use futures::select_biased;
// use futures::stream::FuturesOrdered;
// use futures::{FutureExt, Stream, StreamExt};
// use pin_utils::pin_mut;
// use swim_runtime::time::timeout;
// use tokio::sync::mpsc;
// use tokio::time::Duration;
// use tracing::{event, Level};
//
// use crate::agent::lane::channels::update::UpdateError;
// use crate::agent::lane::model::demand_map::DemandMapLaneController;
//
// #[cfg(test)]
// mod tests;
//
// const NO_COMPLETION: &str = "On Cue did not complete.";
// const CLEANUP_TIMEOUT: &str = "Timeout waiting for pending completions.";
//
// pub struct DemandMapCueHandler<S, Key, Value> {
//     controller: DemandMapLaneController<Key, Value>,
//     cue_requests: S,
//     cleanup_timeout: Duration,
// }
//
// impl<S, Key, Value> DemandMapCueHandler<S, Key, Value>
// where
//     Key: Send + Sync + 'static,
//     Value: Send + Sync + 'static,
//     S: Stream<Item = Value> + Send + Sync + 'static,
// {
//     pub fn new(
//         controller: DemandMapLaneController<Key, Value>,
//         cue_requests: S,
//
//         cleanup_timeout: Duration,
//     ) -> DemandMapCueHandler<S, Key, Value> {
//         DemandMapCueHandler {
//             controller,
//             cue_requests,
//             cleanup_timeout,
//         }
//     }
//
//     pub fn run(
//         self,
//         mut cue_tx: mpsc::Sender<Value>,
//     ) -> BoxFuture<'static, Result<(), UpdateError>> {
//         async move {
//             let DemandMapCueHandler {
//                 controller,
//                 cue_requests,
//                 cleanup_timeout,
//             } = self;
//
//             let cue_requests = cue_requests.fuse();
//
//             pin_mut!(cue_requests);
//
//             let mut values = FuturesOrdered::new();
//             let result: Result<(), UpdateError> = loop {
//                 let req_or_value = if values.is_empty() {
//                     cue_requests.next().await.map(Either::Right)
//                 } else {
//                     select_biased! {
//                         value = values.next().fuse() => value.map(Either::Left),
//                         request = cue_requests.next() => request.map(Either::Right),
//                     }
//                 };
//
//                 match req_or_value {
//                     Some(Either::Left(Ok(None))) => {}
//                     Some(Either::Left(Ok(Some(value)))) => {
//                         if cue_tx.send(value).await.is_err() {
//                             break Err(UpdateError::FeedbackChannelDropped);
//                         }
//                     }
//                     Some(Either::Right(request)) => {
//                         let request = controller.cue(request.0).await;
//                         values.push(request);
//                     }
//                     _ => break Ok(()),
//                 }
//             };
//
//             match result {
//                 e @ Err(UpdateError::FeedbackChannelDropped) => e,
//                 ow => {
//                     loop {
//                         let resp = timeout::timeout(cleanup_timeout, values.next()).await;
//                         match resp {
//                             Ok(Some(Ok(Some(value)))) => {
//                                 if cue_tx.send(value).await.is_err() {
//                                     return Err(UpdateError::FeedbackChannelDropped);
//                                 }
//                             }
//                             Ok(Some(Err(_))) => {
//                                 event!(Level::WARN, NO_COMPLETION);
//                             }
//                             Ok(_) => {
//                                 break;
//                             }
//                             Err(_) => {
//                                 event!(Level::ERROR, CLEANUP_TIMEOUT);
//                                 break;
//                             }
//                         }
//                     }
//                     ow
//                 }
//             }
//         }
//         .boxed()
//     }
// }
