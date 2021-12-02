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
// use crate::byte_routing::routing::Route;
// use crate::error::{ResolutionError, ResolutionErrorKind};
// use crate::routing::RoutingAddr;
// use std::collections::hash_map::Entry;
// use std::collections::HashMap;
// use std::sync::Arc;
// use swim_utilities::algebra::non_zero_usize;
// use swim_utilities::io::byte_channel::{byte_channel, ByteReader};
// use swim_utilities::routing::uri::RelativeUri;
// use tokio::sync::{mpsc, oneshot};
// use tokio::task::JoinHandle;
// use tokio_stream::wrappers::ReceiverStream;
// use tokio_stream::StreamExt;
// use url::Url;
//
// pub enum RoutingRequest {
//     Resolve {
//         callback: oneshot::Sender<Result<Route, ResolutionError>>,
//         addr: RoutingAddr,
//     },
//     Lookup {
//         callback: oneshot::Sender<Result<RoutingAddr, ResolutionError>>,
//         host: Option<Url>,
//         route: RelativeUri,
//     },
// }
//
// pub struct MockRouter {
//     lookup: HashMap<RelativeUri, RoutingAddr>,
//     resolver: HashMap<RoutingAddr, Readers>,
// }
//
// impl MockRouter {
//     async fn run(self, rx: mpsc::Receiver<RoutingRequest>) {
//         let MockRouter {
//             lookup,
//             mut resolver,
//         } = self;
//         let mut requests = ReceiverStream::new(rx);
//
//         while let Some(request) = requests.next().await {
//             match request {
//                 RoutingRequest::Resolve { callback, addr } => {
//                     let (write, read) = byte_channel(non_zero_usize!(128));
//
//                     match resolver.entry(addr) {
//                         Entry::Occupied(mut readers) => {
//                             readers.get_mut().0.push(read);
//                         }
//                         Entry::Vacant(mut entry) => {
//                             entry.insert(Readers(vec![read]));
//                         }
//                     }
//
//                     let _ = callback.send(Ok(Route { sender: write }));
//                 }
//                 RoutingRequest::Lookup {
//                     callback, route, ..
//                 } => {
//                     let response = match lookup.get(&route) {
//                         Some(addr) => Ok(addr.clone()),
//                         None => Err(ResolutionError::new(
//                             ResolutionErrorKind::Unresolvable,
//                             None,
//                         )),
//                     };
//                     let _ = callback.send(response);
//                 }
//             }
//         }
//
//         unimplemented!()
//     }
// }
//
// pub struct Readers(pub Vec<ByteReader>);
//
// pub struct StubRouter {
//     _jh: Arc<JoinHandle<()>>,
//     tx: mpsc::Sender<RoutingRequest>,
// }
//
// impl StubRouter {
//     pub fn new(
//         lookup: HashMap<RelativeUri, RoutingAddr>,
//         resolver: HashMap<RoutingAddr, Readers>,
//     ) -> StubRouter {
//         let (tx, rx) = mpsc::channel(128);
//         let inner = MockRouter { lookup, resolver };
//
//         StubRouter {
//             _jh: Arc::new(tokio::spawn(inner.run(rx))),
//             tx,
//         }
//     }
// }
