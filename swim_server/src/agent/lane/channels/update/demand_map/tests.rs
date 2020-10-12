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
// use std::num::NonZeroUsize;
//
// use tokio::sync::mpsc;
// use tokio::time::Duration;
//
// use crate::agent::lane::channels::update::demand_map::DemandMapCueHandler;
// use crate::agent::lane::model::demand_map::DemandMapLane;
// use futures::future::join;
// use futures::StreamExt;
//
// #[tokio::test]
// async fn cue() {
//     let buffer_size = NonZeroUsize::new(5).unwrap();
//     let (cue_tx, mut cue_rx) = mpsc::channel(buffer_size.get());
//     let (on_cue_tx, mut on_cue_rx) = mpsc::channel(buffer_size.get());
//     let (lane, stream): (DemandMapLane<i32, String>, _) = DemandMapLane::new(cue_tx, buffer_size);
//
//     let mut lane_controller = lane.controller();
//     let cue_handler = DemandMapCueHandler::new(stream, lane, Duration::from_secs(1));
//     let cue_task = cue_handler.run(on_cue_tx);
//
//     let assertion_task = async move {
//         lane_controller.cue(1).await;
//
//         let cue_request = on_cue_rx.next().await;
//         println!("{:?}", cue_request);
//     };
//
//     let (result, _) = join(cue_task, assertion_task).await;
//     assert!(result.is_ok());
// }
