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
// use crate::agent::lane::model::supply::{make_lane_model, Queue};
// use crate::agent::meta::metric::uplink::{
//     TaggedWarpUplinkProfile, UplinkCollectorTask, WarpUplinkProfile, WarpUplinkPulse,
// };
// use futures::future::join;
// use futures::{FutureExt, StreamExt};
// use std::collections::HashMap;
// use std::num::NonZeroUsize;
// use swim_common::warp::path::RelativePath;
// use tokio::sync::mpsc;
// use utilities::sync::trigger;
//
// #[tokio::test]
// async fn test_queue() {
//     let (supply_lane, mut supply_rx) = make_lane_model(Queue(NonZeroUsize::new(4).unwrap()));
//
//     let (metric_tx, metric_rx) = mpsc::channel(5);
//     let (lane_tx, _lane_rx) = mpsc::channel(5);
//
//     let (trigger_tx, trigger_rx) = trigger::trigger();
//     let ident = RelativePath::new("/node", "/lane");
//
//     let mut lanes = HashMap::new();
//     lanes.insert(ident, supply_lane);
//
//     let task = UplinkCollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lane_tx, lanes);
//
//     let send_task = async move {
//         let profile1 = WarpUplinkProfile::new(1, 1, 1, 1, 1, 1, 0, 0);
//         let profile2 = WarpUplinkProfile::new(2, 2, 2, 2, 2, 2, 0, 0);
//         let profile3 = WarpUplinkProfile::new(3, 3, 3, 3, 3, 3, 0, 0);
//
//         let profiles = vec![profile1, profile2, profile3];
//
//         for profile in profiles {
//             let profile =
//                 TaggedWarpUplinkProfile::tag(RelativePath::new("/node", "/lane"), profile);
//             assert!(metric_tx.send(profile).await.is_ok());
//         }
//
//         for i in 1..=3 {
//             match supply_rx.next().await {
//                 Some(pulse) => {
//                     let expected =
//                         WarpUplinkPulse::new(i, i as u64, i as u64, i, i as u64, i as u64);
//                     assert_eq!(expected, pulse);
//                 }
//                 None => {
//                     panic!("Expected an uplink profile")
//                 }
//             }
//         }
//
//         match supply_rx.next().now_or_never() {
//             Some(_) => {
//                 panic!("Unexpected uplink profile")
//             }
//             None => trigger_tx.trigger(),
//         }
//     };
//
//     let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
// }
//
// #[tokio::test]
// async fn test_dropping() {
//     let (supply_lane, mut supply_rx) = make_lane_model(Queue(NonZeroUsize::new(4).unwrap()));
//
//     let (metric_tx, metric_rx) = mpsc::channel(5);
//     let (lane_tx, _lane_rx) = mpsc::channel(5);
//
//     let (trigger_tx, trigger_rx) = trigger::trigger();
//     let ident = RelativePath::new("/node", "/lane");
//
//     let mut lanes = HashMap::new();
//     lanes.insert(ident, supply_lane);
//
//     let task = UplinkCollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lane_tx, lanes);
//
//     let send_task = async move {
//         for i in 0..100 {
//             let mut profile = WarpUplinkProfile::default();
//             profile.event_count = i;
//             let profile =
//                 TaggedWarpUplinkProfile::tag(RelativePath::new("/node", "/lane"), profile);
//
//             assert!(metric_tx.send(profile).await.is_ok());
//         }
//
//         match supply_rx.next().await {
//             Some(pulse) => {
//                 let expected = WarpUplinkPulse {
//                     event_count: 99,
//                     ..Default::default()
//                 };
//
//                 assert_eq!(pulse, expected);
//                 assert!(trigger_tx.trigger());
//             }
//             None => {
//                 panic!("Expected an uplink profile")
//             }
//         }
//     };
//
//     let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
// }
//
// #[tokio::test]
// async fn test_unknown() {
//     let (supply_lane, mut supply_rx) = make_lane_model(Queue(NonZeroUsize::new(4).unwrap()));
//
//     let (metric_tx, metric_rx) = mpsc::channel(5);
//     let (lane_tx, _lane_rx) = mpsc::channel(5);
//
//     let (trigger_tx, trigger_rx) = trigger::trigger();
//     let ident = RelativePath::new("/node", "/lane");
//
//     let mut lanes = HashMap::new();
//     lanes.insert(ident, supply_lane);
//
//     let task = UplinkCollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lane_tx, lanes);
//
//     let send_task = async move {
//         let tagged = TaggedWarpUplinkProfile::tag(
//             RelativePath::new("/node", "/lane"),
//             WarpUplinkProfile::default(),
//         );
//
//         assert!(metric_tx.send(tagged).await.is_ok());
//
//         match supply_rx.next().now_or_never() {
//             Some(_) => {
//                 panic!("Unexpected uplink profile")
//             }
//             None => trigger_tx.trigger(),
//         }
//     };
//
//     let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
// }
