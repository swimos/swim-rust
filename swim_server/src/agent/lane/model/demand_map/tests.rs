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
// use crate::agent::lane::model::demand_map::{make_lane_model, DemandMapLaneEvent};
// use futures::StreamExt;
// use pin_utils::core_reexport::num::NonZeroUsize;
// use std::collections::HashMap;
//
// #[tokio::test]
// async fn test_sync() {
//     let (lane, mut events, _) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap());
//     let mut controller = lane.controller();
//     let _ = controller.sync().await;
//
//     assert!(matches!(
//         events.next().await,
//         Some(DemandMapLaneEvent::Sync(_))
//     ));
// }
//
// #[tokio::test(threaded_scheduler)]
// async fn test_sync_resp() {
//     let (lane, mut events, _) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap());
//     let mut controller = lane.controller();
//
//     let jh = tokio::spawn(async move {
//         while let Some(event) = events.next().await {
//             match event {
//                 DemandMapLaneEvent::Sync(sender) => {
//                     sender.send(vec![1, 2, 3, 4, 5]).unwrap();
//                 }
//                 e => {
//                     panic!("Unexpected event: {:?}", e);
//                 }
//             }
//         }
//     });
//
//     let r = controller.sync().await.await.unwrap();
//
//     assert_eq!(r, vec![1, 2, 3, 4, 5]);
//
//     drop(lane);
//     drop(controller);
//
//     assert!(jh.await.is_ok());
// }
//
// #[tokio::test(threaded_scheduler)]
// async fn test_cue_resp() {
//     let (lane, mut events, _) = make_lane_model(NonZeroUsize::new(5).unwrap());
//     let mut controller = lane.controller();
//
//     let jh = tokio::spawn(async move {
//         let mut map = HashMap::new();
//         map.insert(1, String::from("1"));
//         map.insert(2, String::from("2"));
//         map.insert(3, String::from("3"));
//         map.insert(4, String::from("4"));
//         map.insert(5, String::from("5"));
//
//         while let Some(event) = events.next().await {
//             match event {
//                 DemandMapLaneEvent::Sync(sender) => {
//                     let mut keys: Vec<i32> = map.keys().map(Clone::clone).collect();
//                     keys.sort();
//                     sender.send(keys).unwrap();
//                 }
//                 DemandMapLaneEvent::Cue(sender, key) => {
//                     let entry = map.get(&key).and_then(|e| Some(e.clone()));
//                     sender.send(entry).unwrap();
//                 }
//             }
//         }
//     });
//
//     let r = controller.sync().await.await.unwrap();
//     let keys: Vec<i32> = vec![1, 2, 3, 4, 5];
//
//     assert_eq!(r, keys);
//
//     for k in keys {
//         let value = controller.cue(k).await.await.unwrap();
//         assert_eq!(value, Some(k.to_string()));
//     }
//
//     drop(lane);
//     drop(controller);
//
//     assert!(jh.await.is_ok());
// }
//
// #[tokio::test]
// async fn test_cue() {
//     let (lane, mut events, _) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap());
//     let mut controller = lane.controller();
//     let _ = controller.cue(13).await;
//
//     assert!(matches!(
//         events.next().await,
//         Some(DemandMapLaneEvent::Cue(_, 13))
//     ));
// }
