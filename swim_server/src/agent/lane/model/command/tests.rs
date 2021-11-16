// Copyright 2015-2021 Swim Inc.
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

use crate::agent::lane::model::command::Command;
use futures::StreamExt;
use swim_utilities::algebra::non_zero_usize;

#[tokio::test]
async fn send_command() {
    let n = non_zero_usize!(5);
    let (model, mut events) = super::make_private_lane_model::<i32>(n);
    let mut commander = model.commander();
    commander.command(3).await;
    let event = events.next().await;
    assert!(matches!(event, Some(Command { command: 3, .. })));
}

#[tokio::test]
async fn debug_command_lane() {
    let n = non_zero_usize!(5);
    let (model, _events) = super::make_private_lane_model::<i32>(n);
    assert_eq!(format!("{:?}", model), "CommandLane(fn(i32) -> i32)");
}
