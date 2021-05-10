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

use crate::agent::lane::channels::update::action::ActionLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::action::{Action, ActionLane};
use futures::future::join;
use std::time::Duration;
use swim_common::form::FormErr;
use swim_common::routing::RoutingAddr;
use swim_runtime::time::timeout::timeout;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::test]
async fn cmd_no_feedback() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);

    let lane: ActionLane<i32, ()> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, None, Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr = RoutingAddr::remote(5);

    let assertion_task = async move {
        assert!(msg_tx.send(Ok((addr, 13))).await.is_ok());
        let maybe_action = act_rx.recv().await;
        assert!(maybe_action.is_some());
        let action = maybe_action.unwrap();
        assert_eq!(action.command, 13);
        assert!(action.responder.is_none());

        drop(msg_tx);
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn failure_no_feedback() {
    let (act_tx, _act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);

    let lane: ActionLane<i32, ()> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, None, Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let _msg_tx_cpy = msg_tx.clone();

    let assertion_task = async move {
        assert!(msg_tx
            .send(Err(UpdateError::BadEnvelopeBody(FormErr::Malformatted)))
            .await
            .is_ok());
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(matches!(
        result,
        Err(UpdateError::BadEnvelopeBody(FormErr::Malformatted))
    ));
}

async fn check_receive(act_rx: &mut mpsc::Receiver<Action<i32, i32>>, expect: i32, feedback: i32) {
    let maybe_action = act_rx.recv().await;
    assert!(maybe_action.is_some());
    let Action { command, responder } = maybe_action.unwrap();
    assert_eq!(command, expect);
    match responder {
        Some(responder) => {
            assert!(responder.send(feedback).is_ok());
        }
        _ => {
            panic!("Responder not provided.");
        }
    }
}

async fn check_feedback(
    feedback_rx: &mut mpsc::Receiver<(RoutingAddr, i32)>,
    expected_addr: RoutingAddr,
    expected_value: i32,
) {
    let maybe_fed_back = feedback_rx.recv().await;
    assert!(maybe_fed_back.is_some());
    let (fed_back_addr, fed_back_value) = maybe_fed_back.unwrap();
    assert_eq!(fed_back_addr, expected_addr);
    assert_eq!(fed_back_value, expected_value);
}

#[tokio::test]
async fn cmd_with_feedback() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);
    let (feedback_tx, mut feedback_rx) = mpsc::channel(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, Some(feedback_tx), Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr = RoutingAddr::remote(5);

    let assertion_task = async move {
        assert!(msg_tx.send(Ok((addr, 13))).await.is_ok());

        check_receive(&mut act_rx, 13, -4).await;
        check_feedback(&mut feedback_rx, addr, -4).await;

        drop(msg_tx);
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn multiple_cmd_with_feedback() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);
    let (feedback_tx, mut feedback_rx) = mpsc::channel(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, Some(feedback_tx), Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(6);

    let assertion_task = async move {
        assert!(msg_tx.send(Ok((addr1, 13))).await.is_ok());
        assert!(msg_tx.send(Ok((addr2, 42))).await.is_ok());

        check_receive(&mut act_rx, 13, -4).await;
        check_feedback(&mut feedback_rx, addr1, -4).await;
        check_receive(&mut act_rx, 42, 56).await;
        check_feedback(&mut feedback_rx, addr2, 56).await;

        drop(msg_tx);
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn multiple_cmd_with_out_of_order_feedback() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);
    let (feedback_tx, mut feedback_rx) = mpsc::channel(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, Some(feedback_tx), Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(6);

    let assertion_task = async move {
        assert!(msg_tx.send(Ok((addr1, 13))).await.is_ok());
        assert!(msg_tx.send(Ok((addr2, 42))).await.is_ok());

        let Action {
            responder: first, ..
        } = act_rx.recv().await.unwrap();
        let Action {
            responder: second, ..
        } = act_rx.recv().await.unwrap();

        assert!(second.unwrap().send(2).is_ok());
        assert!(first.unwrap().send(1).is_ok());
        check_feedback(&mut feedback_rx, addr1, 1).await;
        check_feedback(&mut feedback_rx, addr2, 2).await;

        drop(msg_tx);
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn cleanup_on_error() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);
    let (feedback_tx, mut feedback_rx) = mpsc::channel(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, Some(feedback_tx), Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(6);

    let _msg_tx_cpy = msg_tx.clone();

    let assertion_task = async move {
        assert!(msg_tx.send(Ok((addr1, 13))).await.is_ok());
        assert!(msg_tx.send(Ok((addr2, 42))).await.is_ok());

        check_receive(&mut act_rx, 13, -4).await;
        check_feedback(&mut feedback_rx, addr1, -4).await;

        assert!(msg_tx
            .send(Err(UpdateError::BadEnvelopeBody(FormErr::Malformatted)))
            .await
            .is_ok());

        check_receive(&mut act_rx, 42, 56).await;
        check_feedback(&mut feedback_rx, addr2, 56).await;
    };

    let (result, _) = join(timeout(Duration::from_secs(5), update_task), assertion_task).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn fail_on_feedback_dropped() {
    let (act_tx, mut act_rx) = mpsc::channel(5);
    let (msg_tx, msg_rx) = mpsc::channel::<Result<(RoutingAddr, i32), UpdateError>>(5);
    let (feedback_tx, feedback_rx) = mpsc::channel(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(act_tx);

    let lane_task = ActionLaneUpdateTask::new(lane, Some(feedback_tx), Duration::from_secs(1));

    let update_task = lane_task.run_update(ReceiverStream::new(msg_rx));

    let addr1 = RoutingAddr::remote(5);

    let _msg_tx_cpy = msg_tx.clone();

    let assertion_task = async move {
        drop(feedback_rx);

        assert!(msg_tx.send(Ok((addr1, 13))).await.is_ok());

        check_receive(&mut act_rx, 13, -4).await;
    };

    let (result, _) = join(update_task, assertion_task).await;
    assert!(matches!(result, Err(UpdateError::FeedbackChannelDropped)));
}
