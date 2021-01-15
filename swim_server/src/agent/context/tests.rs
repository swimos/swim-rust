// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::context::ContextImpl;
use crate::agent::meta::make_test_meta_context;
use crate::agent::tests::test_clock::TestClock;
use crate::agent::AgentContext;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use swim_runtime::task;
use swim_runtime::time::clock::Clock;
use tokio::sync::mpsc;
use tokio::time::Duration;
use utilities::sync::trigger;

#[test]
fn simple_accessors() {
    let (tx, _rx) = mpsc::channel(1);
    let (_close, close_sig) = trigger::trigger();
    let agent = Arc::new("agent");
    let context = ContextImpl::new(
        agent.clone(),
        "/node".parse().unwrap(),
        tx,
        TestClock::default(),
        close_sig.clone(),
        (),
        HashMap::new(),
        make_test_meta_context("/node".parse().unwrap()),
    );
    assert!(std::ptr::eq(context.agent(), agent.as_ref()));
    assert_eq!(context.node_uri(), "/node");
    assert!(trigger::Receiver::same_receiver(
        &close_sig,
        &context.agent_stop_event()
    ));
}

fn create_context(
    n: usize,
    clock: TestClock,
    close_trigger: trigger::Receiver,
) -> ContextImpl<&'static str, impl Clock, ()> {
    let (tx, rx) = mpsc::channel(n);

    //Run any tasks that get scheduled.
    task::spawn(async move { rx.for_each(|eff| eff).await });

    let agent = Arc::new("agent");
    ContextImpl::new(
        agent.clone(),
        "/node".parse().unwrap(),
        tx,
        clock,
        close_trigger,
        (),
        HashMap::new(),
        make_test_meta_context("/node".parse().unwrap()),
    )
}

#[tokio::test]
async fn send_single_to_scheduler() {
    let (_close, close_sig) = trigger::trigger();
    let clock = TestClock::default();
    let context = create_context(1, clock.clone(), close_sig);

    let (defer_tx, mut defer_rx) = mpsc::channel(5);
    context
        .defer(
            async move {
                let _ = defer_tx.send(6).await;
            },
            Duration::from_millis(50),
        )
        .await;

    clock.advance_when_blocked(Duration::from_millis(50)).await;

    let result = defer_rx.recv().await;

    assert_eq!(result, Some(6));
}

#[tokio::test]
async fn send_multiple_to_scheduler() {
    let (close, close_sig) = trigger::trigger();
    let clock = TestClock::default();
    let context = create_context(1, clock.clone(), close_sig);

    let (defer_tx, mut defer_rx) = mpsc::channel(1);
    let mut i = 0;
    context
        .periodically(
            move || {
                let tx = defer_tx.clone();
                i += 1;
                let c = i;
                async move {
                    let _ = tx.send(c).await;
                }
            },
            Duration::from_millis(50),
            Some(3),
        )
        .await;

    clock.advance_when_blocked(Duration::from_millis(50)).await;
    let result = defer_rx.recv().await;
    assert_eq!(result, Some(1));

    clock.advance_when_blocked(Duration::from_millis(50)).await;
    let result = defer_rx.recv().await;
    assert_eq!(result, Some(2));

    clock.advance_when_blocked(Duration::from_millis(50)).await;
    let result = defer_rx.recv().await;
    assert_eq!(result, Some(3));

    close.trigger();
    let result = defer_rx.recv().await;
    assert!(result.is_none());
}
