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

use crate::agent::context::{ContextImpl, RoutingContext, SchedulerContext};
use crate::agent::tests::test_clock::TestClock;
use crate::agent::AgentContext;
use crate::meta::meta_context_sink;
use server_store::agent::mock::MockNodeStore;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::collections::HashMap;
use std::sync::Arc;
use swim_runtime::task;
use swim_runtime::time::clock::Clock;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::Duration;

#[test]
fn simple_accessors() {
    let (tx, _rx) = mpsc::channel(1);
    let (_close, close_sig) = trigger::trigger();
    let agent = Arc::new("agent");
    let routing_context = RoutingContext::new("/node".parse().unwrap(), (), HashMap::new());
    let schedule_context = SchedulerContext::new(tx, TestClock::default(), close_sig.clone());
    let context = ContextImpl::new(
        agent.clone(),
        routing_context,
        schedule_context,
        meta_context_sink(),
        MockNodeStore::mock(),
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
) -> ContextImpl<&'static str, impl Clock, (), SwimNodeStore<MockPlaneStore>> {
    let (tx, mut rx) = mpsc::channel(n);

    //Run any tasks that get scheduled.
    task::spawn(async move {
        {
            while let Some(eff) = rx.recv().await {
                eff.await
            }
        }
    });

    let agent = Arc::new("agent");
    let routing_context = RoutingContext::new("/node".parse().unwrap(), (), HashMap::new());
    let schedule_context = SchedulerContext::new(tx, clock, close_trigger);

    ContextImpl::new(
        agent.clone(),
        routing_context,
        schedule_context,
        meta_context_sink(),
        MockNodeStore::mock(),
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
