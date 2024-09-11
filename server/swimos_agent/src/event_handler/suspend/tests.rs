// Copyright 2015-2024 Swim Inc.
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

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    event_handler::{
        ActionContext, EventHandler, EventHandlerError, HandlerAction, SideEffect, StepResult,
    },
    meta::AgentMetadata,
    test_context::{TestSpawner, NO_DOWNLINKS, NO_DYN_LANES},
};
use bytes::BytesMut;
use futures::StreamExt;
use parking_lot::Mutex;
use swimos_api::agent::AgentConfig;
use swimos_utilities::{routing::RouteUri, trigger};
use tokio::{sync::mpsc, time::Instant};

use super::Suspend;

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

struct DummyAgent;

#[tokio::test]
async fn suspend_future() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    let (tx, mut rx) = mpsc::channel(4);
    let (done_tx, done_rx) = trigger::trigger();

    let mut suspend = Suspend::new(async move {
        let _ = tx.send(45).await;
        SideEffect::from(move || {
            let _ = done_tx.trigger();
        })
    });

    let mut spawner = TestSpawner::<DummyAgent>::default();

    let result = suspend.step(
        &mut ActionContext::new(
            &spawner,
            &NO_DOWNLINKS,
            &NO_DYN_LANES,
            &mut join_lane_init,
            &mut ad_hoc_buffer,
        ),
        meta,
        &DummyAgent,
    );

    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    tokio::time::timeout(Duration::from_secs(5), async move {
        let mut handler = spawner.next().await.expect("Future was not suspended.");
        assert!(spawner.is_empty());

        assert_eq!(rx.recv().await, Some(45));

        let result = handler.step(
            &mut ActionContext::new(
                &spawner,
                &NO_DOWNLINKS,
                &NO_DYN_LANES,
                &mut join_lane_init,
                &mut ad_hoc_buffer,
            ),
            meta,
            &DummyAgent,
        );
        assert!(matches!(
            result,
            StepResult::Complete {
                modified_item: None,
                ..
            }
        ));
        assert!(spawner.is_empty());
        assert!(done_rx.await.is_ok());

        let result = suspend.step(
            &mut ActionContext::new(
                &spawner,
                &NO_DOWNLINKS,
                &NO_DYN_LANES,
                &mut join_lane_init,
                &mut ad_hoc_buffer,
            ),
            meta,
            &DummyAgent,
        );
        assert!(matches!(
            result,
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        ));

        assert!(spawner.is_empty());
    })
    .await
    .expect("Timed out.");
}

fn run_handler<H>(mut handler: H, spawner: &TestSpawner<DummyAgent>)
where
    H: EventHandler<DummyAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        spawner,
        &NO_DOWNLINKS,
        &NO_DYN_LANES,
        &mut join_lane_init,
        &mut ad_hoc_buffer,
    );
    loop {
        match handler.step(&mut action_context, meta, &DummyAgent) {
            StepResult::Continue { modified_item } => assert!(modified_item.is_none()),
            StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
            StepResult::Complete { modified_item, .. } => {
                assert!(modified_item.is_none());
                let after = handler.step(&mut action_context, meta, &DummyAgent);
                assert!(matches!(
                    after,
                    StepResult::Fail(EventHandlerError::SteppedAfterComplete)
                ));
                break;
            }
        }
    }
}

async fn run_handler_with_futures<H>(handler: H)
where
    H: EventHandler<DummyAgent>,
{
    let mut spawner = TestSpawner::default();
    run_handler(handler, &spawner);

    if !spawner.is_empty() {
        while let Some(h) = spawner.next().await {
            run_handler(h, &spawner);
        }
    }
}

const DELAY: Duration = Duration::from_secs(1);

#[tokio::test(start_paused = true)]
async fn delayed_handler() {
    let value: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let value_cpy = value.clone();

    let handler = SideEffect::from(move || {
        let mut guard = value_cpy.lock();
        *guard = true;
    });

    let delayed = super::run_after::<DummyAgent, _>(DELAY, handler);

    let before = Instant::now();
    run_handler_with_futures(delayed).await;
    let after = Instant::now();
    let delta = after.duration_since(before);
    assert_eq!(delta, DELAY);

    assert!(*value.lock());
}

fn set_n(
    events: Arc<Mutex<Vec<usize>>>,
    n: usize,
) -> impl EventHandler<DummyAgent> + Send + 'static {
    SideEffect::from(move || {
        let mut guard = events.lock();
        guard.push(n);
    })
}

#[tokio::test(start_paused = true)]
async fn scheduled_handler() {
    let events: Arc<Mutex<Vec<usize>>> = Default::default();
    let handlers = vec![
        (DELAY, set_n(events.clone(), 0)),
        (DELAY, set_n(events.clone(), 1)),
        (DELAY, set_n(events.clone(), 2)),
    ];

    let sched = super::run_schedule(handlers);

    let before = Instant::now();
    run_handler_with_futures(sched).await;
    let after = Instant::now();
    let delta = after.duration_since(before);

    assert_eq!(delta, DELAY * 3);

    let guard = events.lock();
    assert_eq!(*guard, vec![0, 1, 2]);
}

async fn set_n_async(
    events: Arc<Mutex<Vec<usize>>>,
    n: usize,
) -> impl EventHandler<DummyAgent> + Send + 'static {
    tokio::time::sleep(DELAY).await;
    set_n(events, n)
}

#[tokio::test(start_paused = true)]
async fn scheduled_handler_async() {
    let events: Arc<Mutex<Vec<usize>>> = Default::default();
    let handlers = vec![
        set_n_async(events.clone(), 0),
        set_n_async(events.clone(), 1),
        set_n_async(events.clone(), 2),
    ];
    let stream = futures::stream::iter(handlers).flat_map(futures::stream::once);

    let sched = super::run_schedule_async(stream.boxed());

    let before = Instant::now();
    run_handler_with_futures(sched).await;
    let after = Instant::now();
    let delta = after.duration_since(before);

    assert_eq!(delta, DELAY * 3);

    let guard = events.lock();
    assert_eq!(*guard, vec![0, 1, 2]);
}
