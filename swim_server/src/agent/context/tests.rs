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
use crate::interface::ServerDownlinksConfig;
use crate::meta::meta_context_sink;
use crate::routing::TopLevelServerRouterFactory;
use futures::future::BoxFuture;
use server_store::agent::mock::MockNodeStore;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_async_runtime::task;
use swim_async_runtime::time::clock::Clock;
use swim_client::configuration::DownlinkConnectionsConfig;
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_client::router::ClientRouterFactory;
use swim_runtime::task;
use swim_runtime::time::clock::Clock;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use tokio::time::Duration;
use url::Url;

#[derive(Clone)]
struct MockRouter {}

impl Router for MockRouter {
    fn resolve_sender(
        &mut self,
        _addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        unimplemented!()
    }
}

#[test]
fn simple_accessors() {
    let (tx, _rx) = mpsc::channel(1);
    let (_close, close_sig) = trigger::trigger();
    let agent = Arc::new("agent");
    let routing_context =
        RoutingContext::new("/node".parse().unwrap(), MockRouter {}, HashMap::new());
    let schedule_context = SchedulerContext::new(tx, TestClock::default(), close_sig.clone());

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        NonZeroUsize::new(8).unwrap(),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);
    let context = ContextImpl::new(
        agent.clone(),
        routing_context,
        schedule_context,
        meta_context_sink(),
        client,
        RelativeUri::try_from("/mock/router".to_string()).unwrap(),
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
) -> ContextImpl<&'static str, impl Clock, MockRouter, SwimNodeStore<MockPlaneStore>> {
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
    let routing_context =
        RoutingContext::new("/node".parse().unwrap(), MockRouter {}, HashMap::new());
    let schedule_context = SchedulerContext::new(tx, clock, close_trigger);

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        NonZeroUsize::new(8).unwrap(),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);
    ContextImpl::new(
        agent.clone(),
        routing_context,
        schedule_context,
        meta_context_sink(),
        client,
        RelativeUri::try_from("/mock/router".to_string()).unwrap(),
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
