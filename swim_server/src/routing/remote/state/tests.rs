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

use crate::routing::remote::state::RemoteConnections;
use crate::routing::remote::test_fixture::{FakeWebsockets, FakeConnections, FakeSocket, FakeListener, LocalRoutes};
use crate::routing::remote::config::ConnectionConfig;
use pin_utils::core_reexport::num::NonZeroUsize;
use pin_utils::core_reexport::time::Duration;
use utilities::future::retryable::strategy::RetryStrategy;
use utilities::future::open_ended::OpenEndedFutures;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::io;
use tokio::sync::mpsc;
use utilities::sync::trigger;
use futures::future::BoxFuture;
use crate::routing::RoutingAddr;
use crate::routing::remote::ConnectionDropped;

type TestSpawner = OpenEndedFutures<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>>;
type TestConnections<'a> = RemoteConnections<'a, FakeConnections, FakeWebsockets, TestSpawner, LocalRoutes>;

struct TestFixture<'a> {
    connections: TestConnections<'a>,
    fake_connections: FakeConnections,
    local: LocalRoutes,
    stop_trigger: trigger::Sender,
}

fn make_state(addr: RoutingAddr,
              ws: &FakeWebsockets,
              incoming: mpsc::Receiver<io::Result<(FakeSocket, SocketAddr)>>) -> TestFixture<'_> {

    let buffer_size = NonZeroUsize::new(8).unwrap();

    let config = ConnectionConfig {
        router_buffer_size: buffer_size,
        channel_buffer_size: buffer_size,
        activity_timeout: Duration::from_secs(30),
        connection_retries: RetryStrategy::none(),
    };

    let fake_connections = FakeConnections::new(HashMap::new(), HashMap::new(), None);
    let router = LocalRoutes::new(addr);

    let (stop_tx, stop_rx) = trigger::trigger();

    let connections = RemoteConnections::new(
        ws,
        config,
        OpenEndedFutures::new(),
        fake_connections.clone(),
        FakeListener::new(incoming),
        stop_rx,
        router.clone(),
    );

    TestFixture {
        connections,
        fake_connections,
        local: router,
        stop_trigger: stop_tx
    }

}