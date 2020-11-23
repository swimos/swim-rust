use crate::agent::lane::channels::AgentExecutionConfig;
use crate::plane::run_plane;
use crate::plane::spec::PlaneBuilder;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::TokioNetworking;
use crate::routing::remote::RemoteConnectionsTask;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;
use utilities::task::TokioSpawner;

#[tokio::test]
async fn plane_test() {
    let agent_config = AgentExecutionConfig::default();
    let clock = swim_runtime::time::clock::runtime_clock();
    let (stop_tx, stop_rx) = trigger::trigger();
    let spec = PlaneBuilder::new().build();
    let plane = run_plane(agent_config, clock, spec, stop_rx, OpenEndedFutures::new());

    plane.await
}

#[tokio::test]
async fn connections_test() {
    let conn_config = ConnectionConfig {
        router_buffer_size: NonZeroUsize::new(10).unwrap(),
        channel_buffer_size: NonZeroUsize::new(10).unwrap(),
        activity_timeout: Default::default(),
        connection_retries: Default::default(),
    };
    let ws = TokioNetworking {};
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let (stop_tx, stop_rx) = trigger::trigger();
    let spawner = TokioSpawner::new();

    let connections = RemoteConnectionsTask::new(conn_config, ws, address);
    connections.await.unwrap().run().await.unwrap()
}
