use crate::agent::lane::channels::AgentExecutionConfig;
use crate::plane::run_plane;
use crate::plane::spec::PlaneBuilder;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::RemoteConnectionsTask;
use crate::routing::ws::tungstenite::TungsteniteWsConnections;
use crate::routing::SuperRouterFactory;
use futures::join;
use futures_util::core_reexport::time::Duration;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;

#[tokio::test]
async fn ws_plane() {
    let (plane_tx, plane_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);

    let super_router_fac = SuperRouterFactory::new(plane_tx.clone(), remote_tx.clone());

    let agent_config = AgentExecutionConfig::default();
    let clock = swim_runtime::time::clock::runtime_clock();
    let (_stop_tx, stop_rx) = trigger::trigger();
    let spec = PlaneBuilder::new().build();

    let plane = run_plane(
        agent_config,
        clock,
        spec,
        stop_rx,
        OpenEndedFutures::new(),
        plane_tx,
        plane_rx,
        super_router_fac.clone(),
    );

    let conn_config = ConnectionConfig {
        router_buffer_size: NonZeroUsize::new(10).unwrap(),
        channel_buffer_size: NonZeroUsize::new(10).unwrap(),
        activity_timeout: Duration::new(30, 00),
        connection_retries: Default::default(),
    };
    let external = TokioPlainTextNetworking {};
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let ws = TungsteniteWsConnections {
        config: Default::default(),
    };

    let (_stop_tx, stop_rx) = trigger::trigger();
    let spawner = OpenEndedFutures::new();

    let connections = RemoteConnectionsTask::new(
        conn_config,
        external,
        address,
        ws,
        super_router_fac,
        stop_rx,
        spawner,
    );

    let _ = join!(connections.await.unwrap().run(), plane);
}
