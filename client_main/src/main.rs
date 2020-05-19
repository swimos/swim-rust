use swim::configuration::downlink::*;
use swim::interface::SwimClient;
use tokio::time::Duration;
use tracing::info;

#[swim::client]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let client_params = ClientParams::new(2).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(60000),
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();

    let config = ConfigHierarchy::new(client_params, default_params);
    let mut client = SwimClient::new(Box::new(config));

    let r = client
        .run_session(|mut ctx| async move {
            info!("Running session");

            ctx.spawn(async {
                info!("Running spawned task");
            });
        })
        .await;

    info!("Session result: {:?}", r);
}
