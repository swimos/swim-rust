use swim::configuration::downlink::*;
use swim::interface::SwimClient;

use swim::common::sink::item::ItemSink;
use swim::common::warp::path::AbsolutePath;
use swim::downlink::model::value::Action;
use tokio::time::Duration;
use tracing::info;

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(60000),
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

#[swim::client]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let mut client = SwimClient::new(config()).await;

    let r = client
        .run_session(|mut ctx| async move {
            info!("Running session");

            let val_path = AbsolutePath::new("my_host", "my_agent", "value_lane");
            let (mut dl, _receiver) = ctx.value_downlink::<i32>(0, val_path).await.unwrap();
            let r = dl.send_item(Action::set(1.into())).await;
            info!("{:?}", r);

            ctx.clone().spawn(async move {
                info!("First inner future");

                ctx.clone().spawn(async {
                    info!("Second inner future");
                });
            });
        })
        .await;

    info!("Session result: {:?}", r);
}
