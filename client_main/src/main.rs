use swim::configuration::downlink::*;
use swim::interface::SwimClient;

use swim::common::sink::item::ItemSink;
use swim::common::topic::Topic;
use swim::common::warp::path::AbsolutePath;
use swim::downlink::model::value::Action;
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
    let mut client = SwimClient::new(config).await;

    let r = client
        .run_session(|mut ctx| async move {
            info!("Running session");

            let val_path = AbsolutePath::new("my_host", "my_agent", "value_lane");
            let mut dl = ctx.value_downlink::<i32>(0, val_path).await.unwrap();
            let r = dl.send_item(Action::set(1.into())).await;
            let _sub = dl.subscribe().await.unwrap();

            println!("Send_item result: {:?}", r);

            ctx.spawn(async {
                // let _r = dl.unwrap().subscribe().await.unwrap();
                info!("Subscribed");
            });
        })
        .await;

    info!("Session result: {:?}", r);
}
