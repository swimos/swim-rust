use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::typed::SchemaViolations;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&format!("ws://127.0.0.1:9001")).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "publish";

    let path = AbsolutePath::new(host_uri, node_uri, lane_uri);

    let mut event_dl = client
        .event_downlink::<i64>(path.clone(), SchemaViolations::Ignore)
        .await
        .unwrap();

    task::spawn(async move {
        while let Some(event) = event_dl.recv().await {
            println!("Link received event: {}", event)
        }
    });

    // command() `msg` TO
    // the "publish" lane OF
    // the agent addressable by `/unit/foo` RUNNING ON
    // the plane with hostUri "warp://localhost:9001"
    let msg = 9035768;
    client
        .send_command(path, msg.into())
        .await
        .expect("Failed to send a command!");
    tokio::time::delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
