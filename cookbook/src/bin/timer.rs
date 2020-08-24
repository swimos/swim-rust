use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::interface::SwimClient;
use swim_common::model::Value;
use swim_common::warp::path::AbsolutePath;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&format!("ws://127.0.0.1:9001")).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "publish";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, lane_uri);

    for _ in 0..10 {
        client
            .send_command(path.clone(), Value::Extant)
            .await
            .expect("Failed to send command!");

        tokio::time::delay_for(Duration::from_secs(5)).await;
    }

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
