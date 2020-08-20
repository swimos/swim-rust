use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::model::value::Action;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::sink::item::ItemSink;
use swim_common::warp::path::AbsolutePath;
use tokio::stream::StreamExt;
use tokio::task;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&format!("ws://127.0.0.1:9001")).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "info";

    let path = AbsolutePath::new(host_uri, node_uri, lane_uri);
    let (mut value_downlink, mut value_recv) = client
        .value_downlink(path.clone(), String::new())
        .await
        .expect("Failed to create value downlink!");

    let mut command_dl = client.command_downlink::<String>(path).await.unwrap();

    task::spawn(async move {
        while let Some(event) = value_recv.next().await {
            match event {
                Remote(event) => println!(
                    "Link watched info change TO {:?} FROM {:?}",
                    event.clone(),
                    event
                ),
                _ => (),
            }
        }
    });

    // Send using either the proxy command lane...
    command_dl
        .send_item(String::from("Hello from command, world!"))
        .await
        .unwrap();
    tokio::time::delay_for(Duration::from_secs(2)).await;

    // ...or a downlink set()
    value_downlink
        .send_item(Action::set(String::from("Hello from link, world").into()))
        .await
        .expect("Failed to send message!");
    tokio::time::delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
