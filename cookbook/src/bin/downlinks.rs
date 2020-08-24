use rand::seq::SliceRandom;
use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::model::map::MapAction;
use swim_client::interface::SwimClient;
use swim_common::sink::item::ItemSink;
use swim_common::warp::path::AbsolutePath;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri_prefix = "/unit/";

    let path = AbsolutePath::new(
        host_uri.clone(),
        &*format!("{}{}", node_uri_prefix, "0"),
        "shoppingCart",
    );

    let (mut map_downlink, _) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create downlink!");

    map_downlink
        .send_item(MapAction::insert(
            String::from("FromClientLink").into(),
            25.into(),
        ))
        .await
        .expect("Failed to send message!");

    tokio::time::delay_for(Duration::from_secs(2)).await;

    drop(map_downlink);

    let items = vec!["bat", "cat", "rat"];

    for i in 0..50 {
        let path = AbsolutePath::new(
            host_uri.clone(),
            &*format!("{}{}", node_uri_prefix, (i % 3).to_string()),
            "addItem",
        );

        client
            .send_command(
                path,
                items
                    .choose(&mut rand::thread_rng())
                    .expect("No items to send!")
                    .to_owned()
                    .into(),
            )
            .await
            .expect("Failed to send command!");
    }

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
