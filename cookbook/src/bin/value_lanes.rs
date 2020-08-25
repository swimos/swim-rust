use futures::StreamExt;
use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::subscription::TypedValueReceiver;
use swim_client::downlink::Downlink;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

async fn did_set(value_recv: TypedValueReceiver<String>, initial_value: String) {
    value_recv
        .filter_map(|event| async {
            match event {
                Remote(event) => Some(event),
                _ => None,
            }
        })
        .scan(initial_value, |state, current| {
            let previous = std::mem::replace(state, current.clone());
            async { Some((previous, current)) }
        })
        .for_each(|(previous, current)| async move {
            println!(
                "Link watched info change TO {:?} FROM {:?}",
                current, previous
            )
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "info";

    let path = AbsolutePath::new(host_uri, node_uri, lane_uri);
    let (value_downlink, value_recv) = client
        .value_downlink(path.clone(), String::new())
        .await
        .expect("Failed to create value downlink!");

    let (_dl_topic, mut dl_sink) = value_downlink.split();

    let initial_value = dl_sink
        .get()
        .await
        .expect("Failed to retrieve initial downlink value!");

    task::spawn(did_set(value_recv, initial_value));

    // Send using either the proxy command lane...
    client
        .send_command(path, String::from("Hello from command, world!").into())
        .await
        .expect("Failed to send command!");
    tokio::time::delay_for(Duration::from_secs(2)).await;

    // ...or a downlink set()
    dl_sink
        .set("Hello from link, world!".to_string())
        .await
        .expect("Failed to send message!");
    tokio::time::delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
