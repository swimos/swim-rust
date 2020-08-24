use futures::StreamExt;
use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::model::map::MapEvent;
use swim_client::downlink::subscription::TypedMapReceiver;
use swim_client::downlink::typed::event::TypedViewWithEvent;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

const THRESHOLD: i32 = 1000;

async fn did_update(map_recv: TypedMapReceiver<String, i32>, default: i32) {
    map_recv
        .filter_map(|event| async {
            match event {
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Insert(key),
                }) => {
                    let value = view.get(&key).unwrap_or(default);

                    if value > THRESHOLD {
                        Some((key, value))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .for_each(|(street_name, population)| async move {
            println!("{:?} has {:?} residents", street_name, population,)
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "/join/state/all";
    let lane_uri = "join";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, lane_uri);

    let (_, map_recv) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create downlink!");

    task::spawn(did_update(map_recv, 0));

    tokio::time::delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
