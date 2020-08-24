use futures::StreamExt;
use std::time::Duration;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::downlink::model::map::{MapAction, MapEvent};
use swim_client::downlink::subscription::TypedMapReceiver;
use swim_client::downlink::typed::event::{TypedMapView, TypedViewWithEvent};
use swim_client::downlink::Downlink;
use swim_client::downlink::Event::Remote;
use swim_client::interface::SwimClient;
use swim_common::sink::item::ItemSink;
use swim_common::warp::path::AbsolutePath;
use tokio::task;

async fn did_update(
    map_recv: TypedMapReceiver<String, i32>,
    initial_value: TypedMapView<String, i32>,
    default: i32,
) {
    map_recv
        .filter_map(|event| async {
            match event {
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Insert(key),
                }) => Some((key, view)),
                Remote(TypedViewWithEvent {
                    view,
                    event: MapEvent::Remove(key),
                }) => Some((key, view)),
                _ => None,
            }
        })
        .scan(initial_value, |state, (key, current_view)| {
            let previous_view = state.clone();
            *state = current_view.clone();
            async { Some((key, previous_view, current_view)) }
        })
        .for_each(|(key, previous, current)| async move {
            println!(
                "Link watched {:?} changed to {:?} from {:?}",
                key,
                current.get(&key).unwrap_or(default),
                previous.get(&key).unwrap_or(default)
            )
        })
        .await;
}

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let cart_lane = "shoppingCart";
    let add_lane = "addItem";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, cart_lane);

    let (map_downlink, map_recv) = client
        .map_downlink::<String, i32>(path)
        .await
        .expect("Failed to create map downlink!");

    let (_, mut dl_sink) = map_downlink.split();

    let initial_value = dl_sink
        .view()
        .await
        .expect("Failed to retrieve initial map downlink!");

    task::spawn(did_update(map_recv, initial_value, 0));

    let path = AbsolutePath::new(host_uri, node_uri, add_lane);
    client
        .send_command(path, "FromClientCommand".into())
        .await
        .expect("Failed to send command!");

    tokio::time::delay_for(Duration::from_secs(2)).await;

    dl_sink
        .send_item(MapAction::insert("FromClientLink".into(), 25.into()))
        .await
        .expect("Failed to send message!");

    tokio::time::delay_for(Duration::from_secs(2)).await;

    dl_sink
        .send_item(MapAction::remove("FromClientLink".into()))
        .await
        .expect("Failed to send message!");

    tokio::time::delay_for(Duration::from_secs(2)).await;

    println!("Stopping client in 2 seconds");
    tokio::time::delay_for(Duration::from_secs(2)).await;
}
