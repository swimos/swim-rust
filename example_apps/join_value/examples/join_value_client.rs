use rand::Rng;
use std::error::Error;
use std::time::Duration;
use swimos_client::{
    BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::new("")
        .add_directive("swimos_client=trace".parse()?)
        .add_directive("swimos_server_app=trace".parse()?)
        .add_directive("swimos_runtime=trace".parse()?)
        .add_directive("swimos_agent=trace".parse()?)
        .add_directive("swimos_messages=trace".parse()?)
        .add_directive("swimos_remote=trace".parse()?);
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let rooms = ["living_room", "kitchen", "office"];
    let buildings = ["a", "b", "c"];

    for building in buildings {
        let aggregated_lifecycle = BasicMapDownlinkLifecycle::default().on_update_blocking(
            |key, _map, _old_state, new_state| println!("Building {key} -> {new_state}"),
        );
        let _aggregated_view = client_handle
            .map_downlink::<String, bool>(RemotePath::new(
                host,
                format!("/buildings/{building}"),
                "lights",
            ))
            .lifecycle(aggregated_lifecycle)
            .open()
            .await?;
    }

    for building in buildings {
        for room in rooms {
            let handle = client_handle.clone();
            let task = async move {
                let node_address = format!("/rooms/{building}/{building}_{room}");
                let building_lifecycle = BasicValueDownlinkLifecycle::default()
                    .on_synced_blocking(move |state| println!("Building '{building}' -> {state}"));
                let building_view = handle
                    .value_downlink::<bool>(RemotePath::new(host, node_address.as_str(), "lights"))
                    .lifecycle(building_lifecycle)
                    .open()
                    .await
                    .expect(format!("Failed to open downlink to {node_address}").as_str());

                let mut rng = rand::rngs::OsRng::default();
                // loop {
                let state = rng.gen_bool(0.5);
                building_view
                    .set(state)
                    .await
                    .expect("Failed to set downlink");

                tokio::time::sleep(Duration::from_secs(1)).await;
                // }
            };
            tokio::spawn(task);
        }
    }

    task_handle.await?;

    Ok(())
}
