use std::error::Error;

use swimos_client::{BasicMapDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let aggregated_lifecycle = BasicMapDownlinkLifecycle::default()
        .on_update_blocking(|key, _map, _old_state, new_state| println!("{key} -> {new_state}"));
    client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/join/state/example", "streets"))
        .lifecycle(aggregated_lifecycle)
        .open()
        .await?;

    let california_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/california", "state"))
        .open()
        .await?;
    california_view.update("street_a".to_string(), 1).await?;

    let texas_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/texas", "state"))
        .open()
        .await?;
    texas_view.update("street_b".to_string(), 2).await?;

    let florida_view = client_handle
        .map_downlink::<String, u64>(RemotePath::new(host, "/state/florida", "state"))
        .open()
        .await?;
    florida_view.update("street_c".to_string(), 3).await?;

    task_handle.await?;

    Ok(())
}
