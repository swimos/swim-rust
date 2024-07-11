use std::error::Error;
use swimos_client::{BasicMapDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";
    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let state_lifecycle = BasicMapDownlinkLifecycle::default().on_update_blocking(
        |key, _map, old_value, new_value| {
            println!("'{key:?}' changed from '{old_value:?}' to '{new_value:?}'")
        },
    );
    let state_view = client_handle
        .map_downlink(RemotePath::new(host, "/example/1", "lane"))
        .lifecycle(state_lifecycle)
        .open()
        .await?;

    let keys = [(1, "a"), (2, "b"), (3, "c")];

    for (key, value) in keys {
        state_view.update(key, value.to_string()).await?;
    }

    task_handle.await?;
    Ok(())
}
