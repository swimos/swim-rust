use std::error::Error;
use swimos_client::{BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let state_lifecycle =
        BasicValueDownlinkLifecycle::default().on_event_blocking(|state| println!("{state}"));
    let state_view = client_handle
        .value_downlink::<i32>(RemotePath::new(host, "/example/1", "lane"))
        .lifecycle(state_lifecycle)
        .open()
        .await?;

    let demand_lifecycle =
        BasicValueDownlinkLifecycle::default().on_event_blocking(|state| println!("{state}"));
    client_handle
        .value_downlink::<i32>(RemotePath::new(host, "/example/1", "demand"))
        .lifecycle(demand_lifecycle)
        .open()
        .await?;

    state_view.set(13).await?;

    task_handle.await?;

    Ok(())
}
