use std::error::Error;
use swimos_client::{BasicEventDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";
    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let state_view = client_handle
        .value_downlink(RemotePath::new(host, "/example/1", "lane"))
        .open()
        .await?;

    let doubled_lifecycle = BasicEventDownlinkLifecycle::default().on_event_blocking(|event| {
        println!("{event:?}");
    });
    let _doubled_view = client_handle
        .event_downlink::<i32>(RemotePath::new(host, "/example/1", "supply"))
        .lifecycle(doubled_lifecycle)
        .open()
        .await?;

    let values = [1, 2, 3, 4, 5];

    for value in values {
        state_view.set(value).await?;
    }

    task_handle.await?;
    Ok(())
}
