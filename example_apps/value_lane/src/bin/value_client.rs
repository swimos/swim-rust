use std::error::Error;
use swimos_client::{BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";
    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let lifecycle =
        BasicValueDownlinkLifecycle::default().on_set_blocking(|old_value, new_value| {
            println!("'{old_value:?}' -> '{new_value:?}'");
        });
    let view = client_handle
        .value_downlink::<i32>(RemotePath::new(host, "/example/1", "lane"))
        .lifecycle(lifecycle)
        .open()
        .await?;

    let values = [1, 2, 3, 4, 5];

    for value in values {
        view.set(value).await?;
    }

    task_handle.await?;
    Ok(())
}
