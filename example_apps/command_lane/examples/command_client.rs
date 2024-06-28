use command_lane::Instruction;
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
    let _state_view = client_handle
        .value_downlink::<i32>(RemotePath::new(host, "/example/1", "lane"))
        .lifecycle(state_lifecycle)
        .open()
        .await?;

    let command_handle = client_handle
        .value_downlink::<Instruction>(RemotePath::new(host, "/example/1", "command"))
        .open()
        .await?;

    for i in 0..10 {
        command_handle.set(Instruction::Add(i)).await?;
    }

    command_handle.set(Instruction::Zero).await?;

    task_handle.await?;

    Ok(())
}
