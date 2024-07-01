use swimos_client::{BasicValueDownlinkLifecycle, DownlinkConfig, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() {
    // Build a Swim Client using the default configuration.
    // The `build` method returns a `SwimClient` instance and its internal
    // runtime future that is spawned below.
    let (client, task) = SwimClientBuilder::default().build().await;
    let _client_task = tokio::spawn(task);
    let handle = client.handle();

    // Build a path the downlink.
    let state_path = RemotePath::new(
        // The host address
        "ws://0.0.0.0:8080",
        // You can provide any agent URI that matches the pattern
        // "/example/:id"
        "/example/1",
        // This is the URI of the ValueLane<i32> in our ExampleAgent
        "state",
    );

    let lifecycle = BasicValueDownlinkLifecycle::<usize>::default()
        // Register an event handler that is invoked when the downlink connects to the agent.
        .on_linked_blocking(|| println!("Downlink linked"))
        // Register an event handler that is invoked when the downlink synchronises its state.
        // with the agent.
        .on_synced_blocking(|value| println!("Downlink synced with: {value:?}"))
        // Register an event handler that is invoked when the downlink receives an event.
        .on_event_blocking(|value| println!("Downlink event: {value:?}"));

    // Build our downlink.
    //
    // This operation may fail if there is a connection issue.
    let state_downlink = handle
        .value_downlink::<usize>(state_path)
        .lifecycle(lifecycle)
        .downlink_config(DownlinkConfig::default())
        .open()
        .await
        .expect("Failed to open downlink");

    for i in 0..10 {
        // Update the lane's state.
        state_downlink
            .set(i)
            .await
            .expect("Failed to set downlink state");
    }

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c.");
}
