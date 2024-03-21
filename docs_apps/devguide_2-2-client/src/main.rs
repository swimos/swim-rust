use std::str::FromStr;

use client::{BasicValueDownlinkLifecycle, DownlinkConfig, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() {
    // Build a Swim Client using the default configuration.
    // The `build` method returns a `SwimClient` instance and its internal
    // runtime future that is spawned below.
    let (client, task) = SwimClientBuilder::default().build().await;
    let _client_task = tokio::spawn(task);
    let handle = client.handle();

    // We pass in the port that the server bound to and access it from the
    // command line arguments provided.
    let args = std::env::args().collect::<Vec<_>>();
    let port = match args.get(1) {
        Some(port) => usize::from_str(port.as_ref()).unwrap(),
        None => panic!("No argument provided"),
    };

    // Build a path the downlink.
    let path = RemotePath::new(
        format!("ws://0.0.0.0:{}", port),
        // We can provide any agent URI that matches the pattern
        // "/example/:id"
        "/example/1",
        // This is the URI of the ValueLane<i32> in our ExampleAgent
        "lane",
    );

    let lifecycle = BasicValueDownlinkLifecycle::<usize>::default()
        // Register an event handler that is invoked when the downlink connects to the  agent.
        .on_linked_blocking(|| println!("Downlink linked"))
        // Register an event handler that is invoked when the downlink synchronises its state
        // with the agent.
        .on_synced_blocking(|value| println!("Downlink synced with: {value:?}"))
        // Register an event handler that is invoked when the downlink receives an event.
        .on_event_blocking(|value| println!("Downlink event: {value:?}"));

    // Build our downlink.
    //
    // This operation may fail due to a connection issue.
    let downlink = handle
        .value_downlink::<usize>(path.clone())
        .lifecycle(lifecycle)
        .downlink_config(DownlinkConfig::default())
        .open()
        .await
        .expect("Failed to open downlink");

    for i in 0..10 {
        // Update the agent's state.
        downlink.set(i).await.expect("Failed to set downlink state");
    }

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c.");
}
