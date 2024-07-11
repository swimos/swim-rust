use std::error::Error;
use swimos_client::{BasicValueDownlinkLifecycle, RemotePath, SwimClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let host = "ws://127.0.0.1:8080";

    let (client, task) = SwimClientBuilder::default().build().await;
    let task_handle = tokio::spawn(task);

    let client_handle = client.handle();

    let car_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/cars/1 speed: {speed}"));
    client_handle
        .value_downlink::<u64>(RemotePath::new(host, "/cars/1", "speed"))
        .lifecycle(car_lifecycle)
        .open()
        .await?;

    let area_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/area/arbury average speed: {speed}"));
    client_handle
        .value_downlink::<f64>(RemotePath::new(host, "/area/arbury", "average_speed"))
        .lifecycle(area_lifecycle)
        .open()
        .await?;

    let city_lifecycle = BasicValueDownlinkLifecycle::default()
        .on_event_blocking(|speed| println!("/city average speed: {speed}"));
    client_handle
        .value_downlink::<f64>(RemotePath::new(host, "/city", "average_speed"))
        .lifecycle(city_lifecycle)
        .open()
        .await?;

    task_handle.await?;

    Ok(())
}
