use futures::StreamExt;
use swim_client::common::model::Value;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::interface::SwimClient;

#[tokio::main]
async fn main() {
    let fac = TungsteniteWsFactory::new(5).await;
    let mut client = SwimClient::new_with_default(fac).await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "random",
    );

    let (_downlink, mut receiver) = client.value_downlink(path, Value::Extant).await.unwrap();

    println!("Opened downlink");

    let mut values = Vec::new();
    let mut averages = Vec::new();
    let window_size = 200;

    while let Some(event) = receiver.next().await {
        if let Value::Int32Value(i) = event.get_inner() {
            values.push(i.clone());

            if values.len() > window_size {
                values.remove(0);
                averages.remove(0);
            }

            if !values.is_empty() {
                let sum = values.iter().fold(0, |total, x| total + *x);
                let sum = sum as f64 / values.len() as f64;

                println!("Average: {:?}", sum);

                averages.push(sum);
            }
        } else {
            panic!("Expected Int32 value");
        }
    }
}
