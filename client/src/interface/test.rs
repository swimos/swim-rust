use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::interface::SwimClient;
use common::model::Value;
use common::warp::path::AbsolutePath;
use tokio::stream::StreamExt;
use tokio::time::Duration;

#[tokio::test]
#[ignore]
async fn test() {
    let host = format!("ws://127.0.0.1:9001");

    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

    let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

    let (mut dl, mut recv) = client
        .value_downlink(path.clone(), String::new())
        .await
        .unwrap();

    tokio::time::delay_for(Duration::from_secs(1)).await;

    let mut recv_view = dl.read_only_view::<Value>().await.unwrap();

    let message = recv.next().await.unwrap();
    println!("{:?}", message);
    // Remote("Hello from link, world!")
    let message = recv_view.next().await.unwrap();
    println!("{:?}", message)
    // Remote(Text("Hello from link, world!"))
}
