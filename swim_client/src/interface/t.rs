use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::downlink::{Downlink, Event};
use crate::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;
use tokio::stream::StreamExt;
use tokio::time::Duration;

#[tokio::test]
async fn test_value_dl_recv() {
    let host = format!("warps://127.0.0.1:{}", 9001);
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

    let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");

    let (downlink, mut recv) = client.value_downlink(path.clone(), 0).await.unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let (_topic, mut sink) = downlink.split();
    let val = 100;
    sink.set(100).await.unwrap();

    let message = recv.next().await.unwrap();

    assert_eq!(message, Event::Remote(val));
}
