// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::StreamExt;
use tokio::time::Duration;

use client::connections::factory::tungstenite::TungsteniteWsFactory;
use client::downlink::model::map::MapModification;
use client::downlink::typed::event::TypedViewWithEvent;
use client::downlink::Event;
use client::interface::SwimClient;
use common::sink::item::ItemSink;
use common::warp::path::AbsolutePath;
use tracing::Level;

#[tokio::test]
async fn t() {
    // utilities::trace::init_trace(vec!["client=trace"]);

    let host = format!("ws://127.0.0.1:9001");
    let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
    let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "integerMap");
    let mut command_dl = client
        .command_downlink::<MapModification<i32, i32>>(path.clone())
        .await
        .unwrap();

    // tokio::time::delay_for(Duration::from_secs(1)).await;
    //
    // command_dl
    //     .send_item(MapModification::Insert(1i32, 6i32))
    //     .await
    //     .unwrap();

    // tokio::time::delay_for(Duration::from_secs(1)).await;

    let (mut dl, mut recv) = client.map_downlink::<i32, i32>(path).await.unwrap();
    let message = recv.next().await.unwrap();

    println!("{:?}", message);

    command_dl
        .send_item(MapModification::Insert(2, 5))
        .await
        .unwrap();
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let message = recv.next().await.unwrap();

    if let Event::Remote(event) = message {
        let TypedViewWithEvent { view, event } = event;

        println!("{:?}", view.get(&1i32));
        println!("{:?}", event);
    }
}
