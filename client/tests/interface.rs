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

#[cfg(feature = "test_server")]
mod tests {
    use client::configuration::downlink::{
        BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
    };
    use client::connections::factory::tungstenite::TungsteniteWsFactory;
    use client::downlink::model::map::{
        MapAction, MapEvent, MapModification, UntypedMapModification,
    };
    use client::downlink::model::value::Action;
    use client::downlink::typed::event::TypedViewWithEvent;
    use client::downlink::Event;
    use client::interface::SwimClient;
    use common::model::{Attr, Item, Value};
    use common::sink::item::ItemSink;
    use common::topic::Topic;
    use common::warp::path::AbsolutePath;
    use swim_form::Form;
    use test_server::clients::Cli;
    use test_server::Docker;
    use test_server::SwimTestServer;
    use tokio::stream::StreamExt;
    use tokio::time::Duration;

    fn config() -> ConfigHierarchy {
        let client_params = ClientParams::new(2, Default::default()).unwrap();
        let timeout = Duration::from_secs(60000);
        let default_params = DownlinkParams::new_queue(
            BackpressureMode::Propagate,
            5,
            timeout,
            5,
            OnInvalidMessage::Terminate,
            256,
        )
        .unwrap();
        ConfigHierarchy::new(client_params, default_params)
    }

    #[tokio::test]
    async fn test_value_dl_recv() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");

        let (_, mut recv) = client.value_downlink(path.clone(), 0).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Remote(0));
    }

    #[tokio::test]
    async fn test_value_dl_send() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");

        let (mut dl, mut recv) = client.value_downlink(path.clone(), 0).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        dl.send_item(Action::set(10.into_value())).await.unwrap();

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Remote(0));

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Local(10));
    }

    #[tokio::test]
    async fn test_map_dl_recv() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let (_, mut recv) = client
            .map_downlink::<String, i32>(path.clone())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let message = recv.next().await.unwrap();

        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.len(), 0);
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_map_dl_send() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let (mut dl, mut recv) = client
            .map_downlink::<String, i32>(path.clone())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        dl.send_item(MapAction::insert(String::from("milk").into(), 1.into()))
            .await
            .unwrap();

        let message = recv.next().await.unwrap();

        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.len(), 0);
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let message = recv.next().await.unwrap();

        if let Event::Local(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.get(&String::from("milk")).unwrap(), 1);
            assert_eq!(event, MapEvent::Insert(String::from("milk")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_recv_untyped_value_event() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut event_dl = client.untyped_event_downlink(event_path).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item("Hello, from Rust!".into())
            .await
            .unwrap();

        let incoming = event_dl.recv().await.unwrap();

        assert_eq!(incoming, Value::Text("Hello, from Rust!".to_string()));
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_valid() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut event_dl = client
            .event_downlink::<String>(event_path, Default::default())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item("Hello, from Rust!".into())
            .await
            .unwrap();

        let incoming = event_dl.recv().await.unwrap();

        assert_eq!(incoming, "Hello, from Rust!");
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_invalid() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut event_dl = client
            .event_downlink::<i32>(event_path, Default::default())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item("Hello, from Rust!".into())
            .await
            .unwrap();

        let incoming = event_dl.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_recv_untyped_map_event() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client.untyped_event_downlink(event_path).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item(
                UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                    .as_value(),
            )
            .await
            .unwrap();

        let incoming = event_dl.recv().await.unwrap();

        let header = Attr::of(("update", Value::record(vec![Item::slot("key", "milk")])));
        let body = Item::of(6);
        let expected = Value::Record(vec![header], vec![body]);

        assert_eq!(incoming, expected);
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_valid() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client
            .event_downlink::<MapModification<String, i32>>(event_path, Default::default())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item(
                UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                    .as_value(),
            )
            .await
            .unwrap();

        let incoming = event_dl.recv().await.unwrap();

        assert_eq!(incoming, MapModification::Insert("milk".to_string(), 6));
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_key() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client
            .event_downlink::<MapModification<i32, i32>>(event_path, Default::default())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item(
                UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                    .as_value(),
            )
            .await
            .unwrap();

        let incoming = event_dl.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_value() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client
            .event_downlink::<MapModification<String, String>>(event_path, Default::default())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item(
                UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                    .as_value(),
            )
            .await
            .unwrap();

        let incoming = event_dl.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_read_only_value() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut command_dl = client
            .command_downlink::<String>(path.clone())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        command_dl
            .send_item("Hello, String!".to_string())
            .await
            .unwrap();

        let (mut dl, mut recv) = client.value_downlink(path, String::new()).await.unwrap();

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Remote(String::from("Hello, String!")));

        let mut recv_view = dl
            .read_only_view::<Value>()
            .await
            .unwrap()
            .subscribe()
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        command_dl
            .send_item("Hello, Value!".to_string())
            .await
            .unwrap();

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Remote(String::from("Hello, Value!")));

        let message = recv_view.next().await.unwrap();
        assert_eq!(
            message,
            Event::Remote(Value::from("Hello, Value!".to_string()))
        );
    }

    #[tokio::test]
    async fn test_read_only_value_schema_error() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default(TungsteniteWsFactory::new(5).await).await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");
        let (mut dl, _) = client.value_downlink(path.clone(), 0i64).await.unwrap();

        if let Err(view_error) = dl.read_only_view::<String>().await {
            assert_eq!(view_error.to_string(),  "A read-only downlink with schema @kind(text) was requested but the original downlink is running with schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.read_only_view::<i32>().await {
            assert_eq!(view_error.to_string(),  "A read-only downlink with schema @kind(int32) was requested but the original downlink is running with schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }
    }
}
