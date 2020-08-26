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
    use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
    use swim_client::downlink::model::map::{
        MapAction, MapEvent, MapModification, UntypedMapModification,
    };
    use swim_client::downlink::model::value::Action;
    use swim_client::downlink::typed::event::TypedViewWithEvent;
    use swim_client::downlink::{Downlink, Event};
    use swim_client::interface::SwimClient;
    use swim_common::form::Form;
    use swim_common::model::{Attr, Item, Value};
    use swim_common::sink::item::ItemSink;
    use swim_common::topic::Topic;
    use swim_common::warp::path::AbsolutePath;
    use test_server::clients::Cli;
    use test_server::Docker;
    use test_server::SwimTestServer;
    use tokio::stream::StreamExt;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_value_dl_recv() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

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
        let mut client = SwimClient::new_with_default().await;

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
        let mut client = SwimClient::new_with_default().await;
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
        let mut client = SwimClient::new_with_default().await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let (mut dl, mut recv) = client
            .map_downlink::<String, i32>(path.clone())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        dl.send_item(MapAction::update(String::from("milk").into(), 1.into()))
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
            assert_eq!(event, MapEvent::Update(String::from("milk")));
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

        let mut client = SwimClient::new_with_default().await;

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

        let mut client = SwimClient::new_with_default().await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");
        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut event_dl = client
            .event_downlink::<String>(event_path, Default::default())
            .await
            .unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client
            .command_downlink::<String>(command_path)
            .await
            .unwrap();
        command_dl
            .send_item("Hello, from Rust!".to_string())
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

        let mut client = SwimClient::new_with_default().await;

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

        let mut client = SwimClient::new_with_default().await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client.untyped_event_downlink(event_path).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send_item(
                UntypedMapModification::Update("milk".to_string().into_value(), 6.into_value())
                    .as_value(),
            )
            .await
            .unwrap();

        let incoming = event_dl.recv().await.unwrap();

        let header = Attr::of(("update", Value::record(vec![Item::slot("key", "milk")])));
        let body = Item::of(6u32);
        let expected = Value::Record(vec![header], vec![body]);

        assert_eq!(incoming, expected);
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_valid() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default().await;

        let event_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let command_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut event_dl = client
            .event_downlink::<MapModification<String, i32>>(event_path, Default::default())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let mut command_dl = client
            .command_downlink::<MapModification<String, i32>>(command_path)
            .await
            .unwrap();

        let item = MapModification::Update("milk".to_string(), 6i32);

        command_dl.send_item(item).await.unwrap();

        let incoming = event_dl.recv().await.unwrap();

        assert_eq!(incoming, MapModification::Update("milk".to_string(), 6i32));
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_key() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new_with_default().await;

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
                UntypedMapModification::Update("milk".to_string().into_value(), 6.into_value())
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

        let mut client = SwimClient::new_with_default().await;

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
                UntypedMapModification::Update("milk".to_string().into_value(), 6.into_value())
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
        let mut client = SwimClient::new_with_default().await;

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
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");
        let (mut dl, _) = client.value_downlink(path.clone(), 0i64).await.unwrap();

        if let Err(view_error) = dl.read_only_view::<String>().await {
            assert_eq!(view_error.to_string(),  "A read-only value downlink with schema @kind(text) was requested but the original value downlink is running with schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.read_only_view::<i32>().await {
            assert_eq!(view_error.to_string(),  "A read-only value downlink with schema @kind(int32) was requested but the original value downlink is running with schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }
    }

    #[tokio::test]
    async fn test_read_only_map() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut command_dl = client
            .command_downlink::<MapModification<String, i32>>(path.clone())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        command_dl
            .send_item(MapModification::Update("milk".to_string(), 1))
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let (mut dl, mut recv) = client.map_downlink::<String, i32>(path).await.unwrap();

        let message = recv.next().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.get(&String::from("milk")).unwrap(), 1);
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let mut recv_view = dl
            .read_only_view::<Value, Value>()
            .await
            .unwrap()
            .subscribe()
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        command_dl
            .send_item(MapModification::Update("eggs".to_string(), 2))
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let message = recv.next().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.get(&String::from("milk")).unwrap(), 1);
            assert_eq!(view.get(&String::from("eggs")).unwrap(), 2);
            assert_eq!(event, MapEvent::Update(String::from("eggs")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let message = recv_view.next().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::Text(String::from("milk"))).unwrap(),
                Value::UInt32Value(1)
            );
            assert_eq!(
                view.get(&Value::Text(String::from("eggs"))).unwrap(),
                Value::UInt32Value(2)
            );
            assert_eq!(event, MapEvent::Update(Value::Text(String::from("eggs"))));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_read_only_map_schema_error() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "integerMap");

        let (mut dl, _) = client.map_downlink::<i64, i64>(path).await.unwrap();

        if let Err(view_error) = dl.read_only_view::<String, String>().await {
            assert_eq!(view_error.to_string(),  "A read-only map downlink with key schema @kind(text) was requested but the original map downlink is running with key schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.read_only_view::<i64, String>().await {
            assert_eq!(view_error.to_string(),  "A read-only map downlink with value schema @kind(text) was requested but the original map downlink is running with value schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.read_only_view::<i32, i64>().await {
            assert_eq!(view_error.to_string(),  "A read-only map downlink with key schema @kind(int32) was requested but the original map downlink is running with key schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.read_only_view::<i64, i32>().await {
            assert_eq!(view_error.to_string(),  "A read-only map downlink with value schema @kind(int32) was requested but the original map downlink is running with value schema @kind(int64).")
        } else {
            panic!("Expected a ViewError!")
        }
    }

    #[tokio::test]
    async fn test_write_only_value() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "info");

        let mut command_dl = client
            .command_downlink::<String>(path.clone())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;
        command_dl.send_item(String::from("milk")).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let (mut dl, mut recv) = client.value_downlink(path, Value::Extant).await.unwrap();

        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Remote(Value::Text(String::from("milk"))));

        let mut sender_view = dl.write_only_sender::<String>().await.unwrap();
        let (_, mut sink) = dl.split();

        sink.set(String::from("bread").into()).await.unwrap();
        let message = recv.next().await.unwrap();
        assert_eq!(message, Event::Local(Value::Text(String::from("bread"))));

        sender_view.set(String::from("chocolate")).await.unwrap();
        let message = recv.next().await.unwrap();
        assert_eq!(
            message,
            Event::Local(Value::Text(String::from("chocolate")))
        );
    }

    #[tokio::test]
    async fn test_write_only_value_schema_error() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "id");
        let (mut dl, _) = client.value_downlink(path.clone(), 0i32).await.unwrap();

        if let Err(view_error) = dl.write_only_sender::<String>().await {
            assert_eq!(view_error.to_string(),  "A write-only value downlink with schema @kind(text) was requested but the original value downlink is running with schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.write_only_sender::<i64>().await {
            assert_eq!(view_error.to_string(),  "A write-only value downlink with schema @kind(int64) was requested but the original value downlink is running with schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }
    }

    #[tokio::test]
    async fn test_write_only_map() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");

        let mut command_dl = client
            .command_downlink::<MapModification<String, i32>>(path.clone())
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        command_dl
            .send_item(MapModification::Update(
                String::from("milk").into(),
                5.into(),
            ))
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let (mut dl, mut recv) = client.map_downlink::<Value, Value>(path).await.unwrap();

        let message = recv.next().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::Text(String::from("milk"))).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let mut sender_view = dl.write_only_sender::<String, i32>().await.unwrap();
        let (_, mut sink) = dl.split();

        sink.modify(String::from("eggs").into(), 3.into())
            .await
            .unwrap();

        let message = recv.next().await.unwrap();
        if let Event::Local(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::Text(String::from("milk"))).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(
                view.get(&Value::Text(String::from("eggs"))).unwrap(),
                Value::UInt32Value(3)
            );
            assert_eq!(event, MapEvent::Update(Value::Text(String::from("eggs"))));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        sender_view
            .modify(String::from("chocolate"), 10)
            .await
            .unwrap();

        let message = recv.next().await.unwrap();
        if let Event::Local(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::Text(String::from("milk"))).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(
                view.get(&Value::Text(String::from("eggs"))).unwrap(),
                Value::UInt32Value(3)
            );
            assert_eq!(
                view.get(&Value::Text(String::from("chocolate"))).unwrap(),
                Value::UInt32Value(10)
            );
            assert_eq!(
                event,
                MapEvent::Update(Value::Text(String::from("chocolate")))
            );
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_write_only_map_schema_error() {
        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);
        let mut client = SwimClient::new_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "integerMap");
        let (mut dl, _) = client.map_downlink::<i32, i32>(path).await.unwrap();

        if let Err(view_error) = dl.write_only_sender::<String, String>().await {
            assert_eq!(view_error.to_string(),  "A write-only map downlink with key schema @kind(text) was requested but the original map downlink is running with key schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.write_only_sender::<i32, String>().await {
            assert_eq!(view_error.to_string(),  "A write-only map downlink with value schema @kind(text) was requested but the original map downlink is running with value schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.write_only_sender::<i64, i32>().await {
            assert_eq!(view_error.to_string(),  "A write-only map downlink with key schema @kind(int64) was requested but the original map downlink is running with key schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }

        if let Err(view_error) = dl.write_only_sender::<i32, i64>().await {
            assert_eq!(view_error.to_string(),  "A write-only map downlink with value schema @kind(int64) was requested but the original map downlink is running with value schema @kind(int32).")
        } else {
            panic!("Expected a ViewError!")
        }
    }
}
