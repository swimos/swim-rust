// Copyright 2015-2021 Swim Inc.
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

mod tests {
    use std::sync::Arc;
    use swim_client::downlink::model::map::{MapEvent, MapModification, UntypedMapModification};
    use swim_client::downlink::typed::map::events::TypedViewWithEvent;
    use swim_client::downlink::Event;
    use swim_client::interface::SwimClientBuilder;
    use swim_form::Form;
    use swim_model::path::{AbsolutePath, RelativePath};
    use swim_model::{Attr, Item, Value};
    use test_server::build_server;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_value_dl_recv() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "id");

        let (_dl, mut recv) = client.value_downlink::<i32>(path.clone(), 0).await.unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote(0));
    }

    #[tokio::test]
    async fn test_value_dl_send() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "id");

        let (dl, mut recv) = client.value_downlink(path.clone(), 0).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        dl.set(10).await.unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote(0));

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Local(10));
    }

    #[tokio::test]
    async fn test_map_dl_recv() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;
        let path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let (_dl, mut recv) = client
            .map_downlink::<String, i32>(path.clone())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let message = recv.recv().await.unwrap();

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
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;
        let path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let (dl, mut recv) = client
            .map_downlink::<String, i32>(path.clone())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        dl.update_and_forget("milk".to_owned(), 1).await.unwrap();

        let message = recv.recv().await.unwrap();

        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.len(), 0);
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let message = recv.recv().await.unwrap();

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
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");
        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

        let event_dl = client.untyped_event_downlink(event_path).await.unwrap();
        let mut rec = event_dl.subscribe().unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl.send("Hello, from Rust!".into()).await.unwrap();

        let incoming = rec.recv().await.unwrap().clone().get_inner();

        assert_eq!(incoming, Value::text("Hello, from Rust!"));
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_valid() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");
        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

        let event_dl = client
            .event_downlink::<String>(event_path, Default::default())
            .await
            .unwrap();

        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client
            .command_downlink::<String>(command_path)
            .await
            .unwrap();
        command_dl
            .command("Hello, from Rust!".to_string())
            .await
            .unwrap();

        let incoming = rec.recv().await.unwrap();

        assert_eq!(incoming, "Hello, from Rust!");
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_invalid() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");
        let command_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

        let event_dl = client
            .event_downlink::<i32>(event_path, Default::default())
            .await
            .unwrap();

        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl.send("Hello, from Rust!".into()).await.unwrap();

        let incoming = rec.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_recv_untyped_map_event() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let event_dl = client.untyped_event_downlink(event_path).await.unwrap();
        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send(
                UntypedMapModification::Update(
                    "milk".to_string().into_value(),
                    Arc::new(6.into_value()),
                )
                .as_value(),
            )
            .await
            .unwrap();

        let incoming = rec.recv().await.unwrap().clone().get_inner();

        let header = Attr::of(("update", Value::record(vec![Item::slot("key", "milk")])));
        let body = Item::of(6u32);
        let expected = Value::Record(vec![header], vec![body]);

        assert_eq!(incoming, expected);
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_valid() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let event_dl = client
            .event_downlink::<MapModification<String, i32>>(event_path, Default::default())
            .await
            .unwrap();

        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client
            .command_downlink::<MapModification<String, i32>>(command_path)
            .await
            .unwrap();

        let item = MapModification::Update("milk".to_string(), Arc::new(6i32));

        command_dl.command(item).await.unwrap();

        let incoming = rec.recv().await.unwrap();

        assert_eq!(
            incoming,
            MapModification::Update("milk".to_string(), Arc::new(6i32))
        );
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_key() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let event_dl = client
            .event_downlink::<MapModification<i32, i32>>(event_path, Default::default())
            .await
            .unwrap();

        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send(
                UntypedMapModification::Update(
                    "milk".to_string().into_value(),
                    Arc::new(6.into_value()),
                )
                .as_value(),
            )
            .await
            .unwrap();

        let incoming = rec.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_value() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let event_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let event_dl = client
            .event_downlink::<MapModification<String, String>>(event_path, Default::default())
            .await
            .unwrap();

        let mut rec = event_dl.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let command_dl = client.untyped_command_downlink(command_path).await.unwrap();
        command_dl
            .send(
                UntypedMapModification::Update(
                    "milk".to_string().into_value(),
                    Arc::new(6.into_value()),
                )
                .as_value(),
            )
            .await
            .unwrap();

        let incoming = rec.recv().await;

        assert_eq!(incoming, None);
    }

    #[tokio::test]
    async fn test_read_only_value() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

        let command_dl = client
            .command_downlink::<String>(path.clone())
            .await
            .unwrap();

        command_dl
            .command("Hello, String!".to_string())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (dl, mut recv) = client.value_downlink(path, String::new()).await.unwrap();
        let sub = dl.subscriber().covariant_cast::<Value>().unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote(String::from("Hello, String!")));

        let mut recv_view = sub.subscribe().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        command_dl
            .command("Hello, Value!".to_string())
            .await
            .unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote(String::from("Hello, Value!")));

        let message = recv_view.recv().await.unwrap();
        assert_eq!(
            message,
            Event::Remote(Value::from("Hello, Value!".to_string()))
        );
    }

    #[tokio::test]
    async fn test_read_only_value_schema_error() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "id");
        let (dl, _rec) = client.value_downlink(path.clone(), 0i64).await.unwrap();

        assert!(dl.subscriber().covariant_cast::<String>().is_err());
        assert!(dl.subscriber().covariant_cast::<i32>().is_err());
    }

    #[tokio::test]
    async fn test_read_only_map() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_dl = client
            .command_downlink::<MapModification<String, i32>>(path.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        command_dl
            .command(MapModification::Update("milk".to_string(), Arc::new(1)))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (dl, mut recv) = client.map_downlink::<String, i32>(path).await.unwrap();

        let message = recv.recv().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.get(&String::from("milk")).unwrap(), 1);
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let mut recv_view = dl
            .subscriber()
            .covariant_cast::<Value, Value>()
            .unwrap()
            .subscribe()
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        command_dl
            .command(MapModification::Update("eggs".to_string(), Arc::new(2)))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let message = recv.recv().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(view.get(&String::from("milk")).unwrap(), 1);
            assert_eq!(view.get(&String::from("eggs")).unwrap(), 2);
            assert_eq!(event, MapEvent::Update(String::from("eggs")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let message = recv_view.recv().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::text("milk")).unwrap(),
                Value::UInt32Value(1)
            );
            assert_eq!(
                view.get(&Value::text("eggs")).unwrap(),
                Value::UInt32Value(2)
            );
            assert_eq!(event, MapEvent::Update(Value::text("eggs")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_read_only_map_schema_error() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "integer_map");

        let (dl, _rec) = client.map_downlink::<i64, i64>(path).await.unwrap();

        assert!(dl.subscriber().covariant_cast::<String, String>().is_err());
        assert!(dl.subscriber().covariant_cast::<i64, String>().is_err());
        assert!(dl.subscriber().covariant_cast::<i32, i64>().is_err());
        assert!(dl.subscriber().covariant_cast::<i64, i32>().is_err());
    }

    #[tokio::test]
    async fn test_write_only_value() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");

        let command_dl = client
            .command_downlink::<String>(path.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;
        command_dl.command(String::from("milk")).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let (dl, mut recv) = client.value_downlink(path, Value::Extant).await.unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote(Value::text("milk")));

        let sender_view = dl.sender().contravariant_view::<String>().unwrap();

        dl.set(String::from("bread").into()).await.unwrap();
        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Local(Value::text("bread")));

        sender_view.set(String::from("chocolate")).await.unwrap();
        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Local(Value::text("chocolate")));
    }

    #[tokio::test]
    async fn test_write_only_value_schema_error() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "id");
        let (dl, _rec) = client.value_downlink(path.clone(), 0i32).await.unwrap();

        assert!(dl.sender().contravariant_view::<String>().is_err());
        assert!(dl.sender().contravariant_view::<i64>().is_err());
    }

    #[tokio::test]
    async fn test_write_only_map() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(
            url::Url::parse(&host).unwrap(),
            "/unit/foo",
            "shopping_cart",
        );

        let command_dl = client
            .command_downlink::<MapModification<String, i32>>(path.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        command_dl
            .command(MapModification::Update(String::from("milk"), 5.into()))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (dl, mut recv) = client.map_downlink::<Value, Value>(path).await.unwrap();

        let message = recv.recv().await.unwrap();
        if let Event::Remote(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::text("milk")).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(event, MapEvent::Initial);
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        let sender_view = dl.sender().contravariant_view::<String, i32>().unwrap();

        dl.update(String::from("eggs").into(), 3.into())
            .await
            .unwrap();

        let message = recv.recv().await.unwrap();
        if let Event::Local(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::text("milk")).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(
                view.get(&Value::text("eggs")).unwrap(),
                Value::UInt32Value(3)
            );
            assert_eq!(event, MapEvent::Update(Value::text("eggs")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }

        sender_view
            .update(String::from("chocolate"), 10)
            .await
            .unwrap();

        let message = recv.recv().await.unwrap();
        if let Event::Local(event) = message {
            let TypedViewWithEvent { view, event } = event;

            assert_eq!(
                view.get(&Value::text("milk")).unwrap(),
                Value::UInt32Value(5)
            );
            assert_eq!(
                view.get(&Value::text("eggs")).unwrap(),
                Value::UInt32Value(3)
            );
            assert_eq!(
                view.get(&Value::text("chocolate")).unwrap(),
                Value::UInt32Value(10)
            );
            assert_eq!(event, MapEvent::Update(Value::text("chocolate")));
        } else {
            panic!("The map downlink did not receive the correct message!")
        }
    }

    #[tokio::test]
    async fn test_write_only_map_schema_error() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();

        let host = format!("warp://127.0.0.1:{}", port);
        let client = SwimClientBuilder::build_with_default().await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "integer_map");
        let (dl, _) = client.map_downlink::<i32, i32>(path).await.unwrap();

        assert!(dl.sender().contravariant_view::<String, String>().is_err());
        assert!(dl.sender().contravariant_view::<i32, String>().is_err());
        assert!(dl.sender().contravariant_view::<i64, i32>().is_err());
        assert!(dl.sender().contravariant_view::<i32, i64>().is_err());
    }

    // todo: failing test
    #[tokio::test]
    #[ignore]
    async fn test_server_dl_between_lanes_same_agent() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();
        let host = format!("warp://127.0.0.1:{}", port);

        let client = SwimClientBuilder::build_with_default().await;

        let park_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "park");
        let garage_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "garage");

        let (dl, mut recv) = client
            .value_downlink(garage_path, "".to_string())
            .await
            .unwrap();

        dl.set("VW".to_string()).await.unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        client
            .command_downlink::<String>(park_path)
            .await
            .unwrap()
            .command("BMW".to_string())
            .await
            .unwrap();

        let message = recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW BMW".to_string()));
    }

    // todo: failing test
    #[tokio::test]
    #[ignore]
    async fn test_server_dl_between_lanes_different_agents() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();
        let host = format!("warp://127.0.0.1:{}", port);

        let client = SwimClientBuilder::build_with_default().await;

        let park_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "park");
        let first_garage_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "garage");
        let second_garage_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/2", "garage");

        let (first_dl, mut first_recv) = client
            .value_downlink(first_garage_path, "".to_string())
            .await
            .unwrap();
        first_dl.set("VW".to_string()).await.unwrap();

        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        let (second_dl, mut second_recv) = client
            .value_downlink(second_garage_path, "".to_string())
            .await
            .unwrap();
        second_dl.set("VW".to_string()).await.unwrap();

        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        client
            .command_downlink::<String>(park_path)
            .await
            .unwrap()
            .command("Audi".to_string())
            .await
            .unwrap();

        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Audi".to_string()));

        let message = first_dl.get().await.unwrap();
        assert_eq!(message, "VW".to_string())
    }

    #[tokio::test]
    async fn test_server_dl_between_lanes_remote() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let first_port = server_handle.address().await.unwrap().port();
        let first_host = format!("warp://127.0.0.1:{}", first_port);

        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let second_port = server_handle.address().await.unwrap().port();
        let second_host = format!("warp://127.0.0.1:{}", second_port);

        let client = SwimClientBuilder::build_with_default().await;

        let park_path =
            AbsolutePath::new(url::Url::parse(&first_host).unwrap(), "/downlink/1", "park");
        let first_garage_path = AbsolutePath::new(
            url::Url::parse(&first_host).unwrap(),
            "/downlink/1",
            "garage",
        );

        let second_garage_path = AbsolutePath::new(
            url::Url::parse(&second_host).unwrap(),
            "/downlink/1",
            "garage",
        );

        let (first_dl, mut first_recv) = client
            .value_downlink(first_garage_path, "".to_string())
            .await
            .unwrap();
        first_dl.set("VW".to_string()).await.unwrap();

        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = first_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        let (second_dl, mut second_recv) = client
            .value_downlink(second_garage_path, "".to_string())
            .await
            .unwrap();
        second_dl.set("VW".to_string()).await.unwrap();

        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        client
            .command_downlink::<String>(park_path)
            .await
            .unwrap()
            .command(format!("Toyota {}", second_port))
            .await
            .unwrap();

        let message = second_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Toyota".to_string()));

        let message = first_dl.get().await.unwrap();
        assert_eq!(message, "VW".to_string())
    }

    // todo: failing test
    #[tokio::test]
    #[ignore]
    async fn test_server_dl_from_server_to_lane_local() {
        let (server, mut server_handle) = build_server().await;
        let client_context = server.client_context();
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();
        let host = format!("warp://127.0.0.1:{}", port);

        let client = SwimClientBuilder::build_with_default().await;

        let garage_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "garage");

        let (client_dl, mut client_recv) = client
            .value_downlink(garage_path, "".to_string())
            .await
            .unwrap();
        client_dl.set("VW".to_string()).await.unwrap();

        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        let (server_dl, mut server_recv) = client_context
            .value_downlink(
                RelativePath::new("/downlink/1", "garage").into(),
                "".to_string(),
            )
            .await
            .unwrap();

        server_dl
            .set(format!("{} {}", server_dl.get().await.unwrap(), "Honda"))
            .await
            .unwrap();

        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));
        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW Honda".to_string()));
        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Honda".to_string()));

        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Honda".to_string()));

        let message = client_dl.get().await.unwrap();
        assert_eq!(message, "VW Honda".to_string())
    }

    #[tokio::test]
    async fn test_server_dl_from_server_to_lane_remote() {
        let (server, mut server_handle) = build_server().await;
        let client_context = server.client_context();
        tokio::spawn(server.run());
        let second_port = server_handle.address().await.unwrap().port();
        let second_host = format!("warp://127.0.0.1:{}", second_port);

        let client = SwimClientBuilder::build_with_default().await;

        let garage_path = AbsolutePath::new(
            url::Url::parse(&second_host).unwrap(),
            "/downlink/1",
            "garage",
        );

        let (client_dl, mut client_recv) = client
            .value_downlink(garage_path, "".to_string())
            .await
            .unwrap();
        client_dl.set("VW".to_string()).await.unwrap();

        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW".to_string()));
        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));

        let (server_dl, mut server_recv) = client_context
            .value_downlink(
                AbsolutePath::new(
                    url::Url::parse(&second_host).unwrap(),
                    "/downlink/1",
                    "garage",
                )
                .into(),
                "".to_string(),
            )
            .await
            .unwrap();

        server_dl
            .set(format!("{} {}", server_dl.get().await.unwrap(), "Volvo"))
            .await
            .unwrap();

        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW".to_string()));
        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Local("VW Volvo".to_string()));
        let message = server_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Volvo".to_string()));

        let message = client_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("VW Volvo".to_string()));

        let message = client_dl.get().await.unwrap();
        assert_eq!(message, "VW Volvo".to_string())
    }

    #[tokio::test]
    async fn test_remote_and_local_dl_to_command_lane() {
        let (server, mut server_handle) = build_server().await;
        tokio::spawn(server.run());
        let port = server_handle.address().await.unwrap().port();
        let host = format!("warp://127.0.0.1:{}", port);

        let client = SwimClientBuilder::build_with_default().await;

        let garage_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "garage");

        let park_path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "park");

        let self_park_path =
            AbsolutePath::new(url::Url::parse(&host).unwrap(), "/downlink/1", "self_park");

        let (_garage_downlink, mut garage_recv) = client
            .value_downlink(garage_path, "".to_string())
            .await
            .unwrap();

        let park_downlink = client
            .event_downlink::<String>(park_path.clone(), Default::default())
            .await
            .unwrap();

        let mut park_downlink_recv = park_downlink.subscribe().unwrap();

        client
            .send_command(park_path, "Kia".to_string())
            .await
            .unwrap();

        let message = garage_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("".to_string()));
        let message = garage_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("Kia".to_string()));

        client
            .send_command(self_park_path, "Mercedes".to_string())
            .await
            .unwrap();

        let message = garage_recv.recv().await.unwrap();
        assert_eq!(message, Event::Remote("Kia Mercedes".to_string()));

        let message = park_downlink_recv.recv().await.unwrap();
        assert_eq!(message, "Kia");
        let message = park_downlink_recv.recv().await.unwrap();
        assert_eq!(message, "Mercedes");
    }
}
