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
    use client::downlink::model::map::{MapModification, UntypedMapModification};
    use client::interface::SwimClient;
    use common::model::{Attr, Item, Value};
    use common::sink::item::ItemSink;
    use common::warp::path::AbsolutePath;
    use form::Form;
    use test_server::clients::Cli;
    use test_server::Docker;
    use test_server::SwimTestServer;
    use tokio::time::Duration;
    use utilities::trace;

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

    fn init_trace() {
        trace::init_trace(vec!["client::router=trace"]);
    }

    #[tokio::test]
    async fn test_send_untyped_value_command() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "publish");
        let mut command_dl = client.untyped_command_downlink(path).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;
        command_dl.send_item(13.into()).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_send_typed_value_command_valid() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "publish");
        let mut command_dl = client.command_downlink::<i32>(path).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;
        command_dl.send_item(13).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_send_untyped_map_command() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;
        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");
        let mut command_dl = client.untyped_command_downlink(path).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let insert =
            UntypedMapModification::Insert("milk".to_string().into_value(), 6.into_value())
                .as_value();

        command_dl.send_item(insert).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_send_typed_map_command() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

        let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "unit/foo", "shoppingCart");
        let mut command_dl = client
            .command_downlink::<MapModification<String, i32>>(path)
            .await
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let insert = MapModification::Insert("milk".to_string(), 6);

        command_dl.send_item(insert).await.unwrap();

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_untyped_value_event() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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
        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_valid() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_typed_value_event_invalid() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_untyped_map_event() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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
        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_valid() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_key() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_recv_typed_map_event_invalid_value() {
        init_trace();

        let docker = Cli::default();
        let container = docker.run(SwimTestServer);
        let port = container.get_host_port(9001).unwrap();
        let host = format!("ws://127.0.0.1:{}", port);

        let mut client = SwimClient::new(config()).await;

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

        tokio::time::delay_for(Duration::from_secs(3)).await;
    }
}
