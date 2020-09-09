#[cfg(test)]
mod t {
    use crate::configuration::downlink::{ClientParams, ConfigHierarchy};
    use crate::configuration::router::RouterParamBuilder;
    use crate::connections::factory::tungstenite::{CompressionConfig, HostConfig};
    use crate::downlink::{Downlink, Event};
    use crate::interface::SwimClient;
    use futures_util::core_reexport::num::NonZeroUsize;
    use std::collections::HashMap;
    use std::env;
    use swim_common::warp::path::AbsolutePath;
    use swim_common::ws::Protocol;
    use tokio::stream::StreamExt;
    use tokio::time::Duration;
    use tokio_tungstenite::tungstenite::extensions::deflate::DeflateConfig;
    use tracing::Level;
    use utilities::future::retryable::strategy::RetryStrategy;

    #[tokio::test]
    async fn test_value_dl_recv() {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            // .with_env_filter(filter)
            .init();

        let host = format!("warp://127.0.0.1:{}", 9001);
        let url = url::Url::parse(&host).unwrap();
        let mut protos = HashMap::new();

        println!("{:?}", env::current_dir());

        protos.insert(
            url.clone(),
            HostConfig {
                protocol: Protocol::tls("../certificate.cert").unwrap(),
                compression_config: CompressionConfig::Deflate(DeflateConfig::default()),
            },
        );

        let config = ConfigHierarchy::new(
            ClientParams::new(
                NonZeroUsize::new(10).unwrap(),
                RouterParamBuilder::new()
                    .with_retry_stategy(RetryStrategy::none())
                    .build(),
            ),
            Default::default(),
        );

        let mut client = SwimClient::config_with_certs(config, protos).await;

        let path = AbsolutePath::new(url, "unit/foo", "id");

        let (downlink, mut recv) = client.value_downlink(path.clone(), 0).await.unwrap();
        tokio::time::delay_for(Duration::from_secs(1)).await;

        let (_topic, mut sink) = downlink.split();
        let val = 100;
        sink.set(100).await.unwrap();

        let message = recv.next().await.unwrap();

        assert_eq!(message, Event::Remote(val));
    }
}
