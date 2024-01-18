#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::sync::Arc;

    use futures_util::future::BoxFuture;
    use futures_util::{join, FutureExt, SinkExt};
    use tokio::sync::watch;
    use tokio_util::codec::FramedWrite;

    use swim_api::agent::{Agent, AgentContext, HttpLaneRequestChannel, LaneConfig};
    use swim_api::downlink::DownlinkKind;
    use swim_api::error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError};
    use swim_api::lane::WarpLaneKind;
    use swim_api::protocol::agent::{LaneRequest, LaneRequestEncoder};
    use swim_api::store::StoreKind;
    use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
    use swim_utilities::non_zero_usize;
    use swim_utilities::routing::route_uri::RouteUri;
    use swim_wasm_host::runtime::wasm::WasmModuleRuntime;
    use swim_wasm_host::wasm::{Config, Engine, Linker};
    use swim_wasm_host::WasmAgentModel;
    use wasm_ir::{LaneKindRepr, LaneSpec};
    use wasm_test_fixture::channels::{channels, ChannelsSender};

    const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(128);

    #[derive(Debug)]
    pub struct TestAgentContext {
        lane_specs: HashMap<String, LaneSpec>,
        sender: Arc<ChannelsSender>,
    }

    impl TestAgentContext {
        pub fn new(
            lane_specs: HashMap<String, LaneSpec>,
            sender: ChannelsSender,
        ) -> TestAgentContext {
            TestAgentContext {
                lane_specs,
                sender: Arc::new(sender),
            }
        }
    }

    impl AgentContext for TestAgentContext {
        fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
            unimplemented!()
        }

        fn add_lane(
            &self,
            name: &str,
            lane_kind: WarpLaneKind,
            config: LaneConfig,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
            let key = name.to_string();
            let TestAgentContext { lane_specs, sender } = self;
            let spec = lane_specs.get(&key).expect("Unknown lane");

            assert_eq!(spec.is_transient, config.transient);

            match (spec.lane_kind_repr, lane_kind) {
                (LaneKindRepr::Value, WarpLaneKind::Value) => {}
                (LaneKindRepr::Command, WarpLaneKind::Command) => {}
                (LaneKindRepr::Demand, WarpLaneKind::Demand) => {}
                (LaneKindRepr::DemandMap, WarpLaneKind::DemandMap) => {}
                (LaneKindRepr::Map, WarpLaneKind::Map) => {}
                (LaneKindRepr::JoinMap, WarpLaneKind::JoinMap) => {}
                (LaneKindRepr::JoinValue, WarpLaneKind::JoinValue) => {}
                (LaneKindRepr::Supply, WarpLaneKind::Supply) => {}
                (LaneKindRepr::Spatial, WarpLaneKind::Spatial) => {}
                (l, r) => panic!("Unexpected lane kind ({l:?} != {r:?})"),
            };

            let sender = sender.clone();

            async move {
                let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
                let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);

                if lane_kind.map_like() {
                    sender.push_map_channel(tx_in, rx_out, key).await;
                } else {
                    sender.push_value_channel(tx_in, rx_out, key).await;
                }

                Ok((tx_out, rx_in))
            }
            .boxed()
        }

        fn add_http_lane(
            &self,
            _name: &str,
        ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
            unimplemented!()
        }

        fn open_downlink(
            &self,
            _host: Option<&str>,
            _node: &str,
            _lane: &str,
            _kind: DownlinkKind,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
            unimplemented!()
        }

        fn add_store(
            &self,
            _name: &str,
            _kind: StoreKind,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
            unimplemented!()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn run() {
        let file = "/Users/sircipher/Desktop/work/swim-rust/target/wasm32-unknown-unknown/debug/simple_guest.wasm";

        let mut config = Config::new();
        config.async_support(true);

        let engine = Engine::new(&config).unwrap();
        let runtime = WasmModuleRuntime::from_file(&engine, Linker::new(&engine), file).unwrap();
        let (_changes_tx, changes_rx) = watch::channel(runtime.clone());
        let model = WasmAgentModel::new(changes_rx);

        let (sender, mut receiver) = channels();
        let specs = HashMap::from([
            (
                "lane".to_string(),
                LaneSpec::new(true, 0, LaneKindRepr::Value),
            ),
            (
                "twice".to_string(),
                LaneSpec::new(true, 1, LaneKindRepr::Value),
            ),
        ]);

        let agent_task = async move {
            model
                .run(
                    RouteUri::from_str("agent").unwrap(),
                    Default::default(),
                    Default::default(),
                    Box::new(TestAgentContext::new(specs, sender)),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        };

        let task = async move {
            for i in 0..1000 {
                receiver
                    .with_writer("lane", |mut writer| async {
                        let mut encoder =
                            FramedWrite::new(&mut writer, LaneRequestEncoder::value());
                        encoder
                            .send(LaneRequest::Command(i.to_string()))
                            .await
                            .unwrap();
                        (writer, ())
                    })
                    .await;

                receiver
                    .expect_value_event("lane", i.to_string().as_str())
                    .await;
                receiver
                    .expect_value_event("twice", (i * 2).to_string().as_str())
                    .await;
            }

            receiver.clear().await;
        };

        join!(agent_task, task);
    }
}
