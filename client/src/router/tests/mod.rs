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

mod host_manager;
//
// fn init_trace() {
//     INIT.call_once(|| {
//         extern crate tracing;
//
//         let filter =
//             EnvFilter::from_default_env().add_directive("client::router=trace".parse().unwrap());
//
//         let _ = tracing_subscriber::fmt()
//             .with_max_level(Level::TRACE)
//             .with_env_filter(filter)
//             .init();
//     });
// }
// use std::{env, thread, time};
//
// use common::model::Value;
// use common::sink::item::ItemSink;
// use common::warp::envelope::Envelope;
// use common::warp::path::AbsolutePath;
// use std::sync::Once;
// use tracing::Level;
// use tracing_subscriber::EnvFilter;
//
// use crate::configuration::router::{ConnectionPoolParams, RouterParamBuilder};
// use crate::connections::factory::tungstenite::TungsteniteWsFactory;
// use crate::connections::SwimConnPool;
// use crate::router::{Router, SwimRouter};
// use test_server::clients::Cli;
// use test_server::Docker;
// use test_server::SwimTestServer;
//
// static INIT: Once = Once::new();
//
// #[tokio::test(core_threads = 2)]
// // #[cfg(test_server)]
// async fn normal_receive() {
//     for v in env::vars() {
//         println!("{:?}", v);
//     }
//
//     init_trace();
//
//     let docker = Cli::default();
//     let container = docker.run(SwimTestServer);
//     let port = container.get_host_port(9001).unwrap();
//     let host = format!("ws://127.0.0.1:{}", port);
//
//     let config = RouterParamBuilder::default().build();
//     let pool = SwimConnPool::new(
//         ConnectionPoolParams::default(),
//         TungsteniteWsFactory::new(5).await,
//     );
//
//     let mut router = SwimRouter::new(config, pool);
//
//     let path = AbsolutePath::new(url::Url::parse(&host).unwrap(), "/unit/foo", "info");
//     let (mut sink, _stream) = router.connection_for(&path).await.unwrap();
//
//     let sync = Envelope::sync(String::from("/unit/foo"), String::from("info"));
//
//     sink.send_item(sync).await.unwrap();
//
//     thread::sleep(time::Duration::from_secs(5));
//     let _ = router.close().await;
//     thread::sleep(time::Duration::from_secs(5));
// }
