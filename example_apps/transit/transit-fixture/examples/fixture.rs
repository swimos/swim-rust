// Copyright 2015-2023 Swim Inc.
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

use std::sync::Arc;

use futures::future::join;
use tokio::{net::TcpListener, sync::Notify};
use transit_fixture::agency::mock_agencies;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let trigger = Arc::new(Notify::new());

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    println!("Listening on: {}", addr);
    let mock_server = transit_fixture::run_mock_server(mock_agencies(), listener.into_std()?, trigger.clone());

    let shutdown = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Registering CTRL-C handler failed.");
        trigger.notify_one();
    };

    let (result, _) = join(mock_server, shutdown).await;
    result.expect("Server failed.");
    println!("Server stopped cleanly.");
    Ok(())
}
