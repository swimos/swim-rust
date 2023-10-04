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

use std::{collections::HashSet, error::Error, time::Duration};

use example_util::example_logging;
use swim::{route::RouteUri, server::ServerBuilder};
use tracing::debug;
use transit::{buses_api::BusesApi, create_plane, model, IncludeRoutes};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;
    let params = read_params();
    debug!(params = ?params, "Processed command line args.");
    with_params(params).await
}

fn read_params() -> HashSet<IncludeRoutes> {
    match std::env::args().next().as_deref() {
        Some("none") => HashSet::new(),
        Some("vehicles") => [IncludeRoutes::Vehicle].into_iter().collect(),
        Some("state") => [IncludeRoutes::Vehicle, IncludeRoutes::State]
            .into_iter()
            .collect(),
        _ => HashSet::new(),
    }
}

async fn with_params(params: HashSet<IncludeRoutes>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let agencies = model::agencies();
    let agency_uris = agencies
        .iter()
        .map(|a| a.uri().parse::<RouteUri>())
        .collect::<Result<Vec<_>, _>>()?;

    server_runner::run_server(agency_uris, move |api: BusesApi| async move {
        let mut builder = ServerBuilder::with_plane_name("Transit Plane");

        builder = create_plane(agencies, api, builder, params)?;

        let server = builder
            .update_config(|config| {
                config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
            })
            .build()
            .await?;
        Ok(server)
    })
    .await
}

mod server_runner {
    use std::{error::Error, future::Future, sync::Arc};
    use swim::{
        route::RouteUri,
        server::{BoxServer, Server},
    };
    use tokio::{net::TcpListener, sync::Notify};

    use tracing::info;
    use transit::{buses_api::BusesApi, start_agencies_and_wait};

    pub async fn run_server<F, Fut>(
        agency_uris: Vec<RouteUri>,
        f: F,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: FnOnce(BusesApi) -> Fut,
        Fut: Future<Output = Result<BoxServer, Box<dyn Error + Send + Sync>>>,
    {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;
        let api = BusesApi::new(format!("http://127.0.0.1:{}", addr.port()), false);

        let swim_server = f(api).await?;
        let (task, handle) = swim_server.run();

        info!(addr = %addr, "Mock service bound.");

        let trigger = Arc::new(Notify::new());
        let mock_server = tokio::spawn(transit_fixture::run_mock_server(
            listener.into_std()?,
            trigger.clone(),
        ));

        let task_with_trigger = async move {
            let result = task.await;
            trigger.notify_one();
            result
        };

        let shutdown = start_agencies_and_wait(agency_uris, handle);

        let (_, mock_result, result) = tokio::join!(shutdown, mock_server, task_with_trigger);

        result?;
        mock_result??;
        println!("Server stopped successfully.");
        Ok(())
    }
}
