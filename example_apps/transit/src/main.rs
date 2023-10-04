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

use std::{error::Error, time::Duration};

use clap::Parser;

use swim::{route::RouteUri, server::ServerBuilder};
use transit::{buses_api::BusesApi, configure_logging, create_plane, IncludeRoutes};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let params = Params::parse();
    if params.enable_logging {
        configure_logging()?;
    }
    let agencies = transit::model::agencies();
    let agency_uris = agencies
        .iter()
        .map(|a| a.uri().parse::<RouteUri>())
        .collect::<Result<Vec<_>, _>>()?;

    server_runner::run_server(agency_uris, move |api: BusesApi| async move {
        let mut builder = ServerBuilder::with_plane_name("Transit Plane");

        builder = create_plane(agencies, api, builder, IncludeRoutes::all())?;

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
    use std::error::Error;
    use std::future::Future;
    use swim::{
        route::RouteUri,
        server::{BoxServer, Server},
    };

    use transit::{buses_api::BusesApi, start_agencies_and_wait};

    pub async fn run_server<F, Fut>(
        agency_uris: Vec<RouteUri>,
        f: F,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        F: FnOnce(BusesApi) -> Fut,
        Fut: Future<Output = Result<BoxServer, Box<dyn Error + Send + Sync>>>,
    {
        let swim_server = f(BusesApi::default()).await?;
        let (task, handle) = swim_server.run();

        let shutdown = start_agencies_and_wait(agency_uris, handle);

        let (_, result) = tokio::join!(shutdown, task);

        result?;
        println!("Server stopped successfully.");
        Ok(())
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Params {
    /// Switch on logging to the console.
    #[arg(long)]
    enable_logging: bool,
}
