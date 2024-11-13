// Copyright 2015-2024 Swim Inc.
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

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Params {
    /// Specify a Recon configuration file for the connector.
    #[arg(long)]
    pub config: Option<String>,
    #[arg(long, default_value = "false")]
    /// Specify that logging should be enabled.
    pub enable_logging: bool,
    /// Specify the MQTT broker (used if --config is not specified.)
    #[arg(short, long)]
    pub broker_url: Option<String>,
}
