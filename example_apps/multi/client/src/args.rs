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

use clap::Parser;

#[derive(Parser, Debug)] // requires `derive` feature Just to make testing across clap features easier
pub struct AppArgs {
    #[arg(short, long)]
    pub host: String,
    #[arg(short, long)]
    pub port: u16,
    #[arg(long = "link-agent")]
    pub link: bool,
    #[arg(long = "collect-agent")]
    pub collect: bool,
}
