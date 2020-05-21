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

use std::collections::HashMap;

pub use testcontainers::*;

#[derive(Default)]
pub struct SwimTestServer;

const IMAGE: &'static str = "org.swimos/swim-test-server:1.1";

impl Image for SwimTestServer {
    type Args = Vec<String>;
    type EnvVars = HashMap<String, String>;
    type Volumes = HashMap<String, String>;

    fn descriptor(&self) -> String {
        String::from(IMAGE)
    }

    fn wait_until_ready<D: Docker>(&self, container: &Container<D, Self>) {
        container
            .logs()
            .stdout
            .wait_for_message("Running Basic server...")
            .expect("Failed to start Docker container");
    }

    fn args(&self) -> <Self as Image>::Args {
        vec![String::from("-p 9001:9001")]
    }

    fn env_vars(&self) -> Self::EnvVars {
        HashMap::new()
    }

    fn volumes(&self) -> Self::Volumes {
        HashMap::new()
    }

    fn with_args(self, mut arguments: <Self as Image>::Args) -> Self {
        arguments.push("-p 9001:9001".into());
        self
    }
}
