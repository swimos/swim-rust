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
/*
use swim_runtime::time::clock::Clock;
use crate::plane::{AgentRoute, EnvChannel};
use futures::future::BoxFuture;
use crate::agent::lane::channels::AgentExecutionConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::any::Any;
use crate::agent::AgentResult;
use crate::plane::router::PlaneRouter;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::context::PlaneContext;

#[derive(Debug)]
struct PingRoute {

}

#[derive(Debug)]
struct PongRoute {

}

#[derive(Debug)]
struct TestLifecycle {

}

impl<Clk: Clock> AgentRoute<Clk, EnvChannel, PlaneRouter> for PingRoute {
    fn run_agent(&self,
                 uri: String,
                 parameters: HashMap<String, String>,
                 execution_config: AgentExecutionConfig,
                 clock: Clk,
                 incoming_envelopes: EnvChannel,
                 router: PlaneRouter) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        unimplemented!()
    }
}

impl<Clk: Clock> AgentRoute<Clk, EnvChannel, PlaneRouter> for PongRoute {
    fn run_agent(&self,
                 uri: String,
                 parameters: HashMap<String, String>,
                 execution_config: AgentExecutionConfig,
                 clock: Clk,
                 incoming_envelopes: EnvChannel,
                 router: PlaneRouter) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        unimplemented!()
    }
}

impl PlaneLifecycle for TestLifecycle {
    fn on_start<'a>(&'a mut self, context: &'a dyn PlaneContext) -> BoxFuture<'a, ()> {
        unimplemented!()
    }

    fn on_stop(&mut self) -> BoxFuture<()> {
        unimplemented!()
    }
}*/
