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

pub mod downlink {
    use std::num::NonZeroUsize;
    use tokio::time::Duration;
    use std::collections::HashMap;
    use common::warp::path::AbsolutePath;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum MuxMode {
        Queue(NonZeroUsize),
        Dropping,
        Buffered(NonZeroUsize),
    }


    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct DownlinkParams {
        pub back_pressure: bool,
        pub mux_mode: MuxMode,
        pub idle_timout: Duration,
        pub buffer_size: NonZeroUsize,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct ClientParams {
        pub dl_req_buffer_size: NonZeroUsize,
    }

    pub trait Config : Send {

        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams;

        fn client_params(&self) -> ClientParams;

        fn duplicate(&self) -> Box<dyn Config>;

    }

    #[derive(Clone, Debug)]
    pub struct ConfigHierarchy {
        client_params: ClientParams,
        default: DownlinkParams,
        by_host: HashMap<String, DownlinkParams>,
        by_lane: HashMap<AbsolutePath, DownlinkParams>,
    }

    impl ConfigHierarchy {

        pub fn new(client_params: ClientParams, default: DownlinkParams) -> ConfigHierarchy {
            ConfigHierarchy {
                client_params,
                default,
                by_host: HashMap::new(),
                by_lane: HashMap::new(),
            }
        }

        pub fn for_host(&mut self, host: &str, params: DownlinkParams) {
            self.by_host.insert(host.to_string(), params);
        }

        pub fn for_lane(&mut self, lane: &AbsolutePath, params: DownlinkParams) {
            self.by_lane.insert(lane.clone(), params);
        }
    }

    impl Config for ConfigHierarchy {
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams {
            let ConfigHierarchy {
                default, by_host, by_lane,..
            } = self;
            match by_lane.get(path) {
                Some(params) => *params,
                _ => match by_host.get(&path.host) {
                    Some(params) => *params,
                    _ => *default
                }
            }
        }

        fn client_params(&self) -> ClientParams {
            self.client_params
        }

        fn duplicate(&self) -> Box<dyn Config> {
            Box::new(self.clone())
        }
    }

    impl<'a> Config for Box<dyn Config + 'a> {
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams {
            (**self).config_for(path)
        }

        fn client_params(&self) -> ClientParams {
            (**self).client_params()
        }

        fn duplicate(&self) -> Box<dyn Config> {
           (**self).duplicate()
        }
    }

}