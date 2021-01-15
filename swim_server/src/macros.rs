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

use crate::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use crate::agent::lane::model::map::MapLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;

mod swim_server {
    pub use crate::*;
}

struct SwimAgentConfig;

macro_rules! swim_agent {
    // entry: agent with no lanes
    ( ($name: ident, $config: ident) => { } ) => {
        swim_agent!(@ { $name, $config });
    };
    // entry: agent with at least one lane
    ( ($name: ident, $config: ident) => { $($x:tt)* } ) => {
        swim_agent!(@ { $name, $config, [] [] } $($x)*,);
    };
    // value lane
    (@ { $name: ident, $config: ident, [$($element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        lane(vis: $visibility: ty, path: $path: tt, $lane_name:ident : ValueLane<$key: ident>, lifecycle: $lifecycle:ident, watch: $watch_type:ty) => {
            on_start($on_start_model:ident, $on_start_context:ident) => $on_start:block
            on_event($on_event_event:ident, $on_event_model:ident, $on_event_context:ident) => $on_event: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [$($element: $ty)* $lane_name: ValueLane<$key>]
                [
                    $($out_impl)*

                    #[crate::value_lifecycle(agent($name), event_type($key))]
                    struct $lifecycle;

                    impl $lifecycle {
                        async fn on_start<Context>(&self, $on_start_model: &crate::agent::lane::model::value::ValueLane<$key>, $on_start_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_start
                        }

                        async fn on_event<Context>(&self, $on_event_event: &std::sync::Arc<$key>, $on_event_model: &crate::agent::lane::model::value::ValueLane<$key>, $on_event_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync + 'static,
                        {
                            $on_event
                        }
                    }

                    impl LaneLifecycle<$config> for $lifecycle {
                        fn create(_config: &$config) -> Self {
                            $lifecycle {}
                        }
                    }

                    impl StatefulLaneLifecycleBase for $lifecycle {
                        type WatchStrategy = $watch_type;

                        fn create_strategy(&self) -> Self::WatchStrategy {
                            <$watch_type>::default()
                        }
                    }
                ]
            } $($remainder)*}
    };
    // map lane
    (@ { $name: ident, $config: ident, [$($element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        lane(vis: $visibility: ty, path: $path: tt, $lane_name:ident : MapLane<$key: ident, $value: ident>, lifecycle: $lifecycle:ident, watch: $watch_type:ty) => {
            on_start($on_start_model:ident, $on_start_context:ident) => $on_start:block
            on_event($on_event_event:ident, $on_event_model:ident, $on_event_context:ident) => $on_event: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [$($element: $ty)* $lane_name: MapLane<$key, $value>]
                [
                    $($out_impl)*

                    #[crate::map_lifecycle(agent($name), key_type($key), value_type($value))]
                    struct $lifecycle;

                    impl $lifecycle {
                        async fn on_start<Context>(&self, $on_start_model: &crate::agent::lane::model::map::MapLane<$key, $value>, $on_start_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_start
                        }

                        async fn on_event<Context>(&self, $on_event_event: &crate::agent::lane::model::map::MapLaneEvent<$key, $value>, $on_event_model: &crate::agent::lane::model::map::MapLane<$key, $value>, $on_event_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync + 'static,
                        {
                            $on_event
                        }
                    }

                    impl LaneLifecycle<$config> for $lifecycle {
                        fn create(_config: &$config) -> Self {
                            $lifecycle {}
                        }
                    }

                    impl StatefulLaneLifecycleBase for $lifecycle {
                        type WatchStrategy = $watch_type;

                        fn create_strategy(&self) -> Self::WatchStrategy {
                            <$watch_type>::default()
                        }
                    }
                ]
            } $($remainder)*}
    };
    // base: impl agent with no lanes
    (@ { $name: ident, $config: ident } $(,)*) => {
        #[derive(Clone, Debug)]
        struct $name;
    };
    // base: impl agent with lanes
    (@ { $name: ident, $config: ident, [$($element: ident: $ty: ty)*] [$($out_impl:tt)*]} $(,)*) => {
        #[derive(Clone, Debug)]
        struct $name {
            $(
                $element: $ty
            ),*
        }

        $($out_impl)*
    };
}

swim_agent! {
    (SwimAgent, SwimAgentConfig) => {
        lane(vis: public, path: "/something/:id", value_lane: ValueLane<String>, lifecycle: ValueLifecycle, watch: Queue) => {
            on_start(_model, _context) => {
                println!("value lane on start");
            }
            on_event(event, _model, _context) => {
                println!("value lane on event: {}", event);
            }
        }
        lane(vis: public, path: "/something/:id", map_lane: MapLane<String, i32>, lifecycle: MapLifecycle, watch: Queue) => {
            on_start(_model, _context) => {
                println!("map lane on start");
            }
            on_event(event, _model, _context) => {
                println!("map lane on event: {:?}", event);
            }
        }
    }
}
