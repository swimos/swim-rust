// Copyright 2015-2021 SWIM.AI inc.
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

mod swim_server {
    pub use crate::*;
}

#[macro_export]
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
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : ValueLane<$key: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_start($on_start_self: ident, $on_start_model:ident, $on_start_context:ident) $on_start:block
            on_event($on_event_self: ident, $on_event_event:ident, $on_event_model:ident, $on_event_context:ident) $on_event: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::value::ValueLane<$key>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::value_lifecycle", in(agent($name), event_type($key)), raw(gen_lifecycle = false, on_start, on_event))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_start<Context>($on_start_self: &Self, $on_start_model: &crate::agent::lane::model::value::ValueLane<$key>, $on_start_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_start
                        }

                        async fn on_event<Context>($on_event_self: &Self, $on_event_event: &crate::agent::lane::model::value::ValueLaneEvent<$key>, $on_event_model: &crate::agent::lane::model::value::ValueLane<$key>, $on_event_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync + 'static,
                        {
                            $on_event
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // map lane
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : MapLane<$key: ident, $value: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_start($on_start_self: ident, $on_start_model:ident, $on_start_context:ident) $on_start:block
            on_event($on_event_self: ident, $on_event_event:ident, $on_event_model:ident, $on_event_context:ident) $on_event: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::map::MapLane<$key, $value>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::map_lifecycle", in(agent($name), key_type($key), value_type($value)), raw(gen_lifecycle = false, on_start, on_event))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_start<Context>($on_start_self: &Self, $on_start_model: &crate::agent::lane::model::map::MapLane<$key, $value>, $on_start_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_start
                        }

                        async fn on_event<Context>($on_event_self: &Self, $on_event_event: &crate::agent::lane::model::map::MapLaneEvent<$key, $value>, $on_event_model: &crate::agent::lane::model::map::MapLane<$key, $value>, $on_event_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync + 'static,
                        {
                            $on_event
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // action lane
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : ActionLane<$command_type: ident, $response_type: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_command($on_command_self: ident, $on_command_event:ident, $on_command_model:ident, $on_command_context:ident) -> $response_type_ret: ty $on_command: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::action::ActionLane<$command_type, $response_type>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::action_lifecycle", in(agent($name), command_type($command_type), response_type($response_type)), raw(gen_lifecycle = false, on_command))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_command<Context>($on_command_self: &Self, $on_command_event: $command_type, $on_command_model: &crate::agent::lane::model::action::ActionLane<$command_type, $response_type>, $on_command_context: &Context)
                        -> $response_type_ret
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_command
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // command lane
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : CommandLane<$command_type: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_command($on_command_self: ident, $on_command_event:ident, $on_command_model:ident, $on_command_context:ident) $on_command: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::command::CommandLane<$command_type>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::command_lifecycle", in(agent($name), command_type($command_type)), raw(gen_lifecycle = false, on_command))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_command<Context>($on_command_self: &Self, $on_command_event: &$command_type, $on_command_model: &crate::agent::lane::model::command::CommandLane<$command_type>, $on_command_context: &Context)
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_command
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // demand lane
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : DemandLane<$cue_type: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_cue($on_cue_self: ident, $on_cue_model:ident, $on_cue_context:ident) -> $cue_type_ret:ty $on_cue: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::demand::DemandLane<$cue_type>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::demand_lifecycle", in(agent($name), event_type($cue_type)), raw(gen_lifecycle = false, on_cue))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_cue<Context>($on_cue_self: &Self, $on_cue_model: &crate::agent::lane::model::demand::DemandLane<$cue_type>, $on_cue_context: &Context)
                        -> $cue_type_ret
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_cue
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // demand map lane
    (@ { $name: ident, $config: ident, [$([$acc_visibilities:vis] $acc_lifecycles: ident $element: ident: $ty: ty)*] [$($out_impl:tt)*] }
        $visibility:vis lane(path: $path: tt, $lane_name:ident : DemandMapLane<$key_type: ident, $value_type: ident>) => {
            lifecycle: $lifecycle:ident { $($lifecycle_fields:tt)* }
            on_create($on_create_config:ident) $on_create:block
            on_cue($on_cue_self: ident, $on_cue_model:ident, $on_cue_context:ident, $on_cue_key:ident) -> $cue_type_ret:ty $on_cue: block
            on_sync($on_sync_self:ident, $on_sync_model:ident, $on_sync_context:ident) -> $sync_type_ret:ty $on_sync: block
        }
        $($remainder:tt)+) => {
            swim_agent!{@ {
                $name, $config,
                [
                    $([$acc_visibilities] $acc_lifecycles $element: $ty)*
                    [$visibility] $lifecycle $lane_name: crate::agent::lane::model::demand_map::DemandMapLane<$key_type, $value_type>
                ]
                [
                    $($out_impl)*

                    #[utilities::stringify_attr_raw(path = "crate::demand_map_lifecycle", in(agent($name), key_type($key_type), value_type($value_type)), raw(gen_lifecycle = false, on_cue, on_sync))]
                    pub struct $lifecycle {
                        $($lifecycle_fields)*
                    }

                    impl $lifecycle {
                        async fn on_cue<Context>($on_cue_self: &Self, $on_cue_model: &crate::agent::lane::model::demand_map::DemandMapLane<$key_type, $value_type>, $on_cue_context: &Context, $on_cue_key: $key_type)
                        -> $cue_type_ret
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_cue
                        }

                        async fn on_sync<Context>($on_sync_self: &Self, $on_sync_model: &crate::agent::lane::model::demand_map::DemandMapLane<$key_type, $value_type>, $on_sync_context: &Context)
                        -> $sync_type_ret
                        where
                            Context: crate::agent::AgentContext<$name> + Sized + Send + Sync,
                        {
                            $on_sync
                        }
                    }

                    impl crate::agent::lane::lifecycle::LaneLifecycle<$config> for $lifecycle {
                        fn create($on_create_config: &$config) -> Self {
                            $on_create
                        }
                    }
                ]
            } $($remainder)*}
    };
    // base: impl agent with no lanes
    (@ { $name: ident, $config: ident } $(,)*) => {
        #[derive(Clone, Debug, crate::agent::SwimAgent)]
        #[agent(config($config))]
        pub struct $name;
    };
    // base: impl agent with lanes
    (@ { $name: ident, $config: ident, [$([$visibility:vis] $lifecycle:ident $element: ident: $ty: ty)*] [$($out_impl:tt)*]} $(,)*) => {
        #[utilities::stringify_attr(agent(config($config)))]
        #[derive(Clone, Debug, crate::agent::SwimAgent)]
        pub struct $name {
            $(
                #[stringify(lifecycle(name($lifecycle)))]
                $visibility $element: $ty
            ),*
        }

        $($out_impl)*
    };
}
