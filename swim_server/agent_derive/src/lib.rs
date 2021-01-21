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

//! Agent derive is a library for creating swim agents and lifecycles for them and their lanes, by
//! annotating structs and asynchronous functions.
//!
//! The minimum requirements for creating lifecycles is to provide the name of the swim agent for
//! which they will be used, the input/output types of the lanes that they will be applied to, and
//! the corresponding lifecycles functions.
//!
//! It is also possible to provide a configuration struct for the swim agent.
//!
//! # Example
//! Creating a custom swim agent with a single command lane and a configuration struct.
//!
//! ```rust
//! use swim_server::agent::AgentContext;
//! use swim_server::agent::lane::model::action::CommandLane;
//! use swim_server::agent::lane::lifecycle::LaneLifecycle;
//! use swim_server::{command_lifecycle, SwimAgent};
//!
//! // ----------------------- Agent derivation -----------------------
//!
//! #[derive(Debug, SwimAgent)]
//! #[agent(config = "TestAgentConfig")]
//! pub struct TestAgent {
//!     #[lifecycle(name = "TestCommandLifecycle")]
//!     pub command: CommandLane<String>,
//! }
//!
//! #[derive(Debug)]
//! pub struct TestAgentConfig;
//!
//! // ----------------------- Command Lifecycle -----------------------
//!
//! #[command_lifecycle(
//!     agent = "TestAgent",
//!     command_type = "String"
//! )]
//! struct TestCommandLifecycle;
//!
//! impl TestCommandLifecycle {
//!     async fn on_command<Context>(
//!         &self,
//!         command: String,
//!         _model: &CommandLane<String>,
//!         _context: &Context,
//!     ) where
//!         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
//!     {
//!         println!("Command received: {}", command);
//!     }
//! }
//!
//! impl LaneLifecycle<TestAgentConfig> for TestCommandLifecycle {
//!     fn create(_config: &TestAgentConfig) -> Self {
//!         TestCommandLifecycle {}
//!     }
//! }
//! ```

use crate::agent::{derive_agent_lifecycle, derive_swim_agent};
use crate::lanes::action::derive_action_lifecycle;
use crate::lanes::command::derive_command_lifecycle;
use crate::lanes::map::derive_map_lifecycle;
use crate::lanes::value::derive_value_lifecycle;

use crate::lanes::demand::derive_demand_lifecycle;
use crate::lanes::demand_map::derive_demand_map_lifecycle;
use crate::utils::derive;
use macro_helpers::as_const;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod agent;
mod lanes;
mod utils;

/// A derive attribute for creating swim agents.
///
/// If the swim agent has configuration, it can be provided from the `config` attribute of the
/// `agent` annotation.
///
/// If the swim agent has lanes, they can be annotated with the appropriate lifecycle attributes
/// which require a correct lifecycle struct to be provided.
///
/// The lifecycles are private by default, and can be made public with the additional `public`
/// attribute.
///
/// # Example
/// Minimal swim agent without any lanes or configuration.
///
/// ```rust
/// use swim_server::SwimAgent;
///
/// #[derive(Debug, SwimAgent)]
/// pub struct TestAgent;
/// ```
///
/// Swim agent with multiple lanes of different types and custom configuration.
///
/// ```rust
/// use swim_server::SwimAgent;
/// use swim_server::agent::lane::model::action::{ActionLane, CommandLane};
/// use swim_server::agent::lane::model::map::MapLane;
/// use swim_server::agent::lane::model::value::ValueLane;
/// # use std::sync::Arc;
/// # use swim_server::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
/// # use swim_server::agent::lane::model::map::MapLaneEvent;
/// # use swim_server::agent::lane::strategy::Queue;
/// # use swim_server::agent::AgentContext;
/// # use swim_server::{action_lifecycle, command_lifecycle, map_lifecycle, value_lifecycle};
///
/// #[derive(Debug, SwimAgent)]
/// #[agent(config = "TestAgentConfig")]
/// pub struct TestAgent {
///     #[lifecycle(name = "TestCommandLifecycle")]
///     command: CommandLane<String>,
///     // This is public.
///     #[lifecycle(name = "TestActionLifecycle")]
///     pub action: ActionLane<String, i32>,
///     #[lifecycle(name = "TestValueLifecycle")]
///     value: ValueLane<i32>,
///     #[lifecycle(name = "TestMapLifecycle")]
///     map: MapLane<String, i32>,
/// }
///
/// pub struct TestAgentConfig;
/// #
/// # #[command_lifecycle(
/// #     agent = "TestAgent",
/// #     command_type = "String",
/// #     on_command = "on_command"
/// # )]
/// # struct TestCommandLifecycle;
/// #
/// # impl TestCommandLifecycle {
/// #     async fn on_command<Context>(
/// #         &self,
/// #         command: String,
/// #         _model: &CommandLane<String>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #        println!("Command received: {}", command);
/// #     }
/// # }
/// #
/// # impl LaneLifecycle<TestAgentConfig> for TestCommandLifecycle {
/// #     fn create(_config: &TestAgentConfig) -> Self {
/// #         TestCommandLifecycle {}
/// #     }
/// # }
/// #
/// # #[action_lifecycle(agent = "TestAgent", command_type = "String", response_type = "i32")]
/// # struct TestActionLifecycle;
/// #
/// # impl TestActionLifecycle {
/// #     async fn on_command<Context>(
/// #         &self,
/// #         command: String,
/// #         _model: &ActionLane<String, i32>,
/// #         _context: &Context,
/// #     ) -> i32
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         println!("Command received: {}", command);
/// #         command.len() as i32
/// #     }
/// # }
/// #
/// # impl LaneLifecycle<TestAgentConfig> for TestActionLifecycle {
/// #     fn create(_config: &TestAgentConfig) -> Self {
/// #         TestActionLifecycle {}
/// #     }
/// # }
/// #
/// # #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
/// # struct TestValueLifecycle;
/// #
/// # impl TestValueLifecycle {
/// #     async fn on_start<Context>(&self, model: &ValueLane<i32>, _context: &Context)
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync,
/// #     {
/// #         println!("Started value lane: {:?}", model)
/// #     }
/// #
/// #     async fn on_event<Context>(
/// #         &self,
/// #         event: &Arc<i32>,
/// #         _model: &ValueLane<i32>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         println!("Event received: {}", event);
/// #     }
/// # }
/// #
/// # impl LaneLifecycle<TestAgentConfig> for TestValueLifecycle {
/// #     fn create(_config: &TestAgentConfig) -> Self {
/// #         TestValueLifecycle {}
/// #     }
/// # }
/// #
/// # impl StatefulLaneLifecycleBase for TestValueLifecycle {
/// #     type WatchStrategy = Queue;
/// #
/// #     fn create_strategy(&self) -> Self::WatchStrategy {
/// #         Queue::default()
/// #     }
/// # }
/// #
/// # #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
/// # struct TestMapLifecycle;
/// #
/// # impl TestMapLifecycle {
/// #     async fn on_start<Context>(&self, model: &MapLane<String, i32>, _context: &Context)
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync,
/// #     {
/// #         println!("Started map lane: {:?}", model)
/// #     }
/// #
/// #     async fn on_event<Context>(
/// #         &self,
/// #         event: &MapLaneEvent<String, i32>,
/// #         _model: &MapLane<String, i32>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         println!("Event received {:?}", event)
/// #     }
/// # }
/// #
/// # impl LaneLifecycle<TestAgentConfig> for TestMapLifecycle {
/// #     fn create(_config: &TestAgentConfig) -> Self {
/// #         TestMapLifecycle {}
/// #     }
/// # }
/// #
/// # impl StatefulLaneLifecycleBase for TestMapLifecycle {
/// #     type WatchStrategy = Queue;
/// #
/// #     fn create_strategy(&self) -> Self::WatchStrategy {
/// #         Queue::default()
/// #     }
/// # }
/// ```
#[proc_macro_derive(SwimAgent, attributes(lifecycle, agent))]
pub fn swim_agent(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let ident = input_ast.ident.clone();
    let derived = match derive_swim_agent(input_ast) {
        Ok(derived) => derived,
        Err(ts) => return ts,
    };

    as_const("SwimAgent", ident, derived.into()).into()
}

/// An attribute for creating agent lifecycles for swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used.
///
/// By default, it expects an async method named `on_start`, but a method with a custom name can be
/// using the `on_start` attribute.
///
/// # Example
/// Agent lifecycle for `TestAgent`, created with the default name for the `on_start` action.
///
/// ```rust
/// use swim_server::agent_lifecycle;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[agent_lifecycle(agent = "TestAgent")]
/// struct TestAgentLifecycle;
///
/// impl TestAgentLifecycle {
///     async fn on_start<Context>(&self, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         println!("Agent started");
///     }
/// }
///
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
///
/// Agent lifecycle for`TestAgent`, created with a custom name for the `on_start` action.
///
/// ```rust
/// use swim_server::agent_lifecycle;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[agent_lifecycle(agent = "TestAgent", on_start = "custom_start_function")]
/// struct TestAgentLifecycle;
///
/// impl TestAgentLifecycle {
///     async fn custom_start_function<Context>(&self, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         println!("Agent started");
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn agent_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_agent_lifecycle)
}

/// An attribute for creating lifecycles for command lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the `CommandLane` to which it will be applied.
///
/// By default, it expects an async method named `on_command`, but a method with a custom name can
/// be provided using the `on_command` attribute.
///
/// # Example
/// Command lifecycle for a `CommandLane` with type [`String`] on the `TestAgent`, created with the
/// default name for the `on_command` action.
///
/// ```
/// use swim_server::command_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::CommandLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[command_lifecycle(
///     agent = "TestAgent",
///     command_type = "String"
/// )]
/// struct TestCommandLifecycle;
///
/// impl LaneLifecycle<TestAgentConfig> for TestCommandLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestCommandLifecycle {}
///     }
/// }
///
/// impl TestCommandLifecycle {
///     async fn on_command<Context>(
///         &self,
///         command: String,
///         _model: &CommandLane<String>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Command received: {}", command);
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
///
/// Command lifecycle for a `CommandLane` with type [`String`] on the `TestAgent`, created with a
/// custom name for the `on_command` action.
///
/// ```rust
/// use swim_server::command_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::CommandLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[command_lifecycle(
///     agent = "TestAgent",
///     command_type = "String",
///     on_command = "custom_on_command"
/// )]
/// struct TestCommandLifecycle;
///
/// impl LaneLifecycle<TestAgentConfig> for TestCommandLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestCommandLifecycle {}
///     }
/// }
///
/// impl TestCommandLifecycle {
///     async fn custom_on_command<Context>(
///         &self,
///         command: String,
///         _model: &CommandLane<String>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Command received: {}", command);
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn command_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_command_lifecycle)
}

/// An attribute for creating lifecycles for action lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// types of the `ActionLane` to which it will be applied.
///
/// By default, it expects an async method named `on_command`, but a method with a custom name can
/// be provided using the `on_command` attribute.
///
/// # Example
/// Action lifecycle for an `ActionLane` with types [`String`] and [`i32`] on the `TestAgent`,
/// created with the default name for the `on_command` action.
///
/// ```rust
/// use swim_server::action_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::ActionLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[action_lifecycle(agent = "TestAgent", command_type = "String", response_type = "i32")]
/// struct TestActionLifecycle;
///
/// impl TestActionLifecycle {
///     async fn on_command<Context>(
///         &self,
///         command: String,
///         _model: &ActionLane<String, i32>,
///         _context: &Context,
///     ) -> i32
///         where
///             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///        println!("Command received: {}", command);
///        command.len() as i32
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestActionLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestActionLifecycle {}
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
///
/// Action lifecycle for an `ActionLane` with types [`String`] and [`i32`] on the`TestAgent`,
/// created with a custom name for the `on_command` action.
///
/// ```rust
/// use swim_server::action_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::ActionLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[action_lifecycle(
///     agent = "TestAgent",
///     command_type = "String",
///     response_type = "i32",
///     on_command = "custom_on_command"
/// )]
/// struct TestActionLifecycle;
///
/// impl TestActionLifecycle {
///     async fn custom_on_command<Context>(
///         &self,
///         command: String,
///         _model: &ActionLane<String, i32>,
///         _context: &Context,
///     ) -> i32
///         where
///             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Command received: {}", command);
///         command.len() as i32
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestActionLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestActionLifecycle {}
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn action_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_action_lifecycle)
}

/// An attribute for creating lifecycles for value lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the `ValueLane` to which it will be applied.
///
/// By default, it expects async methods named `on_start` and `on_event`, but methods with custom
/// names can be provided using the `on_start` and `on_event` attributes.
///
/// # Example
/// Value lifecycle for a `ValueLane` with type [`i32`] on the `TestAgent`, created with the default
/// names for the `on_start` and `on_event` actions.
///
/// ```rust
/// use swim_server::value_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::value::ValueLane;
/// use std::sync::Arc;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
/// struct TestValueLifecycle;
///
/// impl TestValueLifecycle {
///     async fn on_start<Context>(&self, model: &ValueLane<i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///        println!("Started value lane: {:?}", model)
///     }
///
///     async fn on_event<Context>(
///         &self,
///         event: &Arc<i32>,
///         _model: &ValueLane<i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Event received: {}", event);
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestValueLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestValueLifecycle {}
///     }
/// }
///
/// impl StatefulLaneLifecycleBase for TestValueLifecycle {
///     type WatchStrategy = Queue;
///
///     fn create_strategy(&self) -> Self::WatchStrategy {
///         Queue::default()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
///
/// Value lifecycle for a `ValueLane` with type [`i32`] on the `TestAgent`, created with custom
/// names for the `on_start` and `on_event` actions.
///
/// ```rust
/// use swim_server::value_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::value::ValueLane;
/// use std::sync::Arc;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[value_lifecycle(
///     agent = "TestAgent",
///     event_type = "i32",
///     on_start = "custom_on_start",
///     on_event = "custom_on_event"
/// )]
/// struct TestValueLifecycle;
///
/// impl TestValueLifecycle {
///     async fn custom_on_start<Context>(&self, model: &ValueLane<i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         println!("Started value lane: {:?}", model)
///     }
///
///     async fn custom_on_event<Context>(
///         &self,
///         event: &Arc<i32>,
///         _model: &ValueLane<i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Event received: {}", event);
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestValueLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestValueLifecycle {}
///     }
/// }
///
/// impl StatefulLaneLifecycleBase for TestValueLifecycle {
///     type WatchStrategy = Queue;
///
///     fn create_strategy(&self) -> Self::WatchStrategy {
///         Queue::default()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn value_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_value_lifecycle)
}

/// An attribute for creating lifecycles for map lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the `MapLane` to which it will be applied.
///
/// By default, it expects async methods named `on_start` and `on_event`, but methods with custom
/// names can be provided using the `on_start` and `on_event` attributes.
///
/// # Example
/// Map lifecycle for a `MapLane` with types [`String`] and [`i32`] on the `TestAgent`, created with
/// the default names for the `on_start` and `on_event` actions.
///
/// ```rust
/// use swim_server::map_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
/// struct TestMapLifecycle;
///
/// impl TestMapLifecycle {
///     async fn on_start<Context>(&self, model: &MapLane<String, i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///        println!("Started map lane: {:?}", model)
///     }
///
///     async fn on_event<Context>(
///         &self,
///         event: &MapLaneEvent<String, i32>,
///         _model: &MapLane<String, i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         println!("Event received {:?}", event)
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestMapLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestMapLifecycle {}
///     }
/// }
///
/// impl StatefulLaneLifecycleBase for TestMapLifecycle {
///     type WatchStrategy = Queue;
///
///     fn create_strategy(&self) -> Self::WatchStrategy {
///         Queue::default()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
/// Map lifecycle for a `MapLane` with types [`String`] and [`i32`] on the `TestAgent`, created with
/// custom names for the `on_start` and `on_event` actions.
///
/// ```rust
/// use swim_server::map_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
///
/// #[map_lifecycle(
///     agent = "TestAgent",
///     key_type = "String",
///     value_type = "i32",
///     on_start = "custom_on_start",
///     on_event = "custom_on_event")]
/// struct TestMapLifecycle;
///
/// impl TestMapLifecycle {
///     async fn custom_on_start<Context>(&self, model: &MapLane<String, i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         println!("Started map lane: {:?}", model)
///     }
///
///     async fn custom_on_event<Context>(
///         &self,
///         event: &MapLaneEvent<String, i32>,
///         _model: &MapLane<String, i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///        println!("Event received {:?}", event)
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestMapLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestMapLifecycle {}
///     }
/// }
///
/// impl StatefulLaneLifecycleBase for TestMapLifecycle {
///     type WatchStrategy = Queue;
///
///     fn create_strategy(&self) -> Self::WatchStrategy {
///         Queue::default()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn map_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_map_lifecycle)
}

/// An attribute for creating lifecycles for demand lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the `DemandLane` to which it will be applied.
///
/// By default, it expects an async method named `on_cue`, but methods with custom names can be
/// provided using the `on_cue` and `on_event` attributes.
///
/// Demand lifecycle for a `DemandMapLane` with type [`i32`] on the `TestAgent`, created with custom
/// names for the `on_cue` action.
///
/// ```rust
/// use swim_server::demand_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::model::demand::DemandLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
/// # use tokio;
///
/// #[demand_lifecycle(
///     agent = "TestAgent",
///     event_type = "i32",
///     on_cue = "custom_on_cue"
/// )]
/// struct TestDemandLifecycle;
///
/// impl TestDemandLifecycle {
///     async fn custom_on_cue<Context>(
///         &self,
///         _model: &DemandLane<i32>,
///         _context: &Context,
///     ) -> Option<i32> where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         Some(1)
///     }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestDemandLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestDemandLifecycle {}
///     }
/// }
///
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn demand_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_demand_lifecycle)
}

/// An attribute for creating lifecycles for demand map lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the `DemandMapLane` to which it will be applied.
///
/// By default, it expects async methods named `on_cue` and `on_sync`, but methods with custom
/// names can be provided using the `on_cue` and `on_sync` attributes.
///
/// Demand lifecycle for a `DemandMapLane` with a key type of `String` and a value type of `i32`
/// on the `TestAgent`
///
/// ```rust
/// use swim_server::demand_map_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::demand_map::DemandMapLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
/// # use tokio;
///
/// #[demand_map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
/// struct TestDemandLifecycle;
///
/// impl TestDemandLifecycle {
///     async fn on_sync<Context>(
///        &self,
///        _model: &DemandMapLane<String, i32>,
///        _context: &Context,
///    ) -> Vec<String>
///    where
///        Context: AgentContext<TestAgent> + Sized + Send + Sync,
///    {
///        Vec::new()
///    }
///
///    async fn on_cue<Context>(
///        &self,
///        _model: &DemandMapLane<String, i32>,
///        context: &Context,
///        key: String,
///    ) -> Option<i32>
///    where
///        Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///    {
///        Some(1)
///    }
/// }
///
/// impl LaneLifecycle<TestAgentConfig> for TestDemandLifecycle {
///     fn create(_config: &TestAgentConfig) -> Self {
///         TestDemandLifecycle {}
///     }
/// }
///
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent;
/// #
/// # pub struct TestAgentConfig;
/// ```
#[proc_macro_attribute]
pub fn demand_map_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    derive(args, input, derive_demand_map_lifecycle)
}
