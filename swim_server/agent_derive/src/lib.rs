//! Agent derive is a library for creating swim agents and lifecycles for them and their lanes, by annotating structs and asynchronous functions.
//!
//! The minimum requirement for creating swim agents is to provide a config struct that implements the
//! [`AgentConfig`] trait.
//!
//! The minimum requirements for creating lifecycles is to provide the name of the swim agent
//! for which they will be used, the input/output types of the lanes that they will be applied to, and the corresponding
//! lifecycles functions.
//!
//! # Example
//! Creating a custom swim agent with a single command lane.
//! ```rust
//! use std::num::NonZeroUsize;
//! use swim_server::agent::{AgentConfig, AgentContext};
//! use swim_server::agent::lane::model::action::CommandLane;
//! use swim_server::agent::lane::lifecycle::LaneLifecycle;
//! use swim_server::{command_lifecycle, SwimAgent};
//!
//! // ----------------------- Agent derivation -----------------------
//!
//! #[derive(Debug, SwimAgent)]
//! #[agent(config = "TestAgentConfig")]
//! pub struct TestAgent {
//!     #[lifecycle(public, name = "TestCommandLifecycle")]
//!     command: CommandLane<String>,
//! }
//!
//! #[derive(Debug)]
//! pub struct TestAgentConfig {}
//!
//! impl AgentConfig for TestAgentConfig {
//!     fn get_buffer_size(&self) -> NonZeroUsize {
//!         NonZeroUsize::new(5).unwrap()
//!     }
//! }
//!
//! // ----------------------- Command Lifecycle -----------------------
//!
//! #[command_lifecycle(
//!     agent = "TestAgent",
//!     command_type = "String"
//! )]
//! struct TestCommandLifecycle {}
//!
//! impl TestCommandLifecycle {
//!     async fn on_command<Context>(
//!         &self,
//!         _command: String,
//!         _model: &CommandLane<String>,
//!         _context: &Context,
//!     ) where
//!         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
//!     {
//!         unimplemented!()
//!     }
//! }
//!
//! impl LaneLifecycle<TestAgentConfig> for TestCommandLifecycle {
//!     fn create(_config: &TestAgentConfig) -> Self {
//!         TestCommandLifecycle {}
//!     }
//! }
//! ```
use crate::args::{ActionAttrs, AgentAttrs, CommandAttrs, MapAttrs, SwimAgentAttrs, ValueAttrs};
use crate::utils::{get_agent_data, get_task_struct_name, validate_input_ast, InputAstType};
use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, AttributeArgs, DeriveInput};

mod args;
mod utils;

/// A derive attribute for creating swim agents.
///
/// The minimum requirement for creating a swim agent is to provide a config struct that implements the
/// [`AgentConfig`] trait.
///
/// If the swim agent has lanes, they can be annotated with the appropriate lifecycle attributes which require
/// a correct lifecycle struct to be provided.
/// The lifecycles are private by default, and can be made public with the additional `public` attribute.
/// # Example
/// Minimal swim agent and configuration without any lanes.
/// ```rust
/// use std::num::NonZeroUsize;
/// use swim_server::agent::AgentConfig;
/// use swim_server::SwimAgent;
///
/// #[derive(Debug, SwimAgent)]
/// #[agent(config = "TestAgentConfig")]
/// pub struct TestAgent {}
///
/// pub struct TestAgentConfig {}
///
/// impl AgentConfig for TestAgentConfig {
///     fn get_buffer_size(&self) -> NonZeroUsize {
///         NonZeroUsize::new(5).unwrap()
///    }
/// }
/// ```
/// Swim agent with multiple lanes of different types.
/// ```rust
/// use swim_server::SwimAgent;
/// use swim_server::agent::lane::model::action::{ActionLane, CommandLane};
/// use swim_server::agent::lane::model::map::MapLane;
/// use swim_server::agent::lane::model::value::ValueLane;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
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
///     //This is public.
///     #[lifecycle(public, name = "TestActionLifecycle")]
///     action: ActionLane<String, i32>,
///     #[lifecycle(name = "TestValueLifecycle")]
///     value: ValueLane<i32>,
///     #[lifecycle(name = "TestMapLifecycle")]
///     map: MapLane<String, i32>,
/// }
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// #
/// # #[command_lifecycle(
/// # agent = "TestAgent",
/// # command_type = "String",
/// # on_command = "on_command"
/// # )]
/// # struct TestCommandLifecycle {}
/// #
/// # impl TestCommandLifecycle {
/// #     async fn on_command<Context>(
/// #         &self,
/// #         _command: String,
/// #         _model: &CommandLane<String>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         unimplemented!()
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
/// # struct TestActionLifecycle {}
/// #
/// # impl TestActionLifecycle {
/// #     async fn on_command<Context>(
/// #         &self,
/// #         _command: String,
/// #         _model: &ActionLane<String, i32>,
/// #         _context: &Context,
/// #     ) -> i32
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         unimplemented!()
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
/// # struct TestValueLifecycle {}
/// #
/// # impl TestValueLifecycle {
/// #     async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync,
/// #     {
/// #         unimplemented!()
/// #     }
/// #
/// #     async fn on_event<Context>(
/// #         &self,
/// #         _event: &Arc<i32>,
/// #         _model: &ValueLane<i32>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         unimplemented!()
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
/// # struct TestMapLifecycle {}
/// #
/// # impl TestMapLifecycle {
/// #     async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
/// #         where
/// #             Context: AgentContext<TestAgent> + Sized + Send + Sync,
/// #     {
/// #         unimplemented!()
/// #     }
/// #
/// #     async fn on_event<Context>(
/// #         &self,
/// #         _event: &MapLaneEvent<String, i32>,
/// #         _model: &MapLane<String, i32>,
/// #         _context: &Context,
/// #     ) where
/// #         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
/// #     {
/// #         unimplemented!()
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
/// Note: [`TestAgentConfig`] (ommited here) must be a valid struct that implements the [`AgentConfig`] trait, as shown in the previous example.
#[proc_macro_derive(SwimAgent, attributes(lifecycle, agent))]
pub fn swim_agent(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Agent) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match SwimAgentAttrs::from_derive_input(&input_ast) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let (agent_name, config_name, agent_fields) = get_agent_data(args);

    let lanes = agent_fields
        .iter()
        .map(|agent_field| &agent_field.lane_name);
    let tasks = agent_fields
        .iter()
        .map(|agent_field| &agent_field.task_name);
    let lifecycles_ast = agent_fields
        .iter()
        .map(|agent_field| &agent_field.lifecycle_ast);

    let output_ast = quote! {
        use swim_server::agent::LaneTasks as _;

        #[automatically_derived]
        impl swim_server::agent::SwimAgent<#config_name> for #agent_name {
            fn instantiate<Context: swim_server::agent::AgentContext<Self> + swim_server::agent::context::AgentExecutionContext>(
                configuration: &#config_name,
                exec_conf: &swim_server::agent::lane::channels::AgentExecutionConfig,
            ) -> (
                Self,
                std::vec::Vec<std::boxed::Box<dyn swim_server::agent::LaneTasks<Self, Context>>>,
                std::collections::HashMap<std::string::String, std::boxed::Box<dyn swim_server::agent::LaneIo<Context>>>,
            )
                where
                    Context: swim_server::agent::AgentContext<Self> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            {
                let mut io_map: std::collections::HashMap<std::string::String, std::boxed::Box<dyn swim_server::agent::LaneIo<Context>>> = std::collections::HashMap::new();

                #(#lifecycles_ast)*

                let agent = #agent_name {
                    #(#lanes),*
                };

                let tasks = std::vec![
                    #(#tasks.boxed()),*
                ];

                (agent, tasks, io_map)
            }
        }

    };

    TokenStream::from(output_ast)
}

/// An attribute for creating agent lifecycles for swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used.
///
/// By default, it expects an async method named `on_start`, but a method with a custom name can be provided
/// using the `on_start` attribute.
/// # Example
/// Agent lifecycle for [`TestAgent`], created with the default name for the `on_start` action.
/// ```rust
/// use swim_server::agent_lifecycle;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
///
/// #[agent_lifecycle(agent = "TestAgent")]
/// struct TestAgentLifecycle {}
///
/// impl TestAgentLifecycle {
///     async fn on_start<Context>(&self, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
/// Agent lifecycle for `TestAgent`, created with a custom name for the `on_start` action.
/// ```rust
/// use swim_server::agent_lifecycle;
/// use swim_server::agent::AgentContext;
/// # use swim_server::SwimAgent;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
///
/// #[agent_lifecycle(agent = "TestAgent", on_start = "custom_start_function")]
/// struct TestAgentLifecycle {}
///
/// impl TestAgentLifecycle {
///     async fn custom_start_function<Context>(&self, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
#[proc_macro_attribute]
pub fn agent_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match AgentAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let agent_name = &args.agent;
    let on_start_func = &args.on_start;

    let output_ast = quote! {
        use futures::FutureExt as _;

        #input_ast

        #[automatically_derived]
        impl swim_server::agent::lifecycle::AgentLifecycle<#agent_name> for #lifecycle_name {
            fn on_start<'a, C>(&'a self, context: &'a C) -> futures::future::BoxFuture<'a, ()>
            where
                C: swim_server::agent::AgentContext<#agent_name> + core::marker::Send + core::marker::Sync + 'a,
            {
                self.#on_start_func(context).boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

/// An attribute for creating lifecycles for command lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the [`CommandLane`] to which it will be applied.
///
/// By default, it expects an async method named `on_command`, but a method with a custom name can be provided
/// using the `on_command` attribute.
/// # Example
/// Command lifecycle for a [`CommandLane`] with type [`String`] on the [`TestAgent`], created with the default name for the `on_command` action.
/// ```
/// use swim_server::command_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::CommandLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[command_lifecycle(
///     agent = "TestAgent",
///     command_type = "String"
/// )]
/// struct TestCommandLifecycle {}
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
///         _command: String,
///         _model: &CommandLane<String>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
/// Command lifecycle for a [`CommandLane`] with type [`String`] on the [`TestAgent`], created with a custom name for the `on_command` action.
/// ```rust
/// use swim_server::command_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::CommandLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[command_lifecycle(
///     agent = "TestAgent",
///     command_type = "String",
///     on_command = "custom_on_command"
/// )]
/// struct TestCommandLifecycle {}
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
///         _command: String,
///         _model: &CommandLane<String>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
///     }
/// }
/// # #[derive(Debug, SwimAgent)]
/// # #[agent(config = "TestAgentConfig")]
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
#[proc_macro_attribute]
pub fn command_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match CommandAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let command_type = &args.command_type;
    let on_command_func = &args.on_command;

    let output_ast = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;

        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::CommandLane<#command_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, ()>> + core::marker::Send + core::marker::Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        #[automatically_derived]
        impl<T, S> swim_server::agent::Lane for #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::CommandLane<#command_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, ()>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        #[automatically_derived]
        impl<Context, T, S> swim_server::agent::LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: swim_server::agent::AgentContext<#agent_name> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::CommandLane<#command_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, ()>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn start<'a>(&'a self, _context: &'a Context) -> futures::future::BoxFuture<'a, ()> {
                futures::future::ready(()).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> futures::future::BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    let mut events = unsafe { core::pin::Pin::new_unchecked(&mut events) };

                    while let std::option::Option::Some(swim_server::agent::lane::model::action::Action { command, responder }) = events.next().await {

                        tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

                        tracing_futures::Instrument::instrument(
                            lifecycle.#on_command_func(command, &model, &context),
                            tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                        ).await;

                        if let std::option::Option::Some(tx) = responder {
                            if tx.send(()).is_err() {
                                tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                            }
                        }
                    }
                }
                .boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

/// An attribute for creating lifecycles for action lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// types of the [`ActionLane`] to which it will be applied.
///
/// By default, it expects an async method named `on_command`, but a method with a custom name can be provided
/// using the `on_command` attribute.
/// # Example
/// Action lifecycle for an [`ActionLane`] with types [`String`] and [`i32`] on the [`TestAgent`], created with the default name for the `on_command` action.
/// ```rust
/// use swim_server::action_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::ActionLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[action_lifecycle(agent = "TestAgent", command_type = "String", response_type = "i32")]
/// struct TestActionLifecycle {}
///
/// impl TestActionLifecycle {
///     async fn on_command<Context>(
///         &self,
///         _command: String,
///         _model: &ActionLane<String, i32>,
///         _context: &Context,
///     ) -> i32
///         where
///             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
/// Action lifecycle for an [`ActionLane`] with types [`String`] and [`i32`] on the [`TestAgent`], created with a custom name for the `on_command` action.
/// ```rust
/// use swim_server::action_lifecycle;
/// use swim_server::agent::lane::lifecycle::LaneLifecycle;
/// use swim_server::agent::lane::model::action::ActionLane;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[action_lifecycle(
///     agent = "TestAgent",
///     command_type = "String",
///     response_type = "i32",
///     on_command = "custom_on_command"
/// )]
/// struct TestActionLifecycle {}
///
/// impl TestActionLifecycle {
///     async fn custom_on_command<Context>(
///         &self,
///         _command: String,
///         _model: &ActionLane<String, i32>,
///         _context: &Context,
///     ) -> i32
///         where
///             Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
#[proc_macro_attribute]
pub fn action_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match ActionAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let command_type = &args.command_type;
    let response_type = &args.response_type;
    let on_command_func = &args.on_command;

    let output_ast = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;

        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::ActionLane<#command_type, #response_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, #response_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        #[automatically_derived]
        impl<T, S> swim_server::agent::Lane for #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::ActionLane<#command_type, #response_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, #response_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        #[automatically_derived]
        impl<Context, T, S> swim_server::agent::LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: swim_server::agent::AgentContext<#agent_name> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::action::ActionLane<#command_type, #response_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::action::Action<#command_type, #response_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn start<'a>(&'a self, _context: &'a Context) -> futures::future::BoxFuture<'a, ()> {
                futures::future::ready(()).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> futures::future::BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    let mut events = unsafe { core::pin::Pin::new_unchecked(&mut events) };

                    while let std::option::Option::Some(swim_server::agent::lane::model::action::Action { command, responder }) = events.next().await {

                        tracing::event!(tracing::Level::TRACE, commanded = swim_server::agent::COMMANDED, ?command);

                        let response = tracing_futures::Instrument::instrument(
                                lifecycle.#on_command_func(command, &model, &context),
                                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                            ).await;

                        tracing::event!(tracing::Level::TRACE, action_result = swim_server::agent::ACTION_RESULT, ?response);

                        if let std::option::Option::Some(tx) = responder {
                            if tx.send(response).is_err() {
                                tracing::event!(tracing::Level::WARN, response_ingored = swim_server::agent::RESPONSE_IGNORED);
                            }
                        }
                    }
                }
                .boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

/// An attribute for creating lifecycles for value lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the [`ValueLane`] to which it will be applied.
///
/// By default, it expects async methods named `on_start` and `on_event`, but methods with custom names can be provided
/// using the `on_start` and `on_event` attributes.
/// # Example
/// Value lifecycle for a [`ValueLane`] with type [`i32`] on the [`TestAgent`], created with the default names for the `on_start` and `on_event` actions.
/// ```rust
/// use swim_server::value_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::value::ValueLane;
/// use std::sync::Arc;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
/// struct TestValueLifecycle {}
///
/// impl TestValueLifecycle {
///     async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
///
///     async fn on_event<Context>(
///         &self,
///         _event: &Arc<i32>,
///         _model: &ValueLane<i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
/// Value lifecycle for a [`ValueLane`] with type [`i32`] on the [`TestAgent`], created with custom names for the `on_start` and `on_event` actions.
/// ```rust
/// use swim_server::value_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::value::ValueLane;
/// use std::sync::Arc;
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[value_lifecycle(
///     agent = "TestAgent",
///     event_type = "i32",
///     on_start = "custom_on_start",
///     on_event = "custom_on_event"
/// )]
/// struct TestValueLifecycle {}
///
/// impl TestValueLifecycle {
///     async fn custom_on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
///
///     async fn custom_on_event<Context>(
///         &self,
///         _event: &Arc<i32>,
///         _model: &ValueLane<i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
#[proc_macro_attribute]
pub fn value_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match ValueAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let event_type = &args.event_type;
    let on_start_func = &args.on_start;
    let on_event_func = &args.on_event;

    let output_ast = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;

        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::value::ValueLane<#event_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = std::sync::Arc<#event_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        #[automatically_derived]
        impl<T, S> swim_server::agent::Lane for #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::value::ValueLane<#event_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = std::sync::Arc<#event_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        #[automatically_derived]
        impl<Context, T, S> swim_server::agent::LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: swim_server::agent::AgentContext<#agent_name> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::value::ValueLane<#event_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = std::sync::Arc<#event_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn start<'a>(&'a self, context: &'a Context) -> futures::future::BoxFuture<'a, ()> {
                let #task_name { lifecycle, projection, .. } = self;

                let model = projection(context.agent());
                lifecycle.#on_start_func(model, context).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> futures::future::BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    let mut events = unsafe { core::pin::Pin::new_unchecked(&mut events) };

                    while let std::option::Option::Some(event) = events.next().await {
                        tracing_futures::Instrument::instrument(
                                lifecycle.#on_event_func(&event, &model, &context),
                                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
                        ).await;
                    }
                }
                .boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

/// An attribute for creating lifecycles for map lanes on swim agents.
///
/// The attribute requires the name of the swim agent with which this lifecycle will be used and the
/// type of the [`MapLane`] to which it will be applied.
///
/// By default, it expects async methods named `on_start` and `on_event`, but methods with custom names can be provided
/// using the `on_start` and `on_event` attributes.
/// # Example
/// Map lifecycle for a [`MapLane`] with types [`String`] and [`i32`] on the [`TestAgent`], created with the default names for the `on_start` and `on_event` actions.
/// ```rust
/// use swim_server::map_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
/// struct TestMapLifecycle {}
///
/// impl TestMapLifecycle {
///     async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
///
///     async fn on_event<Context>(
///         &self,
///         _event: &MapLaneEvent<String, i32>,
///         _model: &MapLane<String, i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
/// Map lifecycle for a [`MapLane`] with types [`String`] and [`i32`] on the [`TestAgent`], created with custom names for the `on_start` and `on_event` actions.
/// ```rust
/// use swim_server::map_lifecycle;
/// use swim_server::agent::lane::lifecycle::{StatefulLaneLifecycleBase, LaneLifecycle};
/// use swim_server::agent::lane::strategy::Queue;
/// use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
/// use swim_server::agent::AgentContext;
/// # use swim_server::agent::AgentConfig;
/// # use std::num::NonZeroUsize;
/// # use swim_server::SwimAgent;
///
/// #[map_lifecycle(
///     agent = "TestAgent",
///     key_type = "String",
///     value_type = "i32",
///     on_start = "custom_on_start",
///     on_event = "custom_on_event")]
/// struct TestMapLifecycle {}
///
/// impl TestMapLifecycle {
///     async fn custom_on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
///     where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync,
///     {
///         unimplemented!()
///     }
///
///     async fn custom_on_event<Context>(
///         &self,
///         _event: &MapLaneEvent<String, i32>,
///         _model: &MapLane<String, i32>,
///         _context: &Context,
///     ) where
///         Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
///     {
///         unimplemented!()
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
/// # pub struct TestAgent {}
/// #
/// # pub struct TestAgentConfig {}
/// #
/// # impl AgentConfig for TestAgentConfig {
/// #    fn get_buffer_size(&self) -> NonZeroUsize {
/// #        NonZeroUsize::new(5).unwrap()
/// #   }
/// # }
/// ```
#[proc_macro_attribute]
pub fn map_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        let error = error.to_string();
        return TokenStream::from(quote_spanned! {
            input_ast.ident.span() => compile_error!(#error);
        });
    }

    let args = match MapAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let key_type = &args.key_type;
    let value_type = &args.value_type;
    let on_start_func = &args.on_start;
    let on_event_func = &args.on_event;

    let output_ast = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;

        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::map::MapLane<#key_type, #value_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::map::MapLaneEvent<#key_type, #value_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        #[automatically_derived]
        impl<T, S> swim_server::agent::Lane for #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::map::MapLane<#key_type, #value_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::map::MapLaneEvent<#key_type, #value_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        #[automatically_derived]
        impl<Context, T, S> swim_server::agent::LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: swim_server::agent::AgentContext<#agent_name> + swim_server::agent::context::AgentExecutionContext + core::marker::Send + core::marker::Sync + 'static,
            T: core::ops::Fn(&#agent_name) -> &swim_server::agent::lane::model::map::MapLane<#key_type, #value_type> + core::marker::Send + core::marker::Sync + 'static,
            S: futures::Stream<Item = swim_server::agent::lane::model::map::MapLaneEvent<#key_type, #value_type>> + core::marker::Send + core::marker::Sync + 'static
        {
            fn start<'a>(&'a self, context: &'a Context) -> futures::future::BoxFuture<'a, ()> {
                let #task_name { lifecycle, projection, .. } = self;

                let model = projection(context.agent());
                lifecycle.#on_start_func(model, context).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> futures::future::BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    let mut events = unsafe { core::pin::Pin::new_unchecked(&mut events) };

                    while let std::option::Option::Some(event) = events.next().await {
                        tracing_futures::Instrument::instrument(
                                lifecycle.#on_event_func(&event, &model, &context),
                                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
                        ).await;
                    }
                }
                .boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}
