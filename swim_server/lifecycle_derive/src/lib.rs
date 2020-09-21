use crate::args::{ActionAttrs, AgentAttrs, CommandAttrs, MapAttrs, SwimAgent, ValueAttrs};
use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::{Ident, Literal, Span};
use quote::quote;
use std::collections::HashMap;
use syn::{parse_macro_input, AttributeArgs, DeriveInput, Path, Type, TypePath};

mod args;

fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

fn get_task_var_name(name: &str) -> Ident {
    Ident::new(&format!("{}_task", name), Span::call_site())
}

#[proc_macro_attribute]
pub fn agent_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    let args = match AgentAttrs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = &input_ast.ident;
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = &args.agent;
    let on_start_func = &args.on_start;

    let output_ast = quote! {

        #input_ast

        struct #task_name {
            lifecycle: #lifecycle_name
        }

        #[automatically_derived]
        impl swim_server::agent::lifecycle::AgentLifecycle<#agent_name> for #task_name {
            fn on_start<'a, C>(&'a self, context: &'a C) -> futures::future::BoxFuture<'a, ()>
            where
                C: swim_server::agent::AgentContext<#agent_name> + core::marker::Send + core::marker::Sync + 'a,
            {
                #on_start_func(&self.lifecycle, context).boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}

#[proc_macro_attribute]
pub fn command_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    let args = match CommandAttrs::from_list(&attr_args) {
        Ok(v) => v,
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
                        pin_utils::pin_mut!(events);
                        while let std::option::Option::Some(swim_server::agent::lane::model::action::Action { command, responder }) = events.next().await {

                        let commanded = swim_server::agent::COMMANDED;
                        let response_ingored = swim_server::agent::RESPONSE_IGNORED;

                            tracing::event!(tracing::Level::TRACE, commanded, ?command);

                            tracing_futures::Instrument::instrument(
                                #on_command_func(&lifecycle, command, &model, &context),
                                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                            ).await;

                            if let std::option::Option::Some(tx) = responder {
                                if tx.send(()).is_err() {
                                    tracing::event!(tracing::Level::WARN, response_ingored);
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

#[proc_macro_attribute]
pub fn action_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    let args = match ActionAttrs::from_list(&attr_args) {
        Ok(v) => v,
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
                    pin_utils::pin_mut!(events);
                    while let std::option::Option::Some(swim_server::agent::lane::model::action::Action { command, responder }) = events.next().await {
                        tracing::event!(tracing::Level::TRACE, swim_server::agent::COMMANDED, ?command);

                        let response = tracing_futures::Instrument::instrument(
                                #on_command_func(&lifecycle, command, &model, &context),
                                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_COMMAND)
                            ).await;

                        tracing::event!(Level::TRACE, swim_server::agent::ACTION_RESULT, ?response);

                        if let std::option::Option::Some(tx) = responder {
                            if tx.send(response).is_err() {
                                tracing::event!(tracing::Level::WARN, swim_server::agent::RESPONSE_IGNORED);
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

#[proc_macro_attribute]
pub fn value_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    let args = match ValueAttrs::from_list(&attr_args) {
        Ok(v) => v,
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
                #on_start_func(lifecycle, model, context).boxed()
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
                    pin_utils::pin_mut!(events);
                    while let std::option::Option::Some(event) = events.next().await {
                        tracing_futures::Instrument::instrument(
                                #on_event_func(&lifecycle, &event, &model, &context),
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

#[proc_macro_attribute]
pub fn map_lifecycle(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let attr_args = parse_macro_input!(args as AttributeArgs);

    let args = match MapAttrs::from_list(&attr_args) {
        Ok(v) => v,
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
                #on_start_func(lifecycle, model, context).boxed()
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
                    pin_utils::pin_mut!(events);
                    while let std::option::Option::Some(event) = events.next().await {
                        tracing_futures::Instrument::instrument(
                                #on_event_func(&lifecycle, &event, &model, &context),
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

fn create_lane(
    lane_type: String,
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    match lane_type.as_str() {
        "CommandLane" => create_command_lane(&agent_name, &lifecycle, &lane_name),
        "ValueLane" => create_value_lane(&agent_name, &lifecycle, &lane_name),
        "MapLane" => create_map_lane(&agent_name, &lifecycle, &lane_name),
        _ => unimplemented!(),
    }
}

fn create_map_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) = model::map::make_lane_model(lifecycle.create_strategy());

            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}

//Todo add init
fn create_value_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) =
                model::value::make_lane_model(0, lifecycle.create_strategy());

            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}

fn create_command_lane(
    agent_name: &Ident,
    lifecycle: &Ident,
    lane_name: &Ident,
) -> (proc_macro2::TokenStream, Ident) {
    let lane_name_str = lane_name.to_string();
    let task_var_ident = get_task_var_name(&lane_name_str);
    let task_struct_ident = get_task_struct_name(&lifecycle.to_string());
    let lane_name_lit = Literal::string(&lane_name_str);

    (
        quote! {
            let lifecycle = #lifecycle::create(configuration);
            let (#lane_name, event_stream) = model::action::make_lane_model(configuration.command_buffer_size.clone());
            let #task_var_ident = #task_struct_ident {
                lifecycle,
                name: #lane_name_lit.into(),
                event_stream,
                projection: |agent: &#agent_name| &agent.#lane_name,
            };
        },
        task_var_ident,
    )
}

#[proc_macro_derive(SwimAgent, attributes(lifecycle))]
pub fn swim_agent(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);

    let args = match SwimAgent::from_derive_input(&input_ast) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let agent_name = args.ident;

    // Todo add param for config.
    let config_name = Ident::new("TestAgentConfig", Span::call_site());

    // Todo extract into function
    let mut lifecycles: HashMap<Ident, (Ident, String)> = HashMap::new();
    for arg in args.data.take_struct().unwrap().iter() {
        let lane_name = arg.ident.clone().unwrap();
        let lifecycle_name = Ident::new(&arg.name.clone().unwrap(), Span::call_site());

        if let Type::Path(TypePath {
                              path: Path { segments, .. },
                              ..
                          }) = &arg.ty
        {
            let lane_type = segments.first().unwrap().ident.to_string();
            lifecycles.insert(lane_name, (lifecycle_name, lane_type));
        }
    }

    let mut lifecycles_ast = Vec::new();
    let mut tasks = Vec::new();
    let mut lanes = Vec::new();

    for (lane_name, (lifecycle_name, lane_type)) in lifecycles {
        let (ast, task_name) = create_lane(lane_type, &agent_name, &lifecycle_name, &lane_name);

        lifecycles_ast.push(ast);
        tasks.push(task_name);
        lanes.push(lane_name);
    }

    let lifecycles_ast = quote! {
         #(#lifecycles_ast)*
    };

    let agent_ast = quote! {
        #agent_name {
            #(#lanes),*
        };
    };

    let tasks_ast = quote! {
        vec![
                #(#tasks.boxed()),*
        ];
    };

    let output_ast = quote! {

        #[automatically_derived]
        impl SwimAgent<#config_name> for #agent_name {
            fn instantiate<Context: AgentContext<Self> + AgentExecutionContext>(
                configuration: &#config_name,
            ) -> (
                Self,
                Vec<Box<dyn LaneTasks<Self, Context>>>,
                HashMap<String, Box<dyn LaneIo<Context>>>,
            )
                where
                    Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
            {

                #lifecycles_ast
                let agent = #agent_ast
                let tasks = #tasks_ast

                (agent, tasks, HashMap::new())
            }
        }

    };

    TokenStream::from(output_ast)
}
