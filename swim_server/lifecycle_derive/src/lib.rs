use crate::args::{ActionAttrs, AgentAttrs, CommandAttrs, MapAttrs, SwimAgent, ValueAttrs};
use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, AttributeArgs, DeriveInput, Path, Type, TypePath};

mod args;

fn get_lifecycle_task_ident(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
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
    let task_name = get_lifecycle_task_ident(&input_ast.ident.to_string());
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
    let task_name = get_lifecycle_task_ident(&input_ast.ident.to_string());
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
    let task_name = get_lifecycle_task_ident(&input_ast.ident.to_string());
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
    let task_name = get_lifecycle_task_ident(&input_ast.ident.to_string());
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
    let task_name = get_lifecycle_task_ident(&input_ast.ident.to_string());
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

#[proc_macro_derive(SwimAgent, attributes(lifecycle))]
pub fn swim_agent(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);

    let args = match SwimAgent::from_derive_input(&input_ast) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    for arg in args.data.take_struct().unwrap().iter() {
        let lane_name = arg.ident.clone().unwrap().to_string();
        // let lane_type = arg.ty

        if let Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) = &arg.ty
        {
            let lane_type = segments.first().unwrap().ident.to_string();
        }

        let lifecycle_name = &arg.name.clone().unwrap();
        let lifecycle_task_name = get_lifecycle_task_ident(lifecycle_name);

        println!("{:?}", &lifecycle_task_name);
        break;
    }

    let agent_name = args.ident;

    // Todo add param for config.
    let config_name = Ident::new("TestAgentConfig", Span::call_site());

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

                // Command lifecycle
                let action_lifecycle = ActionLifecycle::create(configuration);
                let (action, event_stream) = model::action::make_lane_model(configuration.command_buffer_size.clone());
                let action_tasks = ActionLifecycleTask {
                    lifecycle: action_lifecycle,
                    name: "action".into(),
                    event_stream,
                    projection: |agent: &#agent_name| &agent.action,
                };

                // Value lifecycle
                let total_lifecycle = TotalLifecycle::create(configuration);
                let (total, event_stream) =
                    model::value::make_lane_model(0, total_lifecycle.create_strategy());

                let total_tasks = TotalLifecycleTask {
                    lifecycle: total_lifecycle,
                    name: "total".into(),
                    event_stream,
                    projection: |agent: &#agent_name| &agent.total,
                };

                // Map lifecycle
                let data_lifecycle = DataLifecycle::create(configuration);
                let (data, event_stream) = model::map::make_lane_model(data_lifecycle.create_strategy());

                let data_tasks = DataLifecycleTask {
                    lifecycle: data_lifecycle,
                    name: "data".into(),
                    event_stream,
                    projection: |agent: &#agent_name| &agent.data,
                };

                let agent = #agent_name {
                    data,
                    total,
                    action,
                };

                let tasks = vec![
                    data_tasks.boxed(),
                    total_tasks.boxed(),
                    action_tasks.boxed(),
                ];

                (agent, tasks, HashMap::new())
            }
        }

    };

    TokenStream::from(output_ast)
}
