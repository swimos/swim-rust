use crate::args::{ActionAttrs, AgentAttrs, CommandAttrs, MapAttrs, ValueAttrs};
use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, DeriveInput};
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
    let agent_name = Ident::new(&args.agent, Span::call_site());
    let on_start_func = Ident::new(&args.on_start, Span::call_site());

    let output_ast = quote! {

        #input_ast

        struct #task_name {
            lifecycle: #lifecycle_name
        }

        impl AgentLifecycle<#agent_name> for #task_name {
            fn on_start<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
            where
                C: AgentContext<#agent_name> + Send + Sync + 'a,
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
    let agent_name = Ident::new(&args.agent, Span::call_site());
    let command_type = Ident::new(&args.command_type, Span::call_site());
    let on_command_func = Ident::new(&args.on_command, Span::call_site());

    let output_ast = quote! {

        #input_ast

        struct #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, ()>> + Send + Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }


        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, ()>> + Send + Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + Sized + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, ()>> + Send + Sync + 'static
            {
                fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
                    ready(()).boxed()
                }

                fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                    async move {
                        let #task_name {
                            lifecycle,
                            event_stream,
                            projection,
                            ..
                        } = *self;

                        let model = projection(context.agent()).clone();
                        let mut events = event_stream.take_until(context.agent_stop_event());
                        pin_mut!(events);
                        while let Some(Action { command, responder }) = events.next().await {
                            event!(Level::TRACE, COMMANDED, ?command);
                            #on_command_func(&lifecycle, command, &model, &context)
                                .instrument(span!(Level::TRACE, ON_COMMAND))
                                .await;
                            if let Some(tx) = responder {
                                if tx.send(()).is_err() {
                                    event!(Level::WARN, RESPONSE_IGNORED);
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
    let agent_name = Ident::new(&args.agent, Span::call_site());
    let command_type = Ident::new(&args.command_type, Span::call_site());
    let response_type = Ident::new(&args.response_type, Span::call_site());
    let on_command_func = Ident::new(&args.on_command, Span::call_site());

    let output_ast = quote! {

        #input_ast

        struct #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &ActionLane<#command_type, #response_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, #response_type>> + Send + Sync + 'static
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &ActionLane<#command_type, #response_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, #response_type>> + Send + Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + AgentExecutionContext + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &ActionLane<#command_type, #response_type> + Send + Sync + 'static,
            S: Stream<Item = Action<#command_type, #response_type>> + Send + Sync + 'static
        {
            fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
                ready(()).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    pin_mut!(events);
                    while let Some(Action { command, responder }) = events.next().await {
                        event!(Level::TRACE, COMMANDED, ?command);
                        let response = #on_command_func(&lifecycle, command, &model, &context)
                            .instrument(span!(Level::TRACE, ON_COMMAND))
                            .await;
                        event!(Level::TRACE, ACTION_RESULT, ?response);
                        if let Some(tx) = responder {
                            if tx.send(response).is_err() {
                                event!(Level::WARN, RESPONSE_IGNORED);
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
    let agent_name = Ident::new(&args.agent, Span::call_site());
    let event_type = Ident::new(&args.event_type, Span::call_site());
    let on_start_func = Ident::new(&args.on_start, Span::call_site());
    let on_event_func = Ident::new(&args.on_event, Span::call_site());

    let output_ast = quote! {

        #input_ast

        struct #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &ValueLane<#event_type> + Send + Sync + 'static,
            S: Stream<Item = Arc<#event_type>> + Send + Sync + 'static,
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &ValueLane<#event_type> + Send + Sync + 'static,
            S: Stream<Item = Arc<#event_type>> + Send + Sync + 'static,
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + AgentExecutionContext + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &ValueLane<i32> + Send + Sync + 'static,
            S: Stream<Item = Arc<#event_type>> + Send + Sync + 'static,
        {
            fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
                let #task_name { lifecycle, projection, .. } = self;

                let model = projection(context.agent());
                #on_start_func(lifecycle, model, context).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    pin_mut!(events);
                    while let Some(event) = events.next().await {
                        #on_event_func(&lifecycle, &event, &model, &context)
                            .instrument(span!(Level::TRACE, ON_EVENT, ?event))
                            .await;
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
    let agent_name = Ident::new(&args.agent, Span::call_site());
    let key_type = Ident::new(&args.key_type, Span::call_site());
    let value_type = Ident::new(&args.value_type, Span::call_site());
    let on_start_func = Ident::new(&args.on_start, Span::call_site());
    let on_event_func = Ident::new(&args.on_event, Span::call_site());

    let output_ast = quote! {

        #input_ast

        struct #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &MapLane<#key_type, #value_type> + Send + Sync + 'static,
            S: Stream<Item = MapLaneEvent<#key_type, #value_type>> + Send + Sync + 'static,
        {
            lifecycle: #lifecycle_name,
            name: String,
            event_stream: S,
            projection: T,
        }

        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &MapLane<#key_type, #value_type> + Send + Sync + 'static,
            S: Stream<Item = MapLaneEvent<#key_type, #value_type>> + Send + Sync + 'static,
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + AgentExecutionContext + Send + Sync + 'static,
            S: Stream<Item = MapLaneEvent<#key_type, #value_type>> + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &MapLane<#key_type, #value_type> + Send + Sync + 'static,
        {
            fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
                let #task_name { lifecycle, projection, .. } = self;

                let model = projection(context.agent());
                #on_start_func(lifecycle, model, context).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    let #task_name {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until(context.agent_stop_event());
                    pin_mut!(events);
                    while let Some(event) = events.next().await {
                        #on_event_func(&lifecycle, &event, &model, &context)
                            .instrument(span!(Level::TRACE, ON_EVENT, ?event))
                            .await;
                    }
                }
                .boxed()
            }
        }

    };

    TokenStream::from(output_ast)
}
