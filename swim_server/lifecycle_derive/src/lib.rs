use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{
    parse_macro_input, AttributeArgs, DeriveInput, FnArg, ItemFn, Lit, LitStr, Meta, MetaNameValue,
    NestedMeta, PatType, Path, PathSegment, Type, TypePath,
};

#[proc_macro_attribute]
pub fn action_lifecycle(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as ItemFn);
    let metadata_ast = parse_macro_input!(metadata as AttributeArgs);

    let lifecycle_struct_name = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(0).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing lifecycle struct name!")
    };

    let agent_name = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(1).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing agent name!")
    };

    let command_type =
        if let FnArg::Typed(PatType { ty, .. }) = input_ast.sig.inputs.first().unwrap() {
            if let Type::Path(TypePath {
                path: Path { segments, .. },
                ..
            }) = *ty.to_owned()
            {
                segments.first().unwrap().ident.clone()
            } else {
                panic!("Invalid command type!")
            }
        } else {
            panic!("Invalid command type!")
        };

    let output_ast = quote! {
        #input_ast

        struct #lifecycle_struct_name<T>
        where
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
        {
            name: #command_type,
            event_stream: mpsc::Receiver<#command_type>,
            projection: T,
        }


        impl<T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static> Lane
        for #lifecycle_struct_name<T>
        {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T> LaneTasks<#agent_name, Context> for #lifecycle_struct_name<T>
        where
            Context: AgentContext<#agent_name> + Sized + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &CommandLane<#command_type> + Send + Sync + 'static,
            {
                fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
                    ready(()).boxed()
                }

                fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                    async move {
                        let #lifecycle_struct_name {
                            name,
                            event_stream,
                            projection,
                        } = *self;

                        let model = projection(context.agent()).clone();
                        let mut events = event_stream.take_until_completes(context.agent_stop_event());
                        pin_mut!(events);
                        while let Some(command) = events.next().await {
                            event!(Level::TRACE, COMMANDED, ?command);
                            custom_on_command(command, &model, &context)
                                .instrument(span!(Level::TRACE, ON_COMMAND))
                                .await;
                        }
                    }
                    .boxed()
                }
            }

    };

    TokenStream::from(output_ast)
}

юю#[proc_macro_attribute]
pub fn value_lifecycle(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let metadata_ast = parse_macro_input!(metadata as AttributeArgs);

    let struct_name = input_ast.ident;

    //Todo refactor into a function
    let agent_name = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(0).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing lifecycle struct name!")
    };

    let event_type = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(1).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing lifecycle struct name!")
    };

    let on_start_func = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(2).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing lifecycle struct name!")
    };

    let on_event_func = if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
        path,
        eq_token,
        lit: Lit::Str(lit),
    })) = metadata_ast.get(3).unwrap()
    {
        Ident::new(&lit.value(), Span::call_site())
    } else {
        panic!("Missing lifecycle struct name!")
    };

    let output_ast = quote! {

        struct #struct_name<T>
        where
            T: Fn(&#agent_name) -> &ValueLane<#event_type> + Send + Sync + 'static,
        {
            name: String,
            event_stream: mpsc::Receiver<Arc<#event_type>>,
            projection: T,
        }

        impl<T: Fn(&#agent_name) -> &ValueLane<#event_type> + Send + Sync + 'static> Lane for #struct_name<T> {
            fn name(&self) -> &str {
                &self.name
            }
        }

        impl<Context, T> LaneTasks<#agent_name, Context> for #struct_name<T>
        where
            Context: AgentContext<#agent_name> + Sized + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &ValueLane<i32> + Send + Sync + 'static,
        {
            fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
                let #struct_name { projection, .. } = self;

                let model = projection(context.agent());
                #on_start_func(model, context).boxed()
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    let #struct_name {
                        name,
                        event_stream,
                        projection,
                    } = *self;

                    let model = projection(context.agent()).clone();
                    let mut events = event_stream.take_until_completes(context.agent_stop_event());
                    pin_mut!(events);
                    while let Some(event) = events.next().await {
                        #on_event_func(&event, &model, &context)
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
