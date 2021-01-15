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

use macro_helpers::as_const;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Ident};

pub mod action;
pub mod command;
pub mod demand;
pub mod demand_map;
pub mod map;
pub mod value;

#[allow(clippy::too_many_arguments)]
pub fn derive_lane(
    trait_name: &str,
    typ: Ident,
    task_name: Ident,
    agent_name: Ident,
    input_ast: DeriveInput,
    lane_type: TokenStream,
    item_type: TokenStream,
    on_start: Option<TokenStream>,
    on_event: TokenStream,
    imports: TokenStream,
    field: Option<TokenStream>,
    lane_kind: TokenStream,
) -> proc_macro::TokenStream {
    let public_derived = quote! {
        #input_ast

        struct #task_name<T, S>
        where
            T: core::ops::Fn(&#agent_name) -> &#lane_type + Send + Sync + 'static,
            S: futures::Stream<Item = #item_type> + Send + Sync + 'static
        {
            lifecycle: #typ,
            name: String,
            event_stream: S,
            projection: T,
            #field
        }
    };

    let on_start = on_start.unwrap_or(quote! {
        ready(()).boxed()
    });

    let private_derived = quote! {
        use futures::FutureExt as _;
        use futures::StreamExt as _;
        use futures::Stream;
        use futures::future::{ready, BoxFuture};

        use swim_server::agent::{Lane, LaneTasks};
        use swim_server::agent::AgentContext;
        use swim_server::agent::context::AgentExecutionContext;

        use core::pin::Pin;

        #imports

        #[automatically_derived]
        impl<T, S> Lane for #task_name<T, S>
        where
            T: Fn(&#agent_name) -> &#lane_type + Send + Sync + 'static,
            S: Stream<Item = #item_type> + Send + Sync + 'static
        {
            fn name(&self) -> &str {
                &self.name
            }

            fn kind(&self) -> swim_server::agent::lane::LaneKind {
                swim_server::agent::lane::LaneKind::#lane_kind
            }
        }

        #[automatically_derived]
        impl<Context, T, S> LaneTasks<#agent_name, Context> for #task_name<T, S>
        where
            Context: AgentContext<#agent_name> + AgentExecutionContext + Send + Sync + 'static,
            T: Fn(&#agent_name) -> &#lane_type + Send + Sync + 'static,
            S: Stream<Item = #item_type> + Send + Sync + 'static
        {
            fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
                #on_start
            }

            fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
                async move {
                    #on_event
                }
                .boxed()
            }
        }
    };

    let wrapped = as_const(trait_name, typ, private_derived);

    let derived = quote! {
        #public_derived

        #wrapped
    };

    derived.into()
}
