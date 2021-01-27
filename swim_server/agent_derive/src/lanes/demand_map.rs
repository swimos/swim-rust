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

use crate::internals::{default_on_cue, default_on_remove, default_on_sync};
use crate::lanes::derive_lane;
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use darling::FromMeta;
use macro_helpers::{has_fields, string_to_ident};
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Ident};

#[derive(Debug, FromMeta)]
struct DemandMapAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    key_type: Ident,
    #[darling(map = "string_to_ident")]
    value_type: Ident,
    #[darling(default = "default_on_sync", map = "string_to_ident")]
    on_sync: Ident,
    #[darling(default = "default_on_cue", map = "string_to_ident")]
    on_cue: Ident,
    #[darling(default = "default_on_remove", map = "string_to_ident")]
    on_remove: Ident,
}

pub fn derive_demand_map_lifecycle(
    attr_args: AttributeArgs,
    input_ast: DeriveInput,
) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match DemandMapAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = input_ast.ident.clone();
    let has_fields = has_fields(&input_ast.data);
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = args.agent.clone();
    let key_type = &args.key_type;
    let value_type = &args.value_type;
    let on_sync_func = &args.on_sync;
    let on_cue_func = &args.on_cue;
    let on_remove_func = &args.on_remove;

    let on_event = quote! {
        let #task_name {
            lifecycle,
            event_stream,
            projection,
            ..
        } = *self;

        let model = projection(context.agent()).clone();
        let mut events = event_stream.take_until(context.agent_stop_event());
        let mut events = unsafe { Pin::new_unchecked(&mut events) };

        while let Some(event) = events.next().await {
            match event {
                DemandMapLaneCommand::Sync(sender) => {
                    let keys: Vec<#key_type> = lifecycle.#on_sync_func(&model, &context).await;
                    let keys_len = keys.len();

                    let mut values = iter(keys)
                        .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                            if let Some(value) =
                                lifecycle.#on_cue_func(&model, &context, key.clone()).await
                            {
                                results.push(DemandMapLaneEvent::update(key, value));
                            }

                            results
                        })
                        .await;

                    values.shrink_to_fit();

                    let _ = sender.send(values);
                }
                DemandMapLaneCommand::Cue(sender, key) => {
                    let value = lifecycle.#on_cue_func(&model, &context, key).await;
                    let _ = sender.send(value);
                }
                DemandMapLaneCommand::Remove(key) => {
                    lifecycle.#on_remove_func(&model, &context, key).await;
                }
            }
        }
    };

    derive_lane(
        "DemandMapLifecycle",
        lifecycle_name,
        has_fields,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::demand_map::DemandMapLane<#key_type, #value_type>),
        quote!(swim_server::agent::lane::model::demand_map::DemandMapLaneCommand<#key_type, #value_type>),
        None,
        on_event,
        quote! {
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
            use swim_server::agent::lane::model::demand_map::{DemandMapLane, DemandMapLaneCommand, DemandMapLaneEvent};
            use futures::stream::iter;
        },
        None,
        quote!(DemandMap),
    )
}
