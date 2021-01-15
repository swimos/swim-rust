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

use crate::internals::{default_on_cue, default_on_sync};
use crate::lanes::derive_lane;
use crate::utils::{get_task_struct_name, validate_input_ast, InputAstType};
use macro_helpers::{Context, Symbol};
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{AttributeArgs, DeriveInput, Ident, Meta, NestedMeta};
const AGENT_PATH: Symbol = Symbol("agent");
const KEY_TYPE: Symbol = Symbol("key_type");
const VALUE_TYPE: Symbol = Symbol("value_type");
const ON_CUE: Symbol = Symbol("on_cue");
const ON_SYNC: Symbol = Symbol("on_sync");

pub fn derive_demand_map_lifecycle(
    attr_args: AttributeArgs,
    input_ast: DeriveInput,
) -> Result<TokenStream2, Vec<syn::Error>> {
    let mut context = Context::default();

    if let Err(_) = validate_input_ast(&input_ast, InputAstType::Lifecycle, &mut context) {
        return Err(context.check().unwrap_err());
    }

    let mut agent_opt = None;
    let mut key_type_opt = None;
    let mut value_type_opt = None;
    let mut on_cue_func = default_on_cue();
    let mut on_sync_func = default_on_sync();

    attr_args.iter().for_each(|meta| match meta {
        NestedMeta::Meta(Meta::List(list)) if list.path == AGENT_PATH => {
            agent_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == KEY_TYPE => {
            key_type_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == VALUE_TYPE => {
            value_type_opt = Some(Ident::new(
                &list.nested.to_token_stream().to_string(),
                list.span(),
            ));
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == ON_CUE => {
            on_cue_func = Ident::new(&list.nested.to_token_stream().to_string(), list.span());
        }
        NestedMeta::Meta(Meta::List(list)) if list.path == ON_SYNC => {
            on_sync_func = Ident::new(&list.nested.to_token_stream().to_string(), list.span());
        }
        nm => {
            context.error_spanned_by(nm, "Unknown parameter");
        }
    });

    let agent_name = syn_ok!(agent_opt, &input_ast, "Agent identity must be provided");
    let lifecycle_name = input_ast.ident.clone();
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let key_type = syn_ok!(key_type_opt, &input_ast, "Key type must be provided");
    let value_type = syn_ok!(value_type_opt, &input_ast, "Value type must be provided");

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
                DemandMapLaneEvent::Sync(sender) => {
                    let keys: Vec<#key_type> = lifecycle.#on_sync_func(&model, &context).await;
                    let keys_len = keys.len();

                    let mut values = iter(keys)
                        .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                            if let Some(value) =
                                lifecycle.#on_cue_func(&model, &context, key.clone()).await
                            {
                                results.push(DemandMapLaneUpdate::make(key, value));
                            }

                            results
                        })
                        .await;

                    values.shrink_to_fit();

                    let _ = sender.send(values);
                }
                DemandMapLaneEvent::Cue(sender, key) => {
                    let value = lifecycle.#on_cue_func(&model, &context, key).await;
                    let _ = sender.send(value);
                }
            }
        }
    };

    Ok(derive_lane(
        "DemandMapLifecycle",
        lifecycle_name,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::demand_map::DemandMapLane<#key_type, #value_type>),
        quote!(swim_server::agent::lane::model::demand_map::DemandMapLaneEvent<#key_type, #value_type>),
        None,
        on_event,
        quote! {
            use swim_server::agent::lane::model::demand_map::{DemandMapLane, DemandMapLaneEvent, DemandMapLaneUpdate};
            use futures::stream::iter;
        },
        None,
    ).into())
}
