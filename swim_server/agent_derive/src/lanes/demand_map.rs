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

use crate::lanes::derive_lane;
use crate::utils::{
    get_task_struct_name, validate_input_ast, CallbackKind, InputAstType, LaneTasksImpl,
};
use crate::utils::{parse_callback, Callback};
use darling::FromMeta;
use macro_helpers::{has_fields, string_to_ident};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
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
    #[darling(default)]
    on_sync: Option<darling::Result<String>>,
    #[darling(default)]
    on_cue: Option<darling::Result<String>>,
    #[darling(default)]
    on_remove: Option<darling::Result<String>>,
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
    let on_sync_callback = parse_callback(&args.on_sync, task_name.clone(), CallbackKind::Sync);
    let on_cue_callback = parse_callback(&args.on_cue, task_name.clone(), CallbackKind::Cue);
    let on_remove_callback =
        parse_callback(&args.on_remove, task_name.clone(), CallbackKind::Remove);
    let lane_tasks_impl = LaneTasksImpl::DemandMap {
        on_sync: on_sync_callback,
        on_cue: on_cue_callback,
        on_remove: on_remove_callback,
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
        lane_tasks_impl,
        quote! {
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
            use swim_server::agent::lane::model::demand_map::{DemandMapLane, DemandMapLaneEvent, DemandMapLaneCommand};
            use futures::stream::iter;
        },
        None,
    )
}

fn on_sync_body(on_sync_func_name: &Ident, on_cue_func_name: Option<&Ident>) -> TokenStream2 {
    let op = match on_cue_func_name {
        Some(on_cue_func_name) => quote! {
            if let Some(value) = lifecycle.#on_cue_func_name(&model, &context, key.clone()).await
            {
                results.push(DemandMapLaneEvent::update(key, value));
            }
        },
        None => quote!(),
    };
    quote! {
        DemandMapLaneCommand::Sync(sender) => {
            let keys: Vec<_> = lifecycle.#on_sync_func_name(&model, &context).await;
            let keys_len = keys.len();

            let mut values = iter(keys)
                .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                    #op

                    results
                })
                .await;

            values.shrink_to_fit();

            let _ = sender.send(values);
        }
    }
}

fn on_cue_body(on_cue_func_name: &Ident) -> TokenStream2 {
    quote! {
        DemandMapLaneCommand::Cue(sender, key) => {
            let value = lifecycle.#on_cue_func_name(&model, &context, key).await;
            let _ = sender.send(value);
        }
    }
}

fn on_remove_body(on_remove_func_name: &Ident) -> TokenStream2 {
    quote! {
        DemandMapLaneCommand::Remove(key) => {
            lifecycle.#on_remove_func_name(&model, &context, key).await;
        }
    }
}

fn wrap(task_name: &Ident, body: TokenStream2) -> TokenStream2 {
    quote!(
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
                #body
            }
        }
    )
}

pub fn derive_events_body(
    on_sync: &Option<Callback>,
    on_cue: &Option<Callback>,
    on_remove: &Option<Callback>,
) -> Option<TokenStream2> {
    match (on_sync, on_cue, on_remove) {
        (Some(on_sync), Some(on_cue), Some(on_remove)) => {
            let task_name = &on_sync.task_name;
            let on_sync_body = on_sync_body(&on_sync.func_name, Some(&on_cue.func_name));
            let on_cue_body = on_cue_body(&on_cue.func_name);
            let on_remove_body = on_remove_body(&on_remove.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_sync_body
                    #on_cue_body
                    #on_remove_body
                },
            ))
        }
        (Some(on_sync), None, None) => {
            let task_name = &on_sync.task_name;
            let on_sync_body = on_sync_body(&on_sync.func_name, None);

            Some(wrap(
                task_name,
                quote! {
                    #on_sync_body
                    _=> {}
                },
            ))
        }
        (None, Some(on_cue), None) => {
            let task_name = &on_cue.task_name;
            let on_cue_body = on_cue_body(&on_cue.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_cue_body
                    _=> {}
                },
            ))
        }
        (None, None, Some(on_remove)) => {
            let task_name = &on_remove.task_name;
            let on_remove_body = on_remove_body(&on_remove.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_remove_body
                    _=> {}
                },
            ))
        }
        (None, Some(on_cue), Some(on_remove)) => {
            let task_name = &on_cue.task_name;
            let on_cue_body = on_cue_body(&on_cue.func_name);
            let on_remove_body = on_remove_body(&on_remove.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_cue_body
                    #on_remove_body
                    _=> {}
                },
            ))
        }
        (Some(on_sync), None, Some(on_remove)) => {
            let task_name = &on_sync.task_name;
            let on_sync_body = on_sync_body(&on_sync.func_name, None);
            let on_remove_body = on_remove_body(&on_remove.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_sync_body
                    #on_remove_body
                    _=> {}
                },
            ))
        }
        (Some(on_sync), Some(on_cue), None) => {
            let task_name = &on_sync.task_name;
            let on_sync_body = on_sync_body(&on_sync.func_name, Some(&on_cue.func_name));
            let on_cue_body = on_cue_body(&on_cue.func_name);

            Some(wrap(
                task_name,
                quote! {
                    #on_sync_body
                    #on_cue_body
                    _=> {}
                },
            ))
        }
        (None, None, None) => None,
    }
}
