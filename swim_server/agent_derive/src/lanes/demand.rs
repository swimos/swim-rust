// Copyright 2015-2021 Swim Inc.
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
    get_task_struct_name, parse_callback, validate_input_ast, CallbackKind, InputAstType,
    LaneTasksImpl,
};
use darling::FromMeta;
use macro_utilities::{has_fields, string_to_ident};
use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Ident};

#[derive(Debug, FromMeta)]
struct DemandAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    event_type: Ident,
    #[darling(default)]
    on_cue: Option<darling::Result<String>>,
    #[darling(default)]
    gen_lifecycle: Option<bool>,
}

pub fn derive_demand_lifecycle(attr_args: AttributeArgs, input_ast: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args = match DemandAttrs::from_list(&attr_args) {
        Ok(args) => args,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    let lifecycle_name = input_ast.ident.clone();
    let gen_lifecycle = args
        .gen_lifecycle
        .unwrap_or_else(|| !has_fields(&input_ast.data));
    let task_name = get_task_struct_name(&input_ast.ident.to_string());
    let agent_name = args.agent.clone();
    let event_type = &args.event_type;
    let on_cue_callback = parse_callback(&args.on_cue, task_name.clone(), CallbackKind::Cue);
    let lane_tasks_impl = LaneTasksImpl::Demand {
        on_cue: on_cue_callback,
    };

    let extra_field = Some(quote! {
        response_tx: tokio::sync::mpsc::Sender<#event_type>
    });

    derive_lane(
        "DemandLifecycle",
        lifecycle_name,
        gen_lifecycle,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::demand::DemandLane<#event_type>),
        quote!(()),
        lane_tasks_impl,
        quote! {
            use swim_server::agent::lane::model::demand::DemandLane;
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
        },
        extra_field,
        quote!(Demand),
    )
}

pub fn derive_events_body(task_name: &Ident, on_cue_func: &Ident) -> proc_macro2::TokenStream {
    quote!(
        let #task_name {
            lifecycle,
            event_stream,
            projection,
            response_tx,
            ..
        } = *self;

        let model = projection(context.agent()).clone();
        let mut events = event_stream.take_until(context.agent_stop_event());
        let mut events = unsafe { Pin::new_unchecked(&mut events) };

        while let Some(event) = events.next().await {
            if let Some(value) = lifecycle.#on_cue_func(&model, &context).await {
                let _ = response_tx.send(value).await;
            }
        }
    )
}
