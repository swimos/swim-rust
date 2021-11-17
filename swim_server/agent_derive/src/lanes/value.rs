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
struct ValueAttrs {
    #[darling(map = "string_to_ident")]
    agent: Ident,
    #[darling(map = "string_to_ident")]
    event_type: Ident,
    #[darling(default)]
    on_start: Option<darling::Result<String>>,
    #[darling(default)]
    on_event: Option<darling::Result<String>>,
    #[darling(default)]
    gen_lifecycle: Option<bool>,
}

pub fn derive_value_lifecycle(attr_args: AttributeArgs, input_ast: DeriveInput) -> TokenStream {
    if let Err(error) = validate_input_ast(&input_ast, InputAstType::Lifecycle) {
        return TokenStream::from(quote! {#error});
    }

    let args: ValueAttrs = match ValueAttrs::from_list(&attr_args) {
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

    let on_start_callback = parse_callback(&args.on_start, task_name.clone(), CallbackKind::Start);
    let on_event_callback = parse_callback(&args.on_event, task_name.clone(), CallbackKind::Event);
    let lane_tasks_impl = LaneTasksImpl::Value {
        on_start: on_start_callback,
        on_event: on_event_callback,
    };

    derive_lane(
        "ValueLifecycle",
        lifecycle_name,
        gen_lifecycle,
        task_name,
        agent_name,
        input_ast,
        quote!(swim_server::agent::lane::model::value::ValueLane<#event_type>),
        quote!(std::sync::Arc<#event_type>),
        lane_tasks_impl,
        quote! {
            use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
            use swim_server::SwimStreamExt;
            use swim_server::agent::lane::lifecycle::LaneLifecycle;
        },
        None,
        quote!(Value),
    )
}

pub fn derive_start_body(task_name: &Ident, on_start_func: &Ident) -> proc_macro2::TokenStream {
    quote!(
        let #task_name { lifecycle, projection, .. } = self;
        let model = projection(context.agent());
        lifecycle.#on_start_func(model, context).boxed()
    )
}

pub fn derive_events_body(
    task_name: &Ident,
    on_event_func_name: &Ident,
) -> proc_macro2::TokenStream {
    quote!(
        let #task_name {
            mut lifecycle,
            event_stream,
            projection,
            ..
        } = *self;

        let model = projection(context.agent()).clone();
        let mut events = event_stream.take_until(context.agent_stop_event());

        let mut scan_stream = events.owning_scan(None, |prev_val, event| async move {
            Some((
                Some(event.clone()),
                ValueLaneEvent {
                    previous: prev_val,
                    current: event,
                },
            ))
        });

        let mut scan_stream = unsafe { Pin::new_unchecked(&mut scan_stream) };

        while let Some(event) = scan_stream.next().await {
              tracing_futures::Instrument::instrument(
                lifecycle.#on_event_func_name(&event, &model, &context),
                tracing::span!(tracing::Level::TRACE, swim_server::agent::ON_EVENT, ?event)
            ).await;
        }
    )
}
