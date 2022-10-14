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

use proc_macro2::TokenStream;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{parse_quote, Ident};

mod model;

pub use model::{validate_input, LaneKind, LaneModel, LanesModel};

pub struct DeriveAgentLaneModel<'a>(LanesModel<'a>);

impl<'a> DeriveAgentLaneModel<'a> {
    pub fn new(model: LanesModel<'a>) -> Self {
        DeriveAgentLaneModel(model)
    }
}

impl<'a> ToTokens for DeriveAgentLaneModel<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveAgentLaneModel(LanesModel {
            agent_type,
            ref lanes,
        }) = *self;

        let lane_models = lanes
            .iter()
            .zip(0u64..)
            .map(|(model, i)| OrdinalLaneModel::new(agent_type, i, *model))
            .collect::<Vec<_>>();

        let initializers = lane_models
            .iter()
            .copied()
            .map(FieldInitializer)
            .map(FieldInitializer::into_tokens);

        let base: syn::Type = parse_quote!(::swim_agent::reexport::coproduct::CNil);

        let no_handler: syn::Type = parse_quote!(::swim_agent::event_handler::UnitHandler);

        let (map_type_models, value_type_models) = lane_models
            .iter()
            .copied()
            .partition::<Vec<_>, _>(OrdinalLaneModel::map_like);

        let value_handler = if !value_type_models.is_empty() {
            value_type_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HandlerType(*model);
                    let handler_tok = handler_ty.into_tokens();
                    parse_quote!(::swim_agent::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let map_handler = if !map_type_models.is_empty() {
            map_type_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HandlerType(*model);
                    let handler_tok = handler_ty.into_tokens();
                    parse_quote!(::swim_agent::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let sync_handler = if !lane_models.is_empty() {
            lane_models.iter().rev().fold(base, |acc, model| {
                let handler_ty = SyncHandlerType(*model);
                let handler_tok = handler_ty.into_tokens();
                parse_quote!(::swim_agent::reexport::coproduct::Coproduct<#handler_tok, #acc>)
            })
        } else {
            no_handler
        };

        let val_lane_names = value_type_models.iter().map(|model| model.model.literal());

        let map_lane_names = map_type_models.iter().map(|model| model.model.literal());

        let lane_ids = lane_models
            .iter()
            .map(|model| (model.ordinal, model.model.literal()))
            .map(|(i, lit)| quote!(::std::collections::HashMap::insert(&mut map, #i, ::swim_agent::model::Text::new(#lit))));

        let value_match_blocks = value_type_models
            .iter()
            .enumerate()
            .map(|(i, model)| LaneHandlerMatch::new(i, *model))
            .map(LaneHandlerMatch::into_tokens);

        let map_match_blocks = map_type_models
            .iter()
            .enumerate()
            .map(|(i, model)| LaneHandlerMatch::new(i, *model))
            .map(LaneHandlerMatch::into_tokens);

        let sync_match_blocks = lane_models
            .iter()
            .copied()
            .map(SyncHandlerMatch)
            .map(SyncHandlerMatch::into_tokens);

        let write_match_blocks = lane_models
            .iter()
            .copied()
            .map(|model| WriteToBufferMatch(model.model))
            .map(WriteToBufferMatch::into_tokens);

        tokens.append_all(quote! {

            #[automatically_derived]
            impl ::core::default::Default for #agent_type {
                fn default() -> Self {
                    Self {
                        #(#initializers),*
                    }
                }
            }

            #[automatically_derived]
            impl ::swim_agent::agent_model::AgentLaneModel for #agent_type {
                type ValCommandHandler = #value_handler;

                type MapCommandHandler = #map_handler;

                type OnSyncHandler = #sync_handler;

                fn value_like_lanes(&self) -> ::std::collections::HashSet<&str> {
                    let mut lanes = ::std::collections::HashSet::new();
                    #(::std::collections::HashSet::insert(&mut lanes, #val_lane_names);)*
                    lanes
                }

                fn map_like_lanes(&self) -> ::std::collections::HashSet<&str> {
                    let mut lanes = ::std::collections::HashSet::new();
                    #(::std::collections::HashSet::insert(&mut lanes, #map_lane_names);)*
                    lanes
                }

                fn lane_ids(&self) -> ::std::collections::HashMap<u64, ::swim_agent::model::Text> {
                    let mut map = ::std::collections::HashMap::new();
                    #(#lane_ids;)*
                    map
                }

                fn on_value_command(&self, lane: &str, body: ::swim_agent::reexport::bytes::BytesMut) -> ::core::option::Option<Self::ValCommandHandler> {
                    match lane {
                        #(#value_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn on_map_command(
                    &self,
                    lane: &str,
                    body: ::swim_agent::model::MapMessage<::swim_agent::reexport::bytes::BytesMut, ::swim_agent::reexport::bytes::BytesMut>,
                ) -> ::core::option::Option<Self::MapCommandHandler> {
                    match lane {
                        #(#map_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn on_sync(&self, lane: &str, id: ::swim_agent::reexport::uuid::Uuid) -> Option<Self::OnSyncHandler> {
                    match lane {
                        #(#sync_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn write_event(&self, lane: &str, buffer: &mut ::swim_agent::reexport::bytes::BytesMut) -> ::core::option::Option<::swim_agent::agent_model::WriteResult> {
                    match lane {
                        #(#write_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }
            }

        });
    }
}

#[derive(Clone, Copy)]
struct OrdinalLaneModel<'a> {
    agent_name: &'a Ident,
    ordinal: u64,
    model: LaneModel<'a>,
}

impl<'a> OrdinalLaneModel<'a> {
    fn new(agent_name: &'a Ident, ordinal: u64, model: LaneModel<'a>) -> Self {
        OrdinalLaneModel {
            agent_name,
            ordinal,
            model,
        }
    }

    fn map_like(&self) -> bool {
        matches!(&self.model.kind, LaneKind::Map(_, _))
    }
}

struct FieldInitializer<'a>(OrdinalLaneModel<'a>);

impl<'a> FieldInitializer<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let FieldInitializer(OrdinalLaneModel {
            ordinal,
            model: LaneModel { name, kind, .. },
            ..
        }) = self;

        match kind {
            LaneKind::Command(_) => {
                quote!(#name: ::swim_agent::lanes::CommandLane::new(#ordinal))
            }
            LaneKind::Value(_) => {
                quote!(#name: ::swim_agent::lanes::ValueLane::new(#ordinal, ::core::default::Default::default()))
            }
            LaneKind::Map(_, _) => {
                quote!(#name: ::swim_agent::lanes::MapLane::new(#ordinal, ::core::default::Default::default()))
            }
        }
    }
}

struct HandlerType<'a>(OrdinalLaneModel<'a>);

struct SyncHandlerType<'a>(OrdinalLaneModel<'a>);

impl<'a> HandlerType<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let HandlerType(OrdinalLaneModel {
            agent_name,
            model: LaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            LaneKind::Command(t) => {
                quote!(::swim_agent::lanes::command::DecodeAndCommand<#agent_name, #t>)
            }
            LaneKind::Value(t) => {
                quote!(::swim_agent::lanes::value::DecodeAndSet<#agent_name, #t>)
            }
            LaneKind::Map(k, v) => {
                quote!(::swim_agent::lanes::map::DecodeAndApply<#agent_name, #k, #v>)
            }
        }
    }
}

impl<'a> SyncHandlerType<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let SyncHandlerType(OrdinalLaneModel {
            agent_name,
            model: LaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            LaneKind::Command(_) => quote!(::swim_agent::event_handler::UnitHandler), //TODO Do this properly later.
            LaneKind::Value(t) => {
                quote!(::swim_agent::lanes::value::ValueLaneSync<#agent_name, #t>)
            }
            LaneKind::Map(k, v) => {
                quote!(::swim_agent::lanes::map::MapLaneSync<#agent_name, #k, #v>)
            }
        }
    }
}

struct LaneHandlerMatch<'a> {
    group_ordinal: usize,
    model: OrdinalLaneModel<'a>,
}

impl<'a> LaneHandlerMatch<'a> {
    fn new(group_ordinal: usize, model: OrdinalLaneModel<'a>) -> Self {
        LaneHandlerMatch {
            group_ordinal,
            model,
        }
    }

    fn into_tokens(self) -> impl ToTokens {
        let LaneHandlerMatch {
            group_ordinal,
            model: OrdinalLaneModel {
                agent_name, model, ..
            },
        } = self;
        let name_lit = model.literal();
        let LaneModel { name, kind } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(handler_base, group_ordinal);
        let lane_handler_expr = match kind {
            LaneKind::Command(ty) => {
                quote!(::swim_agent::lanes::command::decode_and_command::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            LaneKind::Value(ty) => {
                quote!(::swim_agent::lanes::value::decode_and_set::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            LaneKind::Map(k, v) => {
                quote!(::swim_agent::lanes::map::decode_and_apply::<#agent_name, #k, #v>(body, |agent: &#agent_name| &agent.#name))
            }
        };
        quote! {
            #name_lit => {
                let handler = #lane_handler_expr;
                ::core::option::Option::Some(#coprod_con)
            }
        }
    }
}

fn coproduct_constructor(expr: syn::Expr, n: usize) -> syn::Expr {
    let mut acc: syn::Expr = parse_quote!(::swim_agent::reexport::coproduct::Coproduct::Inl(#expr));
    for _ in 0..n {
        acc = parse_quote!(::swim_agent::reexport::coproduct::Coproduct::Inr(#acc))
    }
    acc
}

struct SyncHandlerMatch<'a>(OrdinalLaneModel<'a>);

impl<'a> SyncHandlerMatch<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let SyncHandlerMatch(OrdinalLaneModel {
            agent_name,
            ordinal,
            model,
            ..
        }) = self;
        let name_lit = model.literal();
        let LaneModel { name, kind } = model;
        let ord = ordinal as usize;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(handler_base, ord);
        let sync_handler_expr = match kind {
            LaneKind::Command(_) => {
                quote!(::swim_agent::event_handler::UnitHandler::default())
            }
            LaneKind::Value(ty) => {
                quote!(::swim_agent::lanes::value::ValueLaneSync::<#agent_name, #ty>::new(|agent: &#agent_name| &agent.#name, id))
            }
            LaneKind::Map(k, v) => {
                quote!(::swim_agent::lanes::map::MapLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
            }
        };
        quote! {
            #name_lit => {
                let handler = #sync_handler_expr;
                ::core::option::Option::Some(#coprod_con)
            }
        }
    }
}

struct WriteToBufferMatch<'a>(LaneModel<'a>);

impl<'a> WriteToBufferMatch<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let WriteToBufferMatch(model) = self;
        let name_lit = model.literal();
        let LaneModel { name, .. } = model;
        quote!(#name_lit => ::core::option::Option::Some(swim_agent::lanes::Lane::write_to_buffer(&self.#name, buffer)))
    }
}
