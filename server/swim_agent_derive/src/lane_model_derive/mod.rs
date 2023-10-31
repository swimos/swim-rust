// Copyright 2015-2023 Swim Inc.
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

mod attributes;
mod model;

pub use attributes::{combine_agent_attrs, make_agent_attr_consumer};
pub use model::{validate_input, ItemModel, ItemSpec, LanesModel};

use self::{
    attributes::AgentModifiers,
    model::{HttpLaneModel, HttpLaneSpec, ItemKind, WarpLaneModel, WarpLaneSpec},
};

pub struct DeriveAgentLaneModel<'a> {
    root: syn::Path,
    model: LanesModel<'a>,
}

impl<'a> DeriveAgentLaneModel<'a> {
    pub fn new(modifiers: AgentModifiers, mut model: LanesModel<'a>) -> Self {
        let AgentModifiers {
            transient,
            transform,
            root,
        } = modifiers;
        model.apply_modifiers(transient, transform.into());
        DeriveAgentLaneModel { root, model }
    }
}

impl<'a> ToTokens for DeriveAgentLaneModel<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveAgentLaneModel {
            ref root,
            model: LanesModel {
                agent_type,
                ref lanes,
            },
        } = *self;

        let item_models = lanes
            .iter()
            .zip(0u64..)
            .map(|(model, i)| OrdinalItemModel::new(agent_type, i, model.clone()))
            .collect::<Vec<_>>();

        let initializers = item_models
            .iter()
            .cloned()
            .map(FieldInitializer)
            .map(|init| init.into_tokens(root));

        let base: syn::Type = parse_quote!(#root::reexport::coproduct::CNil);

        let no_handler: syn::Type = parse_quote!(#root::event_handler::UnitHandler);

        let (value_item_models, map_item_models) =
            partition_models(item_models.iter().cloned(), OrdinalItemModel::category);

        let warp_lane_models = OrdinalWarpLaneModel::from_item_models(&item_models);

        let http_lane_models = OrdinalHttpLaneModel::from_item_models(&item_models);

        let (map_lane_models, value_lane_models) = warp_lane_models
            .iter()
            .cloned()
            .partition::<Vec<_>, _>(OrdinalWarpLaneModel::map_like);

        let value_handler = if !value_lane_models.is_empty() {
            value_lane_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HandlerType(model.clone());
                    let handler_tok = handler_ty.into_tokens(root);
                    parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let map_handler = if !map_lane_models.is_empty() {
            map_lane_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HandlerType(model.clone());
                    let handler_tok = handler_ty.into_tokens(root);
                    parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let http_handler = if !http_lane_models.is_empty() {
            http_lane_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HttpHandlerType(model.clone());
                    let handler_tok = handler_ty.into_tokens(root);
                    parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let sync_handler = if !warp_lane_models.is_empty() {
            warp_lane_models.iter().rev().fold(base, |acc, model| {
                let handler_ty = SyncHandlerType(model.clone());
                let handler_tok = handler_ty.into_tokens(root);
                parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
            })
        } else {
            no_handler
        };

        let item_specs = item_models
            .iter()
            .map(|model| LaneSpecInsert(model.ordinal, model.model.clone()))
            .map(|insert| insert.into_tokens(root));

        let value_match_blocks = value_lane_models
            .iter()
            .enumerate()
            .map(|(i, model)| WarpLaneHandlerMatch::new(i, model.clone()))
            .map(|hmatch| hmatch.into_tokens(root));

        let map_match_blocks = map_lane_models
            .iter()
            .enumerate()
            .map(|(i, model)| WarpLaneHandlerMatch::new(i, model.clone()))
            .map(|hmatch| hmatch.into_tokens(root));

        let http_match_blocks = http_lane_models
            .iter()
            .map(|model| HttpLaneHandlerMatch::new(model.clone()))
            .map(|hmatch| hmatch.into_tokens(root));

        let sync_match_blocks = warp_lane_models
            .iter()
            .cloned()
            .map(|model| SyncHandlerMatch::new(root, model))
            .map(SyncHandlerMatch::into_tokens);

        let write_match_blocks = item_models
            .iter()
            .filter(|m| m.category() != ItemCategory::Http)
            .cloned()
            .map(|model| WriteToBufferMatch(model.model))
            .map(|wmatch| wmatch.into_tokens(root));

        let value_init_match_blocks = value_item_models
            .iter()
            .filter(|model| model.model.is_stateful())
            .map(ValueItemInitMatch::new)
            .map(|model| model.into_tokens(root));

        let map_init_match_blocks = map_item_models
            .iter()
            .filter(|model| model.model.is_stateful())
            .map(MapItemInitMatch::new)
            .map(|model| model.into_tokens(root));

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
            impl #root::agent_model::AgentSpec for #agent_type {
                type ValCommandHandler = #value_handler;

                type MapCommandHandler = #map_handler;

                type OnSyncHandler = #sync_handler;

                type HttpRequestHandler = #http_handler;

                fn item_specs() -> ::std::collections::HashMap<&'static str, #root::agent_model::ItemSpec> {
                    let mut lanes = ::std::collections::HashMap::new();
                    #(#item_specs;)*
                    lanes
                }

                fn on_value_command(&self, lane: &str, body: #root::reexport::bytes::BytesMut) -> ::core::option::Option<Self::ValCommandHandler> {
                    match lane {
                        #(#value_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn on_map_command(
                    &self,
                    lane: &str,
                    body: #root::model::MapMessage<#root::reexport::bytes::BytesMut, #root::reexport::bytes::BytesMut>,
                ) -> ::core::option::Option<Self::MapCommandHandler> {
                    match lane {
                        #(#map_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn on_sync(&self, lane: &str, id: #root::reexport::uuid::Uuid) -> Option<Self::OnSyncHandler> {
                    match lane {
                        #(#sync_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn on_http_request(
                    &self,
                    lane: &str,
                    request: #root::model::HttpLaneRequest,
                ) -> Result<Self::HttpRequestHandler, #root::model::HttpLaneRequest> {
                    match lane {
                        #(#http_match_blocks,)*
                        _ => ::core::result::Result::Err(request)
                    }
                }

                fn write_event(&self, lane: &str, buffer: &mut #root::reexport::bytes::BytesMut) -> ::core::option::Option<#root::agent_model::WriteResult> {
                    match lane {
                        #(#write_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn init_value_like_item(
                    &self,
                    item: &str,
                ) -> ::core::option::Option<::std::boxed::Box<dyn #root::agent_model::ItemInitializer<Self, #root::reexport::bytes::BytesMut> + ::core::marker::Send + 'static>>
                where
                    Self: 'static,
                {
                    match item {
                        #(#value_init_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }

                fn init_map_like_item(
                    &self,
                    item: &str,
                ) -> ::core::option::Option<::std::boxed::Box<dyn #root::agent_model::ItemInitializer<Self, #root::model::MapMessage<#root::reexport::bytes::BytesMut, #root::reexport::bytes::BytesMut>> + ::core::marker::Send + 'static>>
                where
                    Self: 'static,
                {
                    match item {
                        #(#map_init_match_blocks,)*
                        _ => ::core::option::Option::None,
                    }
                }
            }

        });
    }
}

#[derive(Clone)]
struct OrdinalItemModel<'a> {
    agent_name: &'a Ident,
    ordinal: u64,
    model: ItemModel<'a>,
}

#[derive(Clone)]
struct OrdinalWarpLaneModel<'a> {
    agent_name: &'a Ident,
    lane_ordinal: usize,
    model: WarpLaneModel<'a>,
}

#[derive(Clone)]
struct OrdinalHttpLaneModel<'a> {
    agent_name: &'a Ident,
    lane_ordinal: usize,
    model: HttpLaneModel<'a>,
}

impl<'a> OrdinalWarpLaneModel<'a> {
    pub fn from_item_models(models: &[OrdinalItemModel<'a>]) -> Vec<OrdinalWarpLaneModel<'a>> {
        models
            .iter()
            .cloned()
            .filter_map(
                |OrdinalItemModel {
                     agent_name, model, ..
                 }| model.lane().map(move |lane_model| (agent_name, lane_model)),
            )
            .enumerate()
            .map(|(lane_ordinal, (agent_name, model))| OrdinalWarpLaneModel {
                agent_name,
                lane_ordinal,
                model,
            })
            .collect()
    }

    pub fn map_like(&self) -> bool {
        matches!(
            &self.model.kind,
            WarpLaneSpec::Map(_, _)
                | WarpLaneSpec::DemandMap(_, _)
                | WarpLaneSpec::JoinValue(_, _)
                | WarpLaneSpec::JoinMap(_, _, _)
        )
    }
}

impl<'a> OrdinalHttpLaneModel<'a> {
    pub fn from_item_models(models: &[OrdinalItemModel<'a>]) -> Vec<OrdinalHttpLaneModel<'a>> {
        models
            .iter()
            .cloned()
            .filter_map(
                |OrdinalItemModel {
                     agent_name, model, ..
                 }| model.http().map(move |lane_model| (agent_name, lane_model)),
            )
            .enumerate()
            .map(|(lane_ordinal, (agent_name, model))| OrdinalHttpLaneModel {
                agent_name,
                lane_ordinal,
                model,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ItemCategory {
    ValueLike,
    MapLike,
    Http,
}

impl<'a> OrdinalItemModel<'a> {
    fn new(agent_name: &'a Ident, ordinal: u64, model: ItemModel<'a>) -> Self {
        OrdinalItemModel {
            agent_name,
            ordinal,
            model,
        }
    }

    fn category(&self) -> ItemCategory {
        match &self.model.kind {
            ItemSpec::Map(_, _, _)
            | ItemSpec::JoinValue(_, _)
            | ItemSpec::JoinMap(_, _, _)
            | ItemSpec::DemandMap(_, _) => ItemCategory::MapLike,
            ItemSpec::Http(_) => ItemCategory::Http,
            _ => ItemCategory::ValueLike,
        }
    }
}

struct FieldInitializer<'a>(OrdinalItemModel<'a>);

impl<'a> FieldInitializer<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let FieldInitializer(OrdinalItemModel {
            ordinal,
            model: ItemModel { name, kind, .. },
            ..
        }) = self;

        match kind {
            ItemSpec::Command(_) => {
                quote!(#name: #root::lanes::CommandLane::new(#ordinal))
            }
            ItemSpec::Demand(_) => {
                quote!(#name: #root::lanes::DemandLane::new(#ordinal))
            }
            ItemSpec::DemandMap(_, _) => {
                quote!(#name: #root::lanes::DemandMapLane::new(#ordinal))
            }
            ItemSpec::Value(ItemKind::Lane, _) => {
                quote!(#name: #root::lanes::ValueLane::new(#ordinal, ::core::default::Default::default()))
            }
            ItemSpec::Value(ItemKind::Store, _) => {
                quote!(#name: #root::stores::ValueStore::new(#ordinal, ::core::default::Default::default()))
            }
            ItemSpec::Map(ItemKind::Lane, _, _) => {
                quote!(#name: #root::lanes::MapLane::new(#ordinal, ::core::default::Default::default()))
            }
            ItemSpec::Map(ItemKind::Store, _, _) => {
                quote!(#name: #root::stores::MapStore::new(#ordinal, ::core::default::Default::default()))
            }
            ItemSpec::JoinValue(_, _) => {
                quote!(#name: #root::lanes::JoinValueLane::new(#ordinal))
            }
            ItemSpec::JoinMap(_, _, _) => {
                quote!(#name: #root::lanes::JoinMapLane::new(#ordinal))
            }
            ItemSpec::Http { .. } => {
                quote!(#name: #root::lanes::HttpLane::new(#ordinal))
            }
        }
    }
}

struct HandlerType<'a>(OrdinalWarpLaneModel<'a>);

struct SyncHandlerType<'a>(OrdinalWarpLaneModel<'a>);

struct HttpHandlerType<'a>(OrdinalHttpLaneModel<'a>);

impl<'a> HandlerType<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let HandlerType(OrdinalWarpLaneModel {
            agent_name,
            model: WarpLaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            WarpLaneSpec::Command(t) => {
                quote!(#root::lanes::command::DecodeAndCommand<#agent_name, #t>)
            }
            WarpLaneSpec::Value(t) => {
                quote!(#root::lanes::value::DecodeAndSet<#agent_name, #t>)
            }
            WarpLaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::DecodeAndApply<#agent_name, #k, #v>)
            }
            WarpLaneSpec::Demand(_)
            | WarpLaneSpec::DemandMap(_, _)
            | WarpLaneSpec::JoinValue(_, _)
            | WarpLaneSpec::JoinMap(_, _, _) => {
                quote!(#root::event_handler::UnitHandler)
            }
        }
    }
}

impl<'a> SyncHandlerType<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let SyncHandlerType(OrdinalWarpLaneModel {
            agent_name,
            model: WarpLaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            WarpLaneSpec::Command(_) => quote!(#root::event_handler::UnitHandler), //TODO Do this properly later.
            WarpLaneSpec::Demand(t) => {
                quote!(#root::lanes::demand::DemandLaneSync<#agent_name, #t>)
            }
            WarpLaneSpec::DemandMap(k, v) => {
                quote!(#root::lanes::demand_map::DemandMapLaneSync<#agent_name, #k, #v>)
            }
            WarpLaneSpec::Value(t) => {
                quote!(#root::lanes::value::ValueLaneSync<#agent_name, #t>)
            }
            WarpLaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::MapLaneSync<#agent_name, #k, #v>)
            }
            WarpLaneSpec::JoinValue(k, v) => {
                quote!(#root::lanes::join_value::JoinValueLaneSync<#agent_name, #k, #v>)
            }
            WarpLaneSpec::JoinMap(l, k, v) => {
                quote!(#root::lanes::join_map::JoinMapLaneSync<#agent_name, #l, #k, #v>)
            }
        }
    }
}

impl<'a> HttpHandlerType<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let HttpHandlerType(OrdinalHttpLaneModel {
            agent_name,
            model: HttpLaneModel { kind, .. },
            ..
        }) = self;

        let HttpLaneSpec {
            get,
            post,
            put,
            codec,
        } = kind;
        quote!(#root::lanes::http::HttpLaneAccept<#agent_name, #get, #post, #put, #codec>)
    }
}

struct WarpLaneHandlerMatch<'a> {
    group_ordinal: usize,
    model: OrdinalWarpLaneModel<'a>,
}

impl<'a> WarpLaneHandlerMatch<'a> {
    fn new(group_ordinal: usize, model: OrdinalWarpLaneModel<'a>) -> Self {
        WarpLaneHandlerMatch {
            group_ordinal,
            model,
        }
    }

    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let WarpLaneHandlerMatch {
            group_ordinal,
            model: OrdinalWarpLaneModel {
                agent_name, model, ..
            },
        } = self;
        let name_lit = model.literal();
        let WarpLaneModel { name, kind, .. } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(root, handler_base, group_ordinal);
        let lane_handler_expr = match kind {
            WarpLaneSpec::Command(ty) => {
                quote!(#root::lanes::command::decode_and_command::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            WarpLaneSpec::Value(ty) => {
                quote!(#root::lanes::value::decode_and_set::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            WarpLaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::decode_and_apply::<#agent_name, #k, #v>(body, |agent: &#agent_name| &agent.#name))
            }
            WarpLaneSpec::Demand(_)
            | WarpLaneSpec::DemandMap(_, _)
            | WarpLaneSpec::JoinValue(_, _)
            | WarpLaneSpec::JoinMap(_, _, _) => {
                quote!(#root::event_handler::UnitHandler::default())
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

fn coproduct_constructor(root: &syn::Path, expr: syn::Expr, n: usize) -> syn::Expr {
    let mut acc: syn::Expr = parse_quote!(#root::reexport::coproduct::Coproduct::Inl(#expr));
    for _ in 0..n {
        acc = parse_quote!(#root::reexport::coproduct::Coproduct::Inr(#acc))
    }
    acc
}

struct HttpLaneHandlerMatch<'a> {
    model: OrdinalHttpLaneModel<'a>,
}

impl<'a> HttpLaneHandlerMatch<'a> {
    pub fn new(model: OrdinalHttpLaneModel<'a>) -> Self {
        HttpLaneHandlerMatch { model }
    }

    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let HttpLaneHandlerMatch {
            model:
                OrdinalHttpLaneModel {
                    agent_name,
                    lane_ordinal,
                    model,
                },
        } = self;
        let name_lit = model.literal();
        let HttpLaneModel { name, kind, .. } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(root, handler_base, lane_ordinal);
        let HttpLaneSpec {
            get,
            post,
            put,
            codec,
        } = kind;
        let lane_handler_expr = quote!(#root::lanes::http::HttpLaneAccept::<#agent_name, #get, #post, #put, #codec>::new(|agent: &#agent_name| &agent.#name, request));
        quote! {
            #name_lit => {
                let handler = #lane_handler_expr;
                ::core::result::Result::Ok(#coprod_con)
            }
        }
    }
}

struct SyncHandlerMatch<'a> {
    root: &'a syn::Path,
    model: OrdinalWarpLaneModel<'a>,
}

impl<'a> SyncHandlerMatch<'a> {
    fn new(root: &'a syn::Path, model: OrdinalWarpLaneModel<'a>) -> Self {
        SyncHandlerMatch { root, model }
    }
}

impl<'a> SyncHandlerMatch<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let SyncHandlerMatch {
            root,
            model:
                OrdinalWarpLaneModel {
                    agent_name,
                    lane_ordinal: ord,
                    model,
                    ..
                },
        } = self;
        let name_lit = model.literal();
        let WarpLaneModel { name, kind, .. } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(root, handler_base, ord);
        let sync_handler_expr = match kind {
            WarpLaneSpec::Command(_) => quote!(#root::event_handler::UnitHandler::default()),
            WarpLaneSpec::Demand(ty) => {
                quote!(#root::lanes::demand::DemandLaneSync::<#agent_name, #ty>::new(|agent: &#agent_name| &agent.#name, id))
            }
            WarpLaneSpec::DemandMap(k, v) => {
                quote!(#root::lanes::demand_map::DemandMapLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
            }
            WarpLaneSpec::Value(ty) => {
                quote!(#root::lanes::value::ValueLaneSync::<#agent_name, #ty>::new(|agent: &#agent_name| &agent.#name, id))
            }
            WarpLaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::MapLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
            }
            WarpLaneSpec::JoinValue(k, v) => {
                quote!(#root::lanes::join_value::JoinValueLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
            }
            WarpLaneSpec::JoinMap(l, k, v) => {
                quote!(#root::lanes::join_map::JoinMapLaneSync::<#agent_name, #l, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
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

struct WriteToBufferMatch<'a>(ItemModel<'a>);

impl<'a> WriteToBufferMatch<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let WriteToBufferMatch(model) = self;
        let name_lit = model.external_literal();
        let ItemModel { name, kind, .. } = model;
        match kind.item_kind() {
            ItemKind::Lane => {
                quote!(#name_lit => ::core::option::Option::Some(#root::lanes::LaneItem::write_to_buffer(&self.#name, buffer)))
            }
            ItemKind::Store => {
                quote!(#name_lit => ::core::option::Option::Some(#root::stores::StoreItem::write_to_buffer(&self.#name, buffer)))
            }
        }
    }
}

struct ValueItemInitMatch<'a> {
    agent_name: &'a Ident,
    name: &'a Ident,
    name_lit: proc_macro2::Literal,
    kind: ItemKind,
}

impl<'a> ValueItemInitMatch<'a> {
    pub fn new(item: &OrdinalItemModel<'a>) -> Self {
        ValueItemInitMatch {
            agent_name: item.agent_name,
            name: item.model.name,
            name_lit: item.model.external_literal(),
            kind: item.model.item_kind(),
        }
    }
}

impl<'a> ValueItemInitMatch<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let ValueItemInitMatch {
            agent_name,
            name,
            name_lit,
            kind,
        } = self;
        match kind {
            ItemKind::Lane => {
                quote!(#name_lit => ::core::option::Option::Some(::std::boxed::Box::new(#root::agent_model::ValueLaneInitializer::new(|agent: &#agent_name| &agent.#name))))
            }
            ItemKind::Store => {
                quote!(#name_lit => ::core::option::Option::Some(::std::boxed::Box::new(#root::agent_model::ValueStoreInitializer::new(|agent: &#agent_name| &agent.#name))))
            }
        }
    }
}

enum InitKind {
    MapLane,
    MapStore,
}

struct MapItemInitMatch<'a> {
    agent_name: &'a Ident,
    name: &'a Ident,
    name_lit: proc_macro2::Literal,
    init_kind: InitKind,
}

impl<'a> MapItemInitMatch<'a> {
    pub fn new(item: &OrdinalItemModel<'a>) -> Self {
        let init_kind = match &item.model.kind {
            ItemSpec::Map(ItemKind::Lane, _, _) => InitKind::MapLane,
            _ => InitKind::MapStore,
        };
        MapItemInitMatch {
            agent_name: item.agent_name,
            name: item.model.name,
            name_lit: item.model.external_literal(),
            init_kind,
        }
    }
}

impl<'a> MapItemInitMatch<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let MapItemInitMatch {
            agent_name,
            name,
            name_lit,
            init_kind,
        } = self;
        match init_kind {
            InitKind::MapLane => {
                quote!(#name_lit => ::core::option::Option::Some(::std::boxed::Box::new(#root::agent_model::MapLaneInitializer::new(|agent: &#agent_name| &agent.#name))))
            }
            InitKind::MapStore => {
                quote!(#name_lit => ::core::option::Option::Some(::std::boxed::Box::new(#root::agent_model::MapStoreInitializer::new(|agent: &#agent_name| &agent.#name))))
            }
        }
    }
}

struct LaneSpecInsert<'a>(u64, ItemModel<'a>);

impl<'a> LaneSpecInsert<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let LaneSpecInsert(ordinal, model) = self;

        let flags = if model.is_stateful() {
            quote!(#root::agent_model::ItemFlags::empty())
        } else {
            quote!(#root::agent_model::ItemFlags::TRANSIENT)
        };
        let descriptor = match model.kind {
            ItemSpec::Command(_) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::Command, flags: #flags })
            }
            ItemSpec::Demand(_) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::Demand, flags: #flags })
            }
            ItemSpec::DemandMap(_, _) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::DemandMap, flags: #flags })
            }
            ItemSpec::Value(ItemKind::Lane, _) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::Value, flags: #flags })
            }
            ItemSpec::Value(ItemKind::Store, _) => {
                quote!(#root::agent_model::ItemDescriptor::Store { kind: #root::agent_model::StoreKind::Value, flags: #flags })
            }
            ItemSpec::Map(ItemKind::Lane, _, _) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::Map, flags: #flags })
            }
            ItemSpec::Map(ItemKind::Store, _, _) => {
                quote!(#root::agent_model::ItemDescriptor::Store { kind: #root::agent_model::StoreKind::Map, flags: #flags })
            }
            ItemSpec::JoinValue(_, _) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::JoinValue, flags: #flags })
            }
            ItemSpec::JoinMap(_, _, _) => {
                quote!(#root::agent_model::ItemDescriptor::WarpLane { kind: #root::agent_model::WarpLaneKind::JoinMap, flags: #flags })
            }
            ItemSpec::Http(_) => quote!(#root::agent_model::ItemDescriptor::Http),
        };
        let external_lane_name = model.external_literal();
        let lifecycle_lane_name = model.lifecycle_literal();
        let spec =
            quote!(#root::agent_model::ItemSpec::new(#ordinal, #lifecycle_lane_name, #descriptor));
        quote!(::std::collections::HashMap::insert(&mut lanes, #external_lane_name, #spec))
    }
}

fn partition_models<T, F>(it: impl Iterator<Item = T>, f: F) -> (Vec<T>, Vec<T>)
where
    F: Fn(&T) -> ItemCategory,
{
    it.fold((vec![], vec![]), |mut acc, item| {
        let (value_like, map_like) = &mut acc;
        match f(&item) {
            ItemCategory::ValueLike => value_like.push(item),
            ItemCategory::MapLike => map_like.push(item),
            ItemCategory::Http => {}
        }
        acc
    })
}
