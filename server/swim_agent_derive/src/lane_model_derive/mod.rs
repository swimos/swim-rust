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

mod model;

pub use model::{validate_input, ItemModel, ItemSpec, LanesModel};

use self::model::{ItemKind, LaneModel, LaneSpec};

pub struct DeriveAgentLaneModel<'a> {
    root: &'a syn::Path,
    model: LanesModel<'a>,
}

impl<'a> DeriveAgentLaneModel<'a> {
    pub fn new(root: &'a syn::Path, model: LanesModel<'a>) -> Self {
        DeriveAgentLaneModel { root, model }
    }
}

impl<'a> ToTokens for DeriveAgentLaneModel<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveAgentLaneModel {
            root,
            model: LanesModel {
                agent_type,
                ref lanes,
            },
        } = *self;

        let item_models = lanes
            .iter()
            .zip(0u64..)
            .map(|(model, i)| OrdinalItemModel::new(agent_type, i, *model))
            .collect::<Vec<_>>();

        let initializers = item_models
            .iter()
            .copied()
            .map(FieldInitializer)
            .map(|init| init.into_tokens(root));

        let base: syn::Type = parse_quote!(#root::reexport::coproduct::CNil);

        let no_handler: syn::Type = parse_quote!(#root::event_handler::UnitHandler);

        let (map_item_models, value_item_models) = item_models
            .iter()
            .copied()
            .partition::<Vec<_>, _>(OrdinalItemModel::map_like);

        let lane_models = OrdinalLaneModel::from_item_models(&item_models);

        let (map_lane_models, value_lane_models) = lane_models
            .iter()
            .copied()
            .partition::<Vec<_>, _>(OrdinalLaneModel::map_like);

        let value_handler = if !value_lane_models.is_empty() {
            value_lane_models
                .iter()
                .rev()
                .fold(base.clone(), |acc, model| {
                    let handler_ty = HandlerType(*model);
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
                    let handler_ty = HandlerType(*model);
                    let handler_tok = handler_ty.into_tokens(root);
                    parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
                })
        } else {
            no_handler.clone()
        };

        let sync_handler = if !lane_models.is_empty() {
            lane_models.iter().rev().fold(base, |acc, model| {
                let handler_ty = SyncHandlerType(*model);
                let handler_tok = handler_ty.into_tokens(root);
                parse_quote!(#root::reexport::coproduct::Coproduct<#handler_tok, #acc>)
            })
        } else {
            no_handler
        };

        let item_specs = item_models
            .iter()
            .map(|model| LaneSpecInsert(model.model))
            .map(|insert| insert.into_tokens(root));

        let lane_ids = item_models
            .iter()
            .map(|model| (model.ordinal, model.model.literal()))
            .map(|(i, lit)| quote!(::std::collections::HashMap::insert(&mut map, #i, #root::model::Text::new(#lit))));

        let value_match_blocks = value_lane_models
            .iter()
            .enumerate()
            .map(|(i, model)| LaneHandlerMatch::new(i, *model))
            .map(|hmatch| hmatch.into_tokens(root));

        let map_match_blocks = map_lane_models
            .iter()
            .enumerate()
            .map(|(i, model)| LaneHandlerMatch::new(i, *model))
            .map(|hmatch| hmatch.into_tokens(root));

        let sync_match_blocks = lane_models
            .iter()
            .copied()
            .map(|model| SyncHandlerMatch::new(root, model))
            .map(SyncHandlerMatch::into_tokens);

        let write_match_blocks = item_models
            .iter()
            .copied()
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

                fn item_specs() -> ::std::collections::HashMap<&'static str, #root::agent_model::ItemSpec> {
                    let mut lanes = ::std::collections::HashMap::new();
                    #(#item_specs;)*
                    lanes
                }

                fn item_ids() -> ::std::collections::HashMap<u64, #root::model::Text> {
                    let mut map = ::std::collections::HashMap::new();
                    #(#lane_ids;)*
                    map
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

#[derive(Clone, Copy)]
struct OrdinalItemModel<'a> {
    agent_name: &'a Ident,
    ordinal: u64,
    model: ItemModel<'a>,
}

#[derive(Clone, Copy)]
struct OrdinalLaneModel<'a> {
    agent_name: &'a Ident,
    lane_ordinal: usize,
    model: LaneModel<'a>,
}

impl<'a> OrdinalLaneModel<'a> {
    pub fn from_item_models(models: &[OrdinalItemModel<'a>]) -> Vec<OrdinalLaneModel<'a>> {
        models
            .iter()
            .copied()
            .filter_map(
                |OrdinalItemModel {
                     agent_name, model, ..
                 }| model.lane().map(move |lane_model| (agent_name, lane_model)),
            )
            .enumerate()
            .map(|(lane_ordinal, (agent_name, model))| OrdinalLaneModel {
                agent_name,
                lane_ordinal,
                model,
            })
            .collect()
    }

    pub fn map_like(&self) -> bool {
        matches!(
            &self.model.kind,
            LaneSpec::Map(_, _) | LaneSpec::JoinValue(_, _)
        )
    }
}

impl<'a> OrdinalItemModel<'a> {
    fn new(agent_name: &'a Ident, ordinal: u64, model: ItemModel<'a>) -> Self {
        OrdinalItemModel {
            agent_name,
            ordinal,
            model,
        }
    }

    fn map_like(&self) -> bool {
        matches!(
            &self.model.kind,
            ItemSpec::Map(_, _, _) | ItemSpec::JoinValue(_, _)
        )
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
        }
    }
}

struct HandlerType<'a>(OrdinalLaneModel<'a>);

struct SyncHandlerType<'a>(OrdinalLaneModel<'a>);

impl<'a> HandlerType<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let HandlerType(OrdinalLaneModel {
            agent_name,
            model: LaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            LaneSpec::Command(t) => {
                quote!(#root::lanes::command::DecodeAndCommand<#agent_name, #t>)
            }
            LaneSpec::Value(t) => {
                quote!(#root::lanes::value::DecodeAndSet<#agent_name, #t>)
            }
            LaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::DecodeAndApply<#agent_name, #k, #v>)
            }
            LaneSpec::JoinValue(_, _) => quote!(#root::event_handler::UnitHandler),
        }
    }
}

impl<'a> SyncHandlerType<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let SyncHandlerType(OrdinalLaneModel {
            agent_name,
            model: LaneModel { kind, .. },
            ..
        }) = self;

        match kind {
            LaneSpec::Command(_) => quote!(#root::event_handler::UnitHandler), //TODO Do this properly later.
            LaneSpec::Value(t) => {
                quote!(#root::lanes::value::ValueLaneSync<#agent_name, #t>)
            }
            LaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::MapLaneSync<#agent_name, #k, #v>)
            }
            LaneSpec::JoinValue(k, v) => {
                quote!(#root::lanes::join_value::JoinValueLaneSync<#agent_name, #k, #v>)
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

    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let LaneHandlerMatch {
            group_ordinal,
            model: OrdinalLaneModel {
                agent_name, model, ..
            },
        } = self;
        let name_lit = model.literal();
        let LaneModel { name, kind, .. } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(root, handler_base, group_ordinal);
        let lane_handler_expr = match kind {
            LaneSpec::Command(ty) => {
                quote!(#root::lanes::command::decode_and_command::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            LaneSpec::Value(ty) => {
                quote!(#root::lanes::value::decode_and_set::<#agent_name, #ty>(body, |agent: &#agent_name| &agent.#name))
            }
            LaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::decode_and_apply::<#agent_name, #k, #v>(body, |agent: &#agent_name| &agent.#name))
            }
            LaneSpec::JoinValue(_, _) => {
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

struct SyncHandlerMatch<'a> {
    root: &'a syn::Path,
    model: OrdinalLaneModel<'a>,
}

impl<'a> SyncHandlerMatch<'a> {
    fn new(root: &'a syn::Path, model: OrdinalLaneModel<'a>) -> Self {
        SyncHandlerMatch { root, model }
    }
}

impl<'a> SyncHandlerMatch<'a> {
    fn into_tokens(self) -> impl ToTokens {
        let SyncHandlerMatch {
            root,
            model:
                OrdinalLaneModel {
                    agent_name,
                    lane_ordinal: ord,
                    model,
                    ..
                },
        } = self;
        let name_lit = model.literal();
        let LaneModel { name, kind, .. } = model;
        let handler_base: syn::Expr = parse_quote!(handler);
        let coprod_con = coproduct_constructor(root, handler_base, ord);
        let sync_handler_expr = match kind {
            LaneSpec::Command(_) => quote!(#root::event_handler::UnitHandler::default()),
            LaneSpec::Value(ty) => {
                quote!(#root::lanes::value::ValueLaneSync::<#agent_name, #ty>::new(|agent: &#agent_name| &agent.#name, id))
            }
            LaneSpec::Map(k, v) => {
                quote!(#root::lanes::map::MapLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
            }
            LaneSpec::JoinValue(k, v) => {
                quote!(#root::lanes::join_value::JoinValueLaneSync::<#agent_name, #k, #v>::new(|agent: &#agent_name| &agent.#name, id))
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
        let name_lit = model.literal();
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
            name_lit: item.model.literal(),
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
    JoinValueLane,
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
            ItemSpec::Map(ItemKind::Store, _, _) => InitKind::MapStore,
            _ => InitKind::JoinValueLane,
        };
        MapItemInitMatch {
            agent_name: item.agent_name,
            name: item.model.name,
            name_lit: item.model.literal(),
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
            InitKind::JoinValueLane => {
                quote!(#name_lit => ::core::option::Option::Some(::std::boxed::Box::new(#root::agent_model::JoinValueInitializer::new(|agent: &#agent_name| &agent.#name))))
            }
        }
    }
}

struct LaneSpecInsert<'a>(ItemModel<'a>);

impl<'a> LaneSpecInsert<'a> {
    fn into_tokens(self, root: &syn::Path) -> impl ToTokens {
        let LaneSpecInsert(model) = self;

        let item_kind = match model.kind {
            ItemSpec::Command(_) => {
                quote!(#root::agent_model::ItemKind::Lane(#root::agent_model::LaneKind::Command))
            }
            ItemSpec::Value(ItemKind::Store, _) => {
                quote!(#root::agent_model::ItemKind::Store(#root::agent_model::StoreKind::Value))
            }
            ItemSpec::Value(ItemKind::Lane, _) => {
                quote!(#root::agent_model::ItemKind::Lane(#root::agent_model::LaneKind::Value))
            }
            ItemSpec::Map(ItemKind::Store, _, _) => {
                quote!(#root::agent_model::ItemKind::Store(#root::agent_model::StoreKind::Map))
            }
            ItemSpec::Map(ItemKind::Lane, _, _) => {
                quote!(#root::agent_model::ItemKind::Lane(#root::agent_model::LaneKind::Map))
            }
            ItemSpec::JoinValue(_, _) => {
                quote!(#root::agent_model::ItemKind::Lane(#root::agent_model::LaneKind::JoinValue))
            }
        };

        let lane_name = model.literal();
        let flags = if model.is_stateful() {
            quote!(#root::agent_model::ItemFlags::empty())
        } else {
            quote!(#root::agent_model::ItemFlags::TRANSIENT)
        };

        quote!(::std::collections::HashMap::insert(&mut lanes, #lane_name, #root::agent_model::ItemSpec::new(#item_kind, #flags)))
    }
}
