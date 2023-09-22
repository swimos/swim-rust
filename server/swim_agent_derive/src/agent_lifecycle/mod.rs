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
use syn::{parse_quote, Path, Type};

use self::{
    model::{
        AgentLifecycleDescriptor, CommandLifecycleDescriptor, DemandLifecycleDescriptor,
        DemandMapLifecycleDescriptor, ItemLifecycle, JoinValueInit, MapLifecycleDescriptor,
        ValueLifecycleDescriptor,
    },
    tree::BinTree,
};

mod model;
mod tree;

pub use model::{strip_handler_attrs, validate_attr_args, validate_with_attrs};

/// Generates an additional impl block with a method to convert a type into an agent
/// lifecycle, using the event handler methods extracted from an existing impl block
/// for the type.
pub struct ImplAgentLifecycle<'a> {
    descriptor: AgentLifecycleDescriptor<'a>,
}

impl<'a> ImplAgentLifecycle<'a> {
    pub fn new(descriptor: AgentLifecycleDescriptor<'a>) -> Self {
        ImplAgentLifecycle { descriptor }
    }
}

#[derive(Clone, Copy)]
struct LifecycleTree<'a> {
    root: &'a Path,
    agent_type: &'a Path,
    lifecycle_type: &'a Type,
    tree: &'a BinTree<String, ItemLifecycle<'a>>,
}

struct LaneLifecycleBuilder<'a> {
    agent_type: &'a Path,
    lifecycle_type: &'a Type,
    lifecycle: &'a ItemLifecycle<'a>,
}

impl<'a> LaneLifecycleBuilder<'a> {
    fn new(
        agent_type: &'a Path,
        lifecycle_type: &'a Type,
        lifecycle: &'a ItemLifecycle<'a>,
    ) -> Self {
        LaneLifecycleBuilder {
            agent_type,
            lifecycle_type,
            lifecycle,
        }
    }

    fn into_builder_expr(self, root: &syn::Path) -> syn::Expr {
        let LaneLifecycleBuilder {
            agent_type,
            lifecycle_type,
            lifecycle,
        } = self;
        match lifecycle {
            ItemLifecycle::Value(ValueLifecycleDescriptor {
                on_event, on_set, ..
            }) => {
                let mut builder: syn::Expr = parse_quote! {
                    <#root::lanes::value::lifecycle::StatefulValueLaneLifecycle::<#agent_type, #lifecycle_type, _> as ::core::default::Default>::default()
                };
                if let Some(handler) = on_event {
                    builder = parse_quote! {
                        #root::lanes::value::lifecycle::StatefulValueLaneLifecycle::on_event(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_set {
                    builder = parse_quote! {
                        #root::lanes::value::lifecycle::StatefulValueLaneLifecycle::on_set(#builder, #lifecycle_type::#handler)
                    };
                }
                builder
            }
            ItemLifecycle::Command(CommandLifecycleDescriptor { on_command, .. }) => {
                parse_quote! {
                    #root::lanes::command::lifecycle::StatefulCommandLaneLifecycle::on_command(
                        <#root::lanes::command::lifecycle::StatefulCommandLaneLifecycle::<#agent_type, #lifecycle_type, _> as ::core::default::Default>::default(),
                        #lifecycle_type::#on_command
                    )
                }
            }
            ItemLifecycle::Demand(DemandLifecycleDescriptor { on_cue, .. }) => {
                parse_quote! {
                    #root::lanes::demand::lifecycle::StatefulDemandLaneLifecycle::on_cue(
                        <#root::lanes::demand::lifecycle::StatefulDemandLaneLifecycle::<#agent_type, #lifecycle_type, _> as ::core::default::Default>::default(),
                        #lifecycle_type::#on_cue
                    )
                }
            }
            ItemLifecycle::DemandMap(DemandMapLifecycleDescriptor {
                keys, on_cue_key, ..
            }) => {
                let mut builder: syn::Expr = parse_quote! {
                    <#root::lanes::demand_map::lifecycle::StatefulDemandMapLaneLifecycle::<#agent_type, #lifecycle_type, _, _> as ::core::default::Default>::default()
                };
                if let Some(handler) = keys {
                    builder = parse_quote! {
                        #root::lanes::demand_map::lifecycle::StatefulDemandMapLaneLifecycle::keys(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_cue_key {
                    builder = parse_quote! {
                        #root::lanes::demand_map::lifecycle::StatefulDemandMapLaneLifecycle::on_cue_key(#builder, #lifecycle_type::#handler)
                    };
                }
                builder
            }
            ItemLifecycle::Map(MapLifecycleDescriptor {
                on_update,
                on_remove,
                on_clear,
                ..
            }) => {
                let mut builder: syn::Expr = parse_quote! {
                    <#root::lanes::map::lifecycle::StatefulMapLaneLifecycle::<#agent_type, #lifecycle_type, _, _> as ::core::default::Default>::default()
                };
                if let Some(handler) = on_update {
                    builder = parse_quote! {
                        #root::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_update(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_remove {
                    builder = parse_quote! {
                        #root::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_remove(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_clear {
                    builder = parse_quote! {
                        #root::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_clear(#builder, #lifecycle_type::#handler)
                    };
                }
                builder
            }
        }
    }
}

impl<'a> LifecycleTree<'a> {
    fn new(
        root: &'a Path,
        agent_type: &'a Path,
        lifecycle_type: &'a Type,
        tree: &'a BinTree<String, ItemLifecycle<'a>>,
    ) -> Self {
        LifecycleTree {
            root,
            agent_type,
            lifecycle_type,
            tree,
        }
    }
}

impl<'a> ToTokens for LifecycleTree<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let LifecycleTree {
            root,
            agent_type,
            lifecycle_type,
            tree,
        } = *self;
        tokens.append_all(match tree {
            BinTree::Branch {
                key: name,
                data: lifecycle,
                left,
                right,
            } => {
                let field_ident = lifecycle.item_ident();
                let builder = LaneLifecycleBuilder::new(agent_type, lifecycle_type, lifecycle);
                let builder_expr = builder.into_builder_expr(root);
                let branch_type = lifecycle.branch_type(root);
                let left_tree = LifecycleTree::new(root, agent_type, lifecycle_type, left.as_ref());
                let right_tree = LifecycleTree::new(root, agent_type, lifecycle_type, right.as_ref());
                quote! {
                    #branch_type::new(#name, |agent: &#agent_type| &agent.#field_ident, #builder_expr, #left_tree, #right_tree)
                }
            }
            BinTree::Leaf => {
                quote!(#root::agent_lifecycle::item_event::HLeaf)
            }
        });
    }
}

impl<'a> ToTokens for ImplAgentLifecycle<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ImplAgentLifecycle {
            descriptor:
                AgentLifecycleDescriptor {
                    ref root,
                    ref agent_type,
                    no_clone,
                    lifecycle_type,
                    on_start,
                    on_stop,
                    ref lane_lifecycles,
                    ref init_blocks,
                },
        } = *self;

        let lane_lifecycle_tree =
            LifecycleTree::new(root, agent_type, lifecycle_type, lane_lifecycles);

        let mut lifecycle_builder: syn::Expr = parse_quote! {
            #root::agent_lifecycle::stateful::StatefulAgentLifecycle::<#agent_type, _>::new(self)
        };

        if !init_blocks.is_empty() {
            let init_handler = construct_join_init(init_blocks, root, agent_type, lifecycle_type);
            lifecycle_builder = parse_quote! {
                #root::agent_lifecycle::stateful::StatefulAgentLifecycle::on_init(#lifecycle_builder, #init_handler)
            };
        }

        if let Some(on_start) = on_start {
            lifecycle_builder = parse_quote! {
                #root::agent_lifecycle::stateful::StatefulAgentLifecycle::on_start(#lifecycle_builder, #lifecycle_type::#on_start)
            };
        }

        if let Some(on_stop) = on_stop {
            lifecycle_builder = parse_quote! {
                #root::agent_lifecycle::stateful::StatefulAgentLifecycle::on_stop(#lifecycle_builder, #lifecycle_type::#on_stop)
            };
        }

        if no_clone {
            tokens.append_all(quote! {

                impl #lifecycle_type {
                    pub fn into_lifecycle(self) -> impl #root::agent_lifecycle::AgentLifecycle<#agent_type> + ::core::marker::Send + 'static {
                        let lane_lifecycle = #lane_lifecycle_tree;
                        #root::agent_lifecycle::stateful::StatefulAgentLifecycle::on_lane_event(
                            #lifecycle_builder,
                            lane_lifecycle
                        )
                    }
                }
            });
        } else {
            tokens.append_all(quote! {

                impl #lifecycle_type {
                    pub fn into_lifecycle(self) -> impl #root::agent_lifecycle::AgentLifecycle<#agent_type> + ::core::clone::Clone + ::core::marker::Send + 'static {
                        let lane_lifecycle = #lane_lifecycle_tree;
                        #root::agent_lifecycle::stateful::StatefulAgentLifecycle::on_lane_event(
                            #lifecycle_builder,
                            lane_lifecycle
                        )
                    }
                }
            });
        }
    }
}

fn construct_join_init(
    join_inits: &[JoinValueInit<'_>],
    root: &Path,
    agent_type: &Path,
    lifecycle_type: &Type,
) -> impl ToTokens {
    let base =
        quote!(<#root::agent_lifecycle::on_init::InitNil as ::core::default::Default>::default());
    join_inits.iter().rev().fold(base, |acc, init| {
        let item_name = init.item_ident();
        let lifecycle = init.lifecycle;
        let constructor = quote! {
            #root::agent_lifecycle::on_init::RegisterJoinValue::new(|agent: &#agent_type| &agent.#item_name, #lifecycle_type::#lifecycle)
        };
        quote!(#root::agent_lifecycle::on_init::InitCons::cons(#constructor, #acc))
    })
}
