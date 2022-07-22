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
use syn::{parse_quote, Path, Type};

use self::{
    model::{
        AgentLifecycleDescriptor, CommandLifecycleDescriptor, LaneLifecycle,
        MapLifecycleDescriptor, ValueLifecycleDescriptor,
    },
    tree::BinTree,
};

mod model;
mod tree;

pub use model::{strip_handler_attrs, validate_attr_args, validate_with_attrs};

pub struct ImplAgentLifecycle<'a> {
    descriptor: AgentLifecycleDescriptor<'a>,
}

impl<'a> ImplAgentLifecycle<'a> {
    pub fn new(descriptor: AgentLifecycleDescriptor<'a>) -> Self {
        ImplAgentLifecycle { descriptor }
    }
}

#[derive(Clone, Copy)]
pub struct LifecycleTree<'a> {
    agent_type: &'a Path,
    lifecycle_type: &'a Type,
    tree: &'a BinTree<String, LaneLifecycle<'a>>,
}

struct LaneLifecycleBuilder<'a> {
    agent_type: &'a Path,
    lifecycle_type: &'a Type,
    lifecycle: &'a LaneLifecycle<'a>,
}

impl<'a> LaneLifecycleBuilder<'a> {
    fn new(
        agent_type: &'a Path,
        lifecycle_type: &'a Type,
        lifecycle: &'a LaneLifecycle<'a>,
    ) -> Self {
        LaneLifecycleBuilder {
            agent_type,
            lifecycle_type,
            lifecycle,
        }
    }

    fn into_builder_expr(self) -> syn::Expr {
        let LaneLifecycleBuilder {
            agent_type,
            lifecycle_type,
            lifecycle,
        } = self;
        match lifecycle {
            LaneLifecycle::Value(ValueLifecycleDescriptor {
                on_event, on_set, ..
            }) => {
                let mut builder: syn::Expr = parse_quote! {
                    <::swim_agent::lanes::value::lifecycle::StatefulValueLaneLifecycle::<#agent_type, #lifecycle_type, _> as ::core::default::Default>::default()
                };
                if let Some(handler) = on_event {
                    builder = parse_quote! {
                        ::swim_agent::lanes::value::lifecycle::StatefulValueLaneLifecycle::on_event(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_set {
                    builder = parse_quote! {
                        ::swim_agent::lanes::value::lifecycle::StatefulValueLaneLifecycle::on_set(#builder, #lifecycle_type::#handler)
                    };
                }
                builder
            }
            LaneLifecycle::Command(CommandLifecycleDescriptor { on_command, .. }) => {
                parse_quote! {
                    ::swim_agent::lanes::command::lifecycle::StatefulCommandLaneLifecycle::on_command(
                        <::swim_agent::lanes::command::lifecycle::StatefulCommandLaneLifecycle::<#agent_type, #lifecycle_type, _> as ::core::default::Default>::default(),
                        #lifecycle_type::#on_command
                    )
                }
            }
            LaneLifecycle::Map(MapLifecycleDescriptor {
                on_update,
                on_remove,
                on_clear,
                ..
            }) => {
                let mut builder: syn::Expr = parse_quote! {
                    <::swim_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle::<#agent_type, #lifecycle_type, _, _> as ::core::default::Default>::default()
                };
                if let Some(handler) = on_update {
                    builder = parse_quote! {
                        ::swim_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_update(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_remove {
                    builder = parse_quote! {
                        ::swim_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_remove(#builder, #lifecycle_type::#handler)
                    };
                }
                if let Some(handler) = on_clear {
                    builder = parse_quote! {
                        ::swim_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle::on_clear(#builder, #lifecycle_type::#handler)
                    };
                }
                builder
            }
        }
    }
}

impl<'a> LifecycleTree<'a> {
    fn new(
        agent_type: &'a Path,
        lifecycle_type: &'a Type,
        tree: &'a BinTree<String, LaneLifecycle<'a>>,
    ) -> Self {
        LifecycleTree {
            agent_type,
            lifecycle_type,
            tree,
        }
    }
}

impl<'a> ToTokens for LifecycleTree<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let LifecycleTree {
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
                let field_ident = lifecycle.lane_ident();
                let builder = LaneLifecycleBuilder::new(agent_type, lifecycle_type, lifecycle);
                let builder_expr = builder.into_builder_expr();
                let branch_type = lifecycle.branch_type();
                let left_tree = LifecycleTree::new(agent_type, lifecycle_type, left.as_ref());
                let right_tree = LifecycleTree::new(agent_type, lifecycle_type, right.as_ref());
                quote! {
                    #branch_type::new(#name, |agent: &#agent_type| &agent.#field_ident, #builder_expr, #left_tree, #right_tree)
                }
            }
            BinTree::Leaf => {
                quote!(::swim_agent::lifecycle::lane_event::HLeaf)
            }
        });
    }
}

impl<'a> ToTokens for ImplAgentLifecycle<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ImplAgentLifecycle {
            descriptor:
                AgentLifecycleDescriptor {
                    ref agent_type,
                    lifecycle_type,
                    on_start,
                    on_stop,
                    ref lane_lifecycles,
                },
        } = *self;

        let lane_lifecycles = LifecycleTree::new(agent_type, lifecycle_type, lane_lifecycles);

        let mut lifecycle_builder: syn::Expr = parse_quote! {
            ::swim_agent::lifecycle::stateful::StatefulAgentLifecycle::<#agent_type, _>::new(self)
        };

        if let Some(on_start) = on_start {
            lifecycle_builder = parse_quote! {
                ::swim_agent::lifecycle::stateful::StatefulAgentLifecycle::on_start(#lifecycle_builder, #lifecycle_type::#on_start)
            };
        }

        if let Some(on_stop) = on_stop {
            lifecycle_builder = parse_quote! {
                ::swim_agent::lifecycle::stateful::StatefulAgentLifecycle::on_stop(#lifecycle_builder, #lifecycle_type::#on_stop)
            };
        }

        tokens.append_all(quote! {

            impl #lifecycle_type {
                pub fn into_lifecycle(self) -> impl ::swim_agent::lifecycle::AgentLifecycle<#agent_type> + ::core::clone::Clone + ::core::marker::Send + 'static {
                    let lane_lifecycle = #lane_lifecycles;
                    ::swim_agent::lifecycle::stateful::StatefulAgentLifecycle::on_lane_event(
                        #lifecycle_builder,
                        lane_lifecycle
                    )
                }
            }

        });
    }
}
