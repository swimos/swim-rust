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

use frunk::hlist;
use macro_utilities::{
    attributes::NestedMetaConsumer, CaseConvention, NameTransform, NameTransformConsumer,
    Transformation, TypeLevelNameTransformConsumer,
};
use swimos_utilities::errors::{Errors, Validation, ValidationItExt};
use syn::{parse_quote, Field, NestedMeta};

use super::model::ItemFlags;

const RENAME_TAG: &str = "name";
const CONV_TAG: &str = "convention";
const TRANSIENT_ATTR_NAME: &str = "transient";
const ROOT_ATTR_NAME: &str = "root";
const INVALID_FIELD_ATTR: &str = "Invalid field attribute.";
const INVALID_AGENT_ROOT: &str = "Invalid agent root specifier.";

struct TransientFlag;

/// Attribute consumer to recognize the transient flag for agent items.
struct TransientFlagConsumer;

/// Types of modification that can be applied to an item using attributes.
pub enum ItemAttr {
    /// The should be transient.
    Transient,
    /// The name of the item should be transformed.
    Transform(Transformation),
}

impl NestedMetaConsumer<TransientFlag> for TransientFlagConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<TransientFlag>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) => match path.segments.first() {
                Some(seg) => {
                    if seg.ident == TRANSIENT_ATTR_NAME {
                        if path.segments.len() == 1 && seg.arguments.is_empty() {
                            Ok(Some(TransientFlag))
                        } else {
                            Err(syn::Error::new_spanned(meta, INVALID_FIELD_ATTR))
                        }
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

pub fn make_item_attr_consumer() -> impl NestedMetaConsumer<ItemAttr> {
    let trans_consumer = NameTransformConsumer::new(RENAME_TAG, CONV_TAG);
    hlist![
        TransientFlagConsumer.map(|_| ItemAttr::Transient),
        trans_consumer.map(ItemAttr::Transform)
    ]
}

/// Modifications to an agent item, defined by attributes attached to it.
#[derive(Debug, Default)]
pub struct ItemModifiers {
    /// Transformation to apply to the name of the item.
    pub transform: NameTransform,
    /// Flags that are set for the item.
    pub flags: ItemFlags,
}

/// Attempt to create an [`ItemModifiers`] from the [`ItemAttr`] records extracted from the attributes
/// attached to the associated item.
pub fn combine_item_attrs(
    field: &Field,
    attrs: Vec<ItemAttr>,
) -> Validation<ItemModifiers, Errors<syn::Error>> {
    attrs.into_iter().validate_fold(
        Validation::valid(ItemModifiers::default()),
        false,
        |mut modifiers, attr| {
            let mut errors = Errors::empty();
            match attr {
                ItemAttr::Transient => modifiers.flags.insert(ItemFlags::TRANSIENT),
                ItemAttr::Transform(t) => {
                    if let Err(e) = modifiers.transform.try_add(field, t) {
                        errors.push(e);
                    }
                }
            }
            Validation::valid(modifiers)
        },
    )
}

/// Types of modification that can be applied to an agent using attributes.
pub enum AgentAttr {
    /// Mark all items of the agent as transient.
    Transient,
    /// Module path where the types from the `swimos_agent` create can be found.
    Root(syn::Path),
    /// Renaming convention to apply to all items of the agent.
    RenameConvention(CaseConvention),
}

struct RootConsumer;

impl NestedMetaConsumer<AgentAttr> for RootConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<AgentAttr>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(syn::Meta::List(meta_list)) => {
                if meta_list.path.is_ident(ROOT_ATTR_NAME) {
                    match meta_list.nested.first() {
                        Some(NestedMeta::Meta(syn::Meta::Path(path)))
                            if meta_list.nested.len() == 1 =>
                        {
                            Ok(Some(AgentAttr::Root(path.clone())))
                        }
                        _ => Err(syn::Error::new_spanned(meta_list, INVALID_AGENT_ROOT)),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

pub fn make_agent_attr_consumer() -> impl NestedMetaConsumer<AgentAttr> {
    let trans_consumer = TypeLevelNameTransformConsumer::new(CONV_TAG);
    hlist![
        TransientFlagConsumer.map(|_| AgentAttr::Transient),
        RootConsumer,
        trans_consumer.map(AgentAttr::RenameConvention)
    ]
}

/// Modifications to an agent, defined by attributes attached to it.
#[derive(Debug)]
pub struct AgentModifiers {
    /// Mark all items of the agent as transient.
    pub transient: bool,
    /// Renaming strategy for all items of the agent.
    pub transform: Option<CaseConvention>,
    /// Module path where the types from the `swimos_agent` create can be found.
    pub root: syn::Path,
}

fn default_root() -> syn::Path {
    parse_quote!(::swimos::agent)
}

impl Default for AgentModifiers {
    fn default() -> Self {
        Self {
            transient: false,
            transform: None,
            root: default_root(),
        }
    }
}

/// Attempt to create an [`AgentModifiers`] from the [`AgentAttr`] records extracted from the attributes
/// attached to the associated definition.
pub fn combine_agent_attrs(
    item: &syn::DeriveInput,
    attrs: Vec<AgentAttr>,
) -> Validation<AgentModifiers, Errors<syn::Error>> {
    attrs.into_iter().validate_fold(
        Validation::valid(AgentModifiers::default()),
        false,
        |mut modifiers, attr| {
            let mut errors = Errors::empty();
            match attr {
                AgentAttr::Transient => modifiers.transient = true,
                AgentAttr::Root(path) => modifiers.root = path,
                AgentAttr::RenameConvention(conv) => {
                    if modifiers.transform.is_some() {
                        errors.push(syn::Error::new_spanned(
                            item,
                            "Agent has multiple naming conventions.",
                        ));
                    } else {
                        modifiers.transform = Some(conv);
                    }
                }
            }
            Validation::valid(modifiers)
        },
    )
}
