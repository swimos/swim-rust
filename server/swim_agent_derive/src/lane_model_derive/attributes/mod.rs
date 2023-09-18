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
use swim_utilities::errors::{
    validation::{Validation, ValidationItExt},
    Errors,
};
use syn::{parse_quote, Field, NestedMeta};

use super::model::ItemFlags;

const RENAME_TAG: &str = "name";
const CONV_TAG: &str = "convention";
const TRANSIENT_ATTR_NAME: &str = "transient";
const ROOT_ATTR_NAME: &str = "root";
const INVALID_FIELD_ATTR: &str = "Invalid field attribute.";
const INVALID_AGENT_ROOT: &str = "Invalid agent root specifiier.";

struct TransientFlagConsumer;

pub enum ItemAttr {
    Transient,
    Transform(Transformation),
}

impl NestedMetaConsumer<ItemAttr> for TransientFlagConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<ItemAttr>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) => match path.segments.first() {
                Some(seg) => {
                    if seg.ident == TRANSIENT_ATTR_NAME {
                        if path.segments.len() == 1 && seg.arguments.is_empty() {
                            Ok(Some(ItemAttr::Transient))
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
        TransientFlagConsumer,
        trans_consumer.map(ItemAttr::Transform)
    ]
}

#[derive(Debug, Default)]
pub struct ItemModifiers {
    pub transform: NameTransform,
    pub flags: ItemFlags,
}

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

pub enum AgentAttr {
    Root(syn::Path),
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
        RootConsumer,
        trans_consumer.map(AgentAttr::RenameConvention)
    ]
}

#[derive(Debug)]
pub struct AgentModifiers {
    pub transform: Option<CaseConvention>,
    pub root: syn::Path,
}

fn default_root() -> syn::Path {
    parse_quote!(::swim::agent)
}

impl Default for AgentModifiers {
    fn default() -> Self {
        Self {
            transform: None,
            root: default_root(),
        }
    }
}

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
