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

use proc_macro2::{Span, TokenStream};
use swim_utilities::errors::{
    validation::{Validation, ValidationItExt},
    Errors,
};
use syn::{Generics, Ident, Item, ItemStruct, Type};

/// Model of a the components of a struct type required to generate projection functions
/// for each field.
#[derive(Debug, Clone)]
pub struct AgentFields<'a> {
    pub agent_name: &'a Ident,
    pub generics: &'a Generics,
    pub fields: Vec<AgentField<'a>>,
}

impl<'a> AgentFields<'a> {
    /// #Arguments
    /// * `agent_name` - The name of the struct type.
    /// * `generics` - The generic parameters of the struct (for application to the new impl block).
    /// * `fields` - Required information about each field (name and type).
    pub fn new(agent_name: &'a Ident, generics: &'a Generics, fields: Vec<AgentField<'a>>) -> Self {
        AgentFields {
            agent_name,
            generics,
            fields,
        }
    }
}

/// Name and type of each field from a struct.
#[derive(Debug, Clone, Copy)]
pub struct AgentField<'a> {
    pub field_name: &'a Ident,
    pub field_type: &'a Type,
}

impl<'a> AgentField<'a> {
    pub fn new(field_name: &'a Ident, field_type: &'a Type) -> Self {
        AgentField {
            field_name,
            field_type,
        }
    }

    /// Transform the name of the field to upper case to get the name of the projection function
    /// constant.
    pub fn projection_name(&self) -> syn::Ident {
        let AgentField { field_name, .. } = *self;

        let name_str = field_name.to_string();
        let transformed = name_str.to_uppercase();
        Ident::new(transformed.as_str(), Span::call_site())
    }
}

/// Validate the input to the projections macro.
///
/// - No paramters are expected.
/// - The input should be a struct type with named fields.
pub fn validate_input<'a>(
    attr_body: Option<&'a TokenStream>,
    item: &'a Item,
) -> Validation<AgentFields<'a>, Errors<syn::Error>> {
    let name = validate_attr_body(attr_body);
    let fields = validate_item(item);
    name.join(fields).map(|(_, fields)| fields)
}

const NO_PARAMS: &str = "The projections macro does not take any arguments.";
const ONLY_STRUCTS: &str = "The projections macro can only be applied to struct definitions.";
const NO_TUPLES: &str = "Projections cannot be generated for tuple structs.";

fn validate_attr_body(attr_body: Option<&TokenStream>) -> Validation<(), Errors<syn::Error>> {
    if let Some(meta) = attr_body {
        Validation::fail(syn::Error::new_spanned(meta, NO_PARAMS))
    } else {
        Validation::valid(())
    }
}

fn validate_item(item: &Item) -> Validation<AgentFields<'_>, Errors<syn::Error>> {
    if let Item::Struct(struct_item) = item {
        validate_from_struct(struct_item)
    } else {
        Validation::fail(syn::Error::new_spanned(item, ONLY_STRUCTS))
    }
}

fn validate_from_struct(
    struct_item: &ItemStruct,
) -> Validation<AgentFields<'_>, Errors<syn::Error>> {
    let fields =
        struct_item
            .fields
            .iter()
            .append_fold(Validation::valid(vec![]), true, |mut acc, field| {
                if let Some(name) = &field.ident {
                    acc.push(AgentField::new(name, &field.ty));
                    Validation::valid(acc)
                } else {
                    Validation::Validated(acc, Some(syn::Error::new_spanned(field, NO_TUPLES)))
                }
            });
    fields.map(|fields| AgentFields::new(&struct_item.ident, &struct_item.generics, fields))
}
