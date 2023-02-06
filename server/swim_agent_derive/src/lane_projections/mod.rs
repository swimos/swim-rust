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

mod model;

pub use model::{validate_input, AgentField, AgentFields};
use proc_macro2::TokenStream;
use quote::{quote, ToTokens, TokenStreamExt};

/// Generates an impl block with constants for projection functions to each field
/// of the struct. The name of the projection will be the name of the field made upper
/// case.
pub struct ProjectionsImpl<'a>(AgentFields<'a>);

impl<'a> ProjectionsImpl<'a> {
    pub fn new(model: AgentFields<'a>) -> Self {
        ProjectionsImpl(model)
    }
}

impl<'a> ToTokens for ProjectionsImpl<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ProjectionsImpl(AgentFields {
            agent_name,
            generics,
            ref fields,
        }) = *self;

        let defs = fields
            .iter()
            .copied()
            .map(Projection::new)
            .map(Projection::into_tokens);

        let (impl_gen, type_gen, where_clause) = generics.split_for_impl();

        tokens.append_all(quote! {

            #[automatically_derived]
            impl #impl_gen #agent_name #type_gen #where_clause {

                #(pub const #defs;)*

            }
        });
    }
}

struct Projection<'a> {
    field: AgentField<'a>,
}

impl<'a> Projection<'a> {
    fn new(field: AgentField<'a>) -> Self {
        Projection { field }
    }

    fn into_tokens(self) -> TokenStream {
        let Projection { field } = self;
        let proj_name = field.projection_name();

        let AgentField {
            field_name,
            field_type,
        } = field;

        quote!(#proj_name: for<'a> fn(&'a Self) -> &'a #field_type = |agent| &agent.#field_name)
    }
}
