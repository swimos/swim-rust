// Copyright 2015-2020 SWIM.AI inc.
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

use std::cell::RefCell;
use std::fmt::Display;

use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use syn;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::DeriveInput;

pub struct Parser<'a> {
    pub ident: syn::Ident,
    pub data: TypeContents<'a>,
    pub original: &'a DeriveInput,
}

pub struct Variant<'a> {
    pub ident: syn::Ident,
    pub style: CompoundType,
    pub fields: Vec<Field<'a>>,
    pub original: &'a syn::Variant,
}

pub struct Field<'a> {
    pub member: syn::Member,
    pub ty: &'a syn::Type,
    pub original: &'a syn::Field,
    pub name: String,
    pub ident: Ident,
}

impl<'p> ToTokens for Field<'p> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        self.name.to_tokens(tokens);
    }
}

pub enum TypeContents<'a> {
    Enum(Vec<Variant<'a>>),
    Struct(CompoundType, Vec<Field<'a>>),
}

#[derive(Clone, Copy)]
pub enum CompoundType {
    Struct,
    Tuple,
    NewType,
    Unit,
}

#[derive(Default)]
pub struct Context {
    errors: RefCell<Option<Vec<syn::Error>>>,
}

pub enum Fragment {
    Expr(TokenStream),
    Block(TokenStream),
}

pub struct TokenBlock(pub Fragment);

impl ToTokens for TokenBlock {
    fn to_tokens(&self, out: &mut TokenStream) {
        match &self.0 {
            Fragment::Expr(expr) => expr.to_tokens(out),
            Fragment::Block(block) => block.to_tokens(out),
        }
    }
}

impl Context {
    pub fn error_spanned_by<A: ToTokens, T: Display>(&self, obj: A, msg: T) {
        self.errors
            .borrow_mut()
            .as_mut()
            .unwrap()
            .push(syn::Error::new_spanned(obj.into_token_stream(), msg));
    }

    pub fn new() -> Context {
        Context {
            errors: RefCell::new(Some(Vec::new())),
        }
    }

    pub fn check(self) -> Result<(), Vec<syn::Error>> {
        let errors = self.errors.borrow_mut().take().unwrap();
        match errors.len() {
            0 => Ok(()),
            _ => Err(errors),
        }
    }
}

impl<'p> Parser<'p> {
    pub fn from_ast(context: &Context, input: &'p syn::DeriveInput) -> Option<Parser<'p>> {
        let data = match &input.data {
            syn::Data::Enum(data) => {
                context.error_spanned_by(input, "Enums not implemented yet");
                return None;
            }
            syn::Data::Struct(data) => {
                let (style, fields) = struct_from_ast(context, &data.fields);
                TypeContents::Struct(style, fields)
            }
            syn::Data::Union(_) => {
                context.error_spanned_by(input, "Unions are not supported");
                return None;
            }
        };

        let item = Parser {
            ident: input.ident.clone(),
            data,
            original: input,
        };

        Some(item)
    }

    pub fn receiver_assert_quote(&self) -> Vec<TokenStream> {
        match &self.data {
            TypeContents::Struct(CompoundType::Struct, fields) => fields
                .iter()
                .map(|field| {
                    let span = field.span();
                    let ty = field.ty;
                    let receiver_ident = Ident::new(
                        &format!("__Assert_{}_Receivers", field.ident.to_string()),
                        Span::call_site(),
                    );

                    quote_spanned! {span=>
                        struct #receiver_ident where #ty: Form;
                    }
                })
                .collect(),
            _ => unimplemented!("match_funcs"),
        }
    }
}

fn struct_from_ast<'a>(
    context: &Context,
    fields: &'a syn::Fields,
) -> (CompoundType, Vec<Field<'a>>) {
    match fields {
        syn::Fields::Named(fields) => (
            CompoundType::Struct,
            fields_from_ast(context, &fields.named),
        ),
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => (
            CompoundType::NewType,
            fields_from_ast(context, &fields.unnamed),
        ),
        syn::Fields::Unnamed(fields) => (
            CompoundType::Tuple,
            fields_from_ast(context, &fields.unnamed),
        ),
        syn::Fields::Unit => (CompoundType::Unit, Vec::new()),
    }
}

fn fields_from_ast<'a>(
    context: &Context,
    fields: &'a Punctuated<syn::Field, Token![,]>,
) -> Vec<Field<'a>> {
    fields
        .iter()
        .enumerate()
        .map(|(index, original_field)| Field {
            member: match &original_field.ident {
                Some(ident) => syn::Member::Named(ident.clone()),
                None => syn::Member::Unnamed(index.into()),
            },
            ty: &original_field.ty,
            original: original_field,
            name: match &original_field.ident {
                Some(ident) => ident.to_string().trim_start_matches("r#").to_owned(),
                None => index.to_string(),
            },
            ident: Ident::new(&format!("__self_0_{}", index), Span::call_site()),
        })
        .collect()
}
