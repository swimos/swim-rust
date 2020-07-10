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
use syn::punctuated::Punctuated;
use syn::{DeriveInput, Meta, NestedMeta};

const FORM_PATH: &str = "form";
const SER_NAME: &str = "ser_name";
const DE_NAME: &str = "de_name";
const PULL_UP_NAME: &str = "pull_up";
const IGNORE_NAME: &str = "ignore";

fn get_form_attributes(ctx: &Context, attr: &syn::Attribute) -> Result<Vec<syn::NestedMeta>, ()> {
    if !attr.path.is_ident(FORM_PATH) {
        Ok(Vec::new())
    } else {
        match attr.parse_meta() {
            Ok(Meta::List(meta)) => Ok(meta.nested.into_iter().collect()),
            Ok(other) => {
                ctx.error_spanned_by(other, "Invalid attribute. Expected #[form(...)]");
                Err(())
            }
            Err(err) => {
                // ctx.syn_error(err);
                Err(())
            }
        }
    }
}

pub struct Parser<'a> {
    pub ident: syn::Ident,
    pub data: TypeContents<'a>,
    pub original: &'a DeriveInput,
    pub generics: &'a syn::Generics,
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
    pub attributes: Attributes,
    pub ident: Ident,
}

pub struct Attributes {
    name: FieldName,
}

impl Attributes {
    fn from(ctx: &Context, idx: usize, field: &syn::Field) -> Attributes {
        field
            .attrs
            .iter()
            .flat_map(|a| get_form_attributes(ctx, a))
            .flatten()
            .for_each(|meta: NestedMeta| match &meta {
                NestedMeta::Meta(Meta::NameValue(name)) => {}
                NestedMeta::Meta(Meta::Path(_)) => {}
                NestedMeta::Meta(Meta::List(_)) => {}
                NestedMeta::Lit(_) => {}
            });

        let field_name = match &field.ident {
            Some(ident) => ident.to_string().trim_start_matches("r#").to_owned(),
            None => idx.to_string(),
        };

        Attributes {
            name: FieldName {
                serialize_as: field_name.clone(),
                deserialize_as: field_name.clone(),
                original: field_name,
            },
        }
    }
}

pub struct FieldName {
    serialize_as: String,
    deserialize_as: String,
    original: String,
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

fn serialize_struct<'a>(fields: &[Field], parser: &Parser<'a>) -> TokenStream {
    let struct_name = parser.ident.to_string();

    let fields: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let name = &f.attributes.name.original;
            let ident = Ident::new(&name, Span::call_site());

            quote!(serializer.serialize_field(Some(#name), &self.#ident, None);)
        })
        .collect();

    let no_fields = fields.len();

    quote! {
        let mut serializer = swim_form::ValueSerializer::default();

        serializer.serialize_struct(#struct_name, #no_fields);
        #(#fields)*

        serializer.exit_nested();
        serializer.output()
    }
}

fn serialize_newtype_struct<'a>(field: &Field, parser: &Parser<'a>) -> TokenStream {
    let struct_name = parser.ident.to_string();

    quote! {
        let mut serializer = swim_form::ValueSerializer::default();

        serializer.serialize_struct(#struct_name, 1);
        serializer.serialize_field(None, &self.0, None);

        serializer.exit_nested();
        serializer.output()
    }
}

impl<'p> Parser<'p> {
    pub fn serialize_fields(&self) -> TokenStream {
        match &self.data {
            TypeContents::Struct(CompoundType::Struct, fields) => serialize_struct(fields, self),
            TypeContents::Struct(CompoundType::NewType, fields) => {
                serialize_newtype_struct(&fields[0], self)
            }
            TypeContents::Struct(CompoundType::Tuple, fields) => unimplemented!(),
            TypeContents::Struct(CompoundType::Unit, fields) => unimplemented!(),
            TypeContents::Enum(variants) => unimplemented!(),
        }
    }

    pub fn from_ast(context: &Context, input: &'p syn::DeriveInput) -> Option<Parser<'p>> {
        let data = match &input.data {
            syn::Data::Enum(data) => {
                let variants = parse_enum(context, &data.variants);
                TypeContents::Enum(variants)
            }
            syn::Data::Struct(data) => {
                let (style, fields) = parse_struct(context, &data.fields);
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
            generics: &input.generics,
        };

        Some(item)
    }
}

fn parse_enum<'a>(
    cx: &Context,
    variants: &'a Punctuated<syn::Variant, syn::Token![,]>,
) -> Vec<Variant<'a>> {
    variants
        .iter()
        .map(|variant| {
            let (style, fields) = parse_struct(cx, &variant.fields);
            Variant {
                ident: variant.ident.clone(),
                style,
                fields,
                original: variant,
            }
        })
        .collect()
}

fn parse_struct<'a>(context: &Context, fields: &'a syn::Fields) -> (CompoundType, Vec<Field<'a>>) {
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
    fields: &'a Punctuated<syn::Field, syn::Token![,]>,
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
            attributes: Attributes::from(context, index, original_field),
            ident: Ident::new(&format!("__self_0_{}", index), Span::call_site()),
        })
        .collect()
}
