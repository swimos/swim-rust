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
use syn::{DeriveInput, Index, Meta, NestedMeta};

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
    crate_binding: &'a NestedMeta,
}

pub struct EnumVariant<'a> {
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
                write_as: field_name.clone(),
                read_as: field_name.clone(),
                original: field_name,
            },
        }
    }
}

pub struct FieldName {
    write_as: String,
    read_as: String,
    original: String,
}

pub enum TypeContents<'a> {
    Enum(Vec<EnumVariant<'a>>),
    Struct(CompoundType, Vec<Field<'a>>),
}

#[derive(Clone, Copy)]
pub enum CompoundType {
    Struct,
    Tuple,
    NewType,
    Unit,
}

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

fn transmute_struct<'a>(fields: &[Field], parser: &Parser<'a>) -> TokenStream {
    let struct_name = parser.ident.to_string();
    let crate_binding = &parser.crate_binding;

    let fields: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let ty = &f.ident.to_string();
            let name = &f.attributes.name.write_as;
            let ident = Ident::new(&name, Span::call_site());

            quote!(#crate_binding::Item::of((#name, self.#ident.transmute_to_value(Some(#ty)))))
        })
        .collect();

    let no_fields = fields.len();

    quote! {
        let items = vec![#(#fields),*];
        #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#struct_name)], items)
    }
}

fn transmute_newtype_struct<'a>(field: &Field, parser: &Parser<'a>) -> TokenStream {
    let struct_name = parser.ident.to_string();
    let ty = &field.ident.to_string();
    let crate_binding = &parser.crate_binding;

    quote! {
        #crate_binding::Value::Record(
            vec![#crate_binding::Attr::of(#struct_name)],
            vec![#crate_binding::Item::ValueItem(self.0.transmute_to_value(None))]
        )
    }
}

fn transmute_unit_struct(parser: &Parser) -> TokenStream {
    let struct_name = parser.ident.to_string();
    let crate_binding = &parser.crate_binding;

    quote! {
        #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#struct_name)], Vec::new())
    }
}

fn transmute_tuple_struct<'a>(fields: &[Field], parser: &Parser<'a>) -> TokenStream {
    let struct_name = parser.ident.to_string();
    let crate_binding = &parser.crate_binding;

    let fields: Vec<TokenStream> = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let index = Index {
                index: idx as u32,
                span: Span::call_site(),
            };

            quote!(#crate_binding::Item::of(self.#index.transmute_to_value(None)))
        })
        .collect();

    let no_fields = fields.len();

    quote! {
        let items = vec![#(#fields),*];
        #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#struct_name)], items)
    }
}

fn transmute_enum<'a>(variants: &[EnumVariant], parser: &Parser<'a>) -> TokenStream {
    let ident = &parser.ident;
    let enum_name = ident.to_string();
    let crate_binding = &parser.crate_binding;

    let arms: Vec<_> = variants
        .iter()
        .enumerate()
        .map(|(idx, variant)| {
            let variant_ident = &variant.ident;
            let variant_name = variant_ident.to_string();

            match variant.style {
                CompoundType::Tuple => {
                    let field_names = (0..variant.fields.len())
                        .map(|i| Ident::new(&format!("__field{}", i), Span::call_site()));
                    let fields = (0..variant.fields.len()).map(|i| {
                        let index = Ident::new(&format!("__field{}", i), Span::call_site());

                        quote!(#crate_binding::Item::of(#index.transmute_to_value(None)))
                    });
                    let len = fields.len();

                    quote! {
                        #ident::#variant_ident(#(ref #field_names),*) => {
                            let items = vec![#(#fields),*];
                            #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#variant_name)], items)
                        }
                    }
                }
                CompoundType::Unit => {
                    let body = quote! {
                        #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#variant_name)], Vec::new())
                    };
                    quote!(#ident::#variant_ident => { #body })
                }
                CompoundType::NewType => {
                    let body = quote! {
                        #crate_binding::Value::Record(
                            vec![#crate_binding::Attr::of(#variant_name)],
                            vec![#crate_binding::Item::ValueItem(__field0.transmute_to_value(None))]
                        )
                    };

                    quote!(#ident::#variant_ident(ref __field0) => { #body })
                }
                CompoundType::Struct => {
                    let fields: Vec<TokenStream> = variant
                        .fields
                        .iter()
                        .map(|f| {
                            let name = &f.attributes.name.write_as;
                            let field_ident = Ident::new(&name, Span::call_site());

                            quote!(#crate_binding::Item::of((#name, #field_ident.transmute_to_value(None))))
                        })
                        .collect();

                    let no_fields = fields.len();

                    let body = quote! {
                        let items = vec![#(#fields),*];
                        #crate_binding::Value::Record(vec![#crate_binding::Attr::of(#variant_name)], items)
                    };

                    let vars = variant.fields.iter().map(|f| &f.member);
                    quote!(#ident::#variant_ident { #(ref #vars),* } => { #body })
                }
            }
        })
        .collect();

    quote! {
        match *self {
            #(#arms)*
        }
    }
}

impl<'p> Parser<'p> {
    pub fn transmute_fields(&self) -> TokenStream {
        match &self.data {
            TypeContents::Struct(CompoundType::Struct, fields) => transmute_struct(fields, self),
            TypeContents::Struct(CompoundType::NewType, fields) => {
                transmute_newtype_struct(&fields[0], self)
            }
            TypeContents::Struct(CompoundType::Tuple, fields) => {
                transmute_tuple_struct(fields, self)
            }
            TypeContents::Struct(CompoundType::Unit, _fields) => transmute_unit_struct(self),
            TypeContents::Enum(variants) => transmute_enum(variants, self),
        }
    }

    pub fn from_ast(
        context: &Context,
        input: &'p syn::DeriveInput,
        crate_binding: &'p NestedMeta,
    ) -> Option<Parser<'p>> {
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
            crate_binding,
        };

        Some(item)
    }
}

fn parse_enum<'a>(
    cx: &Context,
    variants: &'a Punctuated<syn::Variant, syn::Token![,]>,
) -> Vec<EnumVariant<'a>> {
    variants
        .iter()
        .map(|variant| {
            let (style, fields) = parse_struct(cx, &variant.fields);
            EnumVariant {
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
