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

use std::collections::HashMap;

use either::Either;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::{Generics, TypeGenerics};

use macro_utilities::{CompoundTypeKind, FieldKind};

use crate::quote::TokenStreamExt;
use crate::structural::model::enumeration::SegregatedEnumModel;
use crate::structural::model::field::{BodyFields, FieldModel, HeaderFields, SegregatedFields};
use crate::structural::model::record::SegregatedStructModel;

use super::model::record::StructModel;

/// Implements the StructuralReadable trait for either of [`SegregatedStructModel`] or
/// [`SegregatedEnumModel`].
pub struct DeriveStructuralReadable<'a, S>(pub S, pub &'a Generics);

impl<'a> ToTokens for DeriveStructuralReadable<'a, SegregatedStructModel<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model, generics) = self;
        let root = model.inner.root;
        let mut new_generics = (*generics).clone();
        super::add_bounds(
            generics,
            &mut new_generics,
            parse_quote!(#root::structural::read::recognizer::RecognizerReadable),
        );

        let (impl_gen, type_gen, where_clause) = new_generics.split_for_impl();
        let name = model.inner.name;
        if model.inner.fields_model.type_kind == CompoundTypeKind::Unit {
            let lit_name = model.inner.resolve_name();

            tokens.append_all(quote! {
                #[automatically_derived]
                #[allow(non_snake_case)]
                impl #root::structural::read::recognizer::RecognizerReadable for #name {
                    type Rec = #root::structural::read::recognizer::UnitStructRecognizer<#name>;
                    type AttrRec = #root::structural::read::recognizer::SimpleAttrBody<
                        #root::structural::read::recognizer::UnitStructRecognizer<#name>
                    >;
                    type BodyRec = Self::Rec;

                    #[inline]
                    fn make_recognizer() -> Self::Rec {
                        #root::structural::read::recognizer::UnitStructRecognizer::new(
                            #lit_name,
                            || #name
                        )
                    }

                    #[inline]
                    fn make_attr_recognizer() -> Self::AttrRec {
                        #root::structural::read::recognizer::SimpleAttrBody::new(
                            <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                        )
                    }

                    #[inline]
                    fn make_body_recognizer() -> Self::BodyRec {
                        <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                    }

                }
            })
        } else {
            let target = parse_quote!(#name #type_gen);
            let builder_type = RecognizerState::new(model, &target);

            let select_feed = SelectFeedFn::new(model);
            let constructor = parse_quote!(#name);
            let on_done = OnDoneFn::new(model, constructor);
            let select_index = match &model.fields.body {
                BodyFields::StdBody(body_fields)
                    if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled =>
                {
                    SelectIndexFnLabelled::new(model, body_fields).into_token_stream()
                }
                _ => SelectIndexFnOrdinal::new(model).into_token_stream(),
            };

            let read_impl = StructReadableImpl::new(model, &type_gen);

            let on_reset = ResetFn::new(root, model.fields.num_field_blocks());

            let select_feed_name = select_feed_name();
            let on_done_name = on_done_name();
            let on_reset_name = on_reset_name();
            let builder_name = builder_ident();

            let header_recog_block = if model.fields.header.header_fields.is_empty() {
                None
            } else {
                let HeaderFields {
                    tag_body,
                    header_fields,
                    ..
                } = &model.fields.header;
                Some(HeaderRecognizerFns::new(
                    root,
                    &target,
                    *tag_body,
                    header_fields.as_slice(),
                    &new_generics,
                ))
            };

            tokens.append_all(quote! {
                const _: () = {
                    type #builder_name #type_gen = #builder_type;
                    #select_index

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: #root::structural::read::event::ReadEvent<'_>)
                        -> ::core::option::Option<::core::result::Result<(), #root::structural::read::error::ReadError>>
                    #where_clause
                    {
                        #select_feed
                    }

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    fn #on_done_name #impl_gen(state: &mut #builder_name #type_gen) -> ::core::result::Result<#target, #root::structural::read::error::ReadError>
                    #where_clause
                    {
                        #on_done
                    }

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    fn #on_reset_name #impl_gen(state: &mut #builder_name #type_gen)
                    #where_clause
                    {
                         #on_reset
                    }

                    #header_recog_block

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    impl #impl_gen #root::structural::read::recognizer::RecognizerReadable for #name #type_gen #where_clause {
                        #read_impl
                    }

                };
            })
        }
    }
}

fn suffix_ident(stem: &str, suffix: usize) -> syn::Ident {
    format_ident!("{}_{}", stem, suffix)
}

fn builder_ident() -> syn::Ident {
    syn::Ident::new(BUILDER_NAME, Span::call_site())
}

fn header_builder_ident() -> syn::Ident {
    syn::Ident::new(HEADER_BUILDER_NAME, Span::call_site())
}

fn suffixed_builder_ident(suffix: usize) -> syn::Ident {
    format_ident!("{}{}", BUILDER_NAME, suffix)
}

fn suffixed_header_builder_ident(suffix: usize) -> syn::Ident {
    format_ident!("{}{}", HEADER_BUILDER_NAME, suffix)
}

impl<'a> ToTokens for DeriveStructuralReadable<'a, SegregatedEnumModel<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model, generics) = self;
        let SegregatedEnumModel {
            inner, variants, ..
        } = model;
        let name = inner.name;
        let root = inner.root;

        let mut new_generics = (*generics).clone();
        super::add_bounds(
            generics,
            &mut new_generics,
            parse_quote!(#root::structural::read::recognizer::RecognizerReadable),
        );

        let (impl_gen, type_gen, where_clause) = new_generics.split_for_impl();

        let enum_ty: syn::Type = parse_quote!(#name #type_gen);

        if variants.is_empty() {
            tokens.append_all(quote! {
                #[automatically_derived]
                #[allow(non_snake_case)]
                impl #impl_gen #root::structural::read::recognizer::RecognizerReadable for #name #type_gen
                #where_clause
                {
                    type Rec = #root::structural::read::recognizer::RecognizeNothing<#enum_ty>;
                    type AttrRec = #root::structural::read::recognizer::RecognizeNothing<#enum_ty>;
                    type BodyRec = Self::Rec;

                    #[inline]
                    fn make_recognizer() -> Self::Rec {
                        ::core::default::Default::default()
                    }

                    #[inline]
                    fn make_attr_recognizer() -> Self::AttrRec {
                        ::core::default::Default::default()
                    }

                    #[inline]
                    fn make_body_recognizer() -> Self::BodyRec {
                        <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                    }
                }
            });
        } else {
            let variant_functions = variants
                .iter()
                .enumerate()
                .filter(|(_, model)| model.inner.fields_model.type_kind != CompoundTypeKind::Unit)
                .map(|(i, model)| {
                    let builder_type = RecognizerState::new(model, &enum_ty);

                    let select_index = match &model.fields.body {
                        BodyFields::StdBody(body_fields)
                        if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled =>
                            {
                                SelectIndexFnLabelled::variant(model, body_fields, i).into_token_stream()
                            }
                        _ => SelectIndexFnOrdinal::variant(model, i).into_token_stream(),
                    };

                    let select_feed_name = suffix_ident(SELECT_FEED_NAME, i);
                    let on_done_name = suffix_ident(ON_DONE_NAME, i);
                    let on_reset_name = suffix_ident(ON_RESET_NAME, i);
                    let builder_name = suffixed_builder_ident(i);

                    let select_feed = SelectFeedFn::new(model);
                    let var_name = model.inner.name;
                    let constructor = parse_quote!(#name::#var_name);
                    let on_done = OnDoneFn::new(model, constructor);

                    let on_reset = ResetFn::new(root, model.fields.num_field_blocks());

                    let header_recog_block = if model.fields.header.header_fields.is_empty() {
                        None
                    } else {
                        let HeaderFields { tag_body, header_fields, ..} = &model.fields.header;
                        Some(HeaderRecognizerFns::variant(
                            root,
                            &enum_ty,
                            *tag_body,
                            header_fields.as_slice(),
                            &new_generics,
                            i
                        ))
                    };
                    quote! {
                        type #builder_name #type_gen = #builder_type;
                        #select_index

                        #[automatically_derived]
                        #[allow(non_snake_case)]
                        fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: #root::structural::read::event::ReadEvent<'_>)
                            -> ::core::option::Option<::core::result::Result<(), #root::structural::read::error::ReadError>>
                        #where_clause
                        {
                            #select_feed
                        }

                        #[automatically_derived]
                        #[allow(non_snake_case)]
                        fn #on_done_name #impl_gen(state: &mut #builder_name #type_gen) -> ::core::result::Result<#enum_ty, #root::structural::read::error::ReadError>
                        #where_clause
                        {
                            #on_done
                        }

                        #[automatically_derived]
                        #[allow(non_snake_case)]
                        fn #on_reset_name #impl_gen(state: &mut #builder_name #type_gen)
                        #where_clause
                        {
                            #on_reset
                        }

                        #header_recog_block

                    }
                });

            let state = EnumState::new(model, &type_gen);
            let select_var_name = select_var_name();
            let builder_name = builder_ident();
            let select_var = SelectVariantFn::new(model, &type_gen);

            let recog_ty = quote!(#root::structural::read::recognizer::TaggedEnumRecognizer<#builder_name #type_gen>);

            tokens.append_all(quote! {
                const _: () = {
                    #(#variant_functions)*

                    #state

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    #[inline]
                    fn #select_var_name #impl_gen(name: &str) -> ::core::option::Option<#builder_name #type_gen>
                    #where_clause
                    {
                         #select_var
                    }

                    #[automatically_derived]
                    #[allow(non_snake_case)]
                    impl #impl_gen #root::structural::read::recognizer::RecognizerReadable for #name #type_gen
                    #where_clause
                    {
                        type Rec = #recog_ty;
                        type AttrRec = #root::structural::read::recognizer::SimpleAttrBody<
                            #recog_ty,
                        >;
                        type BodyRec = Self::Rec;

                        #[inline]
                        fn make_recognizer() -> Self::Rec {
                            <#recog_ty>::new(
                                #select_var_name
                            )
                        }

                        #[inline]
                        fn make_attr_recognizer() -> Self::AttrRec {
                            #root::structural::read::recognizer::SimpleAttrBody::new(
                                <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                            )
                        }

                        #[inline]
                        fn make_body_recognizer() -> Self::BodyRec {
                            <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                        }
                    }
                };
            });
        }
    }
}

struct RecognizerState<'a> {
    target: &'a syn::Type,
    model: &'a SegregatedStructModel<'a>,
}

impl<'a> RecognizerState<'a> {
    fn new(model: &'a SegregatedStructModel<'a>, target: &'a syn::Type) -> Self {
        RecognizerState { target, model }
    }
}

impl<'a> ToTokens for RecognizerState<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let RecognizerState {
            target,
            model:
                SegregatedStructModel {
                    inner: StructModel { root, .. },
                    fields,
                    ..
                },
        } = self;
        let it = enumerate_fields(fields);

        let builder_types = it.clone().map(|grp| match grp {
            FieldGroup::Tag(fld)
            | FieldGroup::Item(fld)
            | FieldGroup::DelegateBody(fld)
            | FieldGroup::Attribute(fld) => {
                let ty = fld.field_ty;
                quote!(::core::option::Option<#ty>)
            }
            FieldGroup::Header {
                tag_body,
                header_fields,
            } => match tag_body {
                Some(fld) if header_fields.is_empty() => {
                    let ty = fld.field_ty;
                    quote!(::core::option::Option<#ty>)
                }
                ow => {
                    let header_fields = HeaderFieldsState {
                        tag_body: ow,
                        header_fields,
                    };
                    quote!(::core::option::Option<#header_fields>)
                }
            },
        });

        let recognizer_types = it.map(|grp| {
            match grp {
                FieldGroup::Tag(fld) => {
                    let ty = fld.field_ty;
                    quote!(#root::structural::read::recognizer::TagRecognizer<#ty>)
                }
                FieldGroup::Attribute(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::AttrRec)
                }
                FieldGroup::Item(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::Rec)
                }
                FieldGroup::DelegateBody(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::BodyRec)
                }
                FieldGroup::Header { tag_body, header_fields } => {
                    match tag_body {
                        Some(fld) if header_fields.is_empty() => {
                            let ty = fld.field_ty;
                            quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::AttrRec)
                        }
                        ow => {
                            let header_rep = HeaderFieldsState { tag_body: ow, header_fields };
                            let recog_state = HeaderRecognizerState {
                                root,
                                target,
                                tag_body: ow,
                                header_fields,
                            };
                            let header_recognizer = quote!(#root::structural::read::recognizer::HeaderRecognizer<#header_rep, #recog_state>);
                            quote!(#root::structural::read::recognizer::FirstOf<#header_recognizer, #header_recognizer>)
                        }
                    }
                }
            }

        });

        tokens.append_all(quote! {
            ((#(#builder_types,)*), (#(#recognizer_types,)*), ::core::marker::PhantomData<fn() -> #target>)
        });
    }
}

struct HeaderFieldsState<'a> {
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
}

impl<'a> ToTokens for HeaderFieldsState<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderFieldsState {
            tag_body,
            header_fields,
        } = self;
        let it = tag_body.iter().chain(header_fields.iter());

        let builder_types = it.clone().map(|fld| {
            let ty = fld.field_ty;
            quote!(::core::option::Option<#ty>)
        });

        tokens.append_all(quote! {
            (#(#builder_types,)*)
        });
    }
}

struct HeaderRecognizerState<'a> {
    root: &'a syn::Path,
    target: &'a syn::Type,
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
}

impl<'a> HeaderRecognizerState<'a> {
    fn new(
        root: &'a syn::Path,
        target: &'a syn::Type,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
    ) -> Self {
        HeaderRecognizerState {
            root,
            target,
            tag_body,
            header_fields,
        }
    }
}

impl<'a> ToTokens for HeaderRecognizerState<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderRecognizerState {
            root,
            target,
            tag_body,
            header_fields,
        } = self;

        let it = tag_body.iter().chain(header_fields.iter());

        let fields_state = HeaderFieldsState {
            tag_body: *tag_body,
            header_fields,
        };

        let recognizer_types = it.map(|fld| {
            let ty = fld.field_ty;
            quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::Rec)
        });

        tokens.append_all(quote! {
            (#fields_state, (#(#recognizer_types,)*), ::core::marker::PhantomData<fn() -> #target>)
        });
    }
}

struct SelectIndexFnLabelled<'a> {
    fields: &'a SegregatedStructModel<'a>,
    body_fields: &'a [&'a FieldModel<'a>],
    variant: Option<usize>,
}

impl<'a> SelectIndexFnLabelled<'a> {
    fn new(model: &'a SegregatedStructModel<'a>, body_fields: &'a [&'a FieldModel<'a>]) -> Self {
        SelectIndexFnLabelled {
            fields: model,
            body_fields,
            variant: None,
        }
    }

    fn variant(
        model: &'a SegregatedStructModel<'a>,
        body_fields: &'a [&'a FieldModel<'a>],
        variant_ordinal: usize,
    ) -> Self {
        SelectIndexFnLabelled {
            fields: model,
            body_fields,
            variant: Some(variant_ordinal),
        }
    }
}

const SELECT_INDEX_NAME: &str = "select_index";
const HEADER_SELECT_INDEX_NAME: &str = "select_index_header";

fn select_index_name() -> syn::Ident {
    syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
}
fn headerselect_index_name() -> syn::Ident {
    syn::Ident::new(HEADER_SELECT_INDEX_NAME, Span::call_site())
}

const SELECT_FEED_NAME: &str = "select_feed";
const HEADER_SELECT_FEED_NAME: &str = "header_select_feed";

fn select_feed_name() -> syn::Ident {
    syn::Ident::new(SELECT_FEED_NAME, Span::call_site())
}

fn header_select_feed_name() -> syn::Ident {
    syn::Ident::new(HEADER_SELECT_FEED_NAME, Span::call_site())
}

const ON_DONE_NAME: &str = "on_done";

fn on_done_name() -> syn::Ident {
    syn::Ident::new(ON_DONE_NAME, Span::call_site())
}
const ON_RESET_NAME: &str = "on_reset";
const HEADER_ON_RESET_NAME: &str = "header_on_reset";

fn on_reset_name() -> syn::Ident {
    syn::Ident::new(ON_RESET_NAME, Span::call_site())
}
fn header_on_reset_name() -> syn::Ident {
    syn::Ident::new(HEADER_ON_RESET_NAME, Span::call_site())
}

const SELECT_VAR_NAME: &str = "select_variant";

fn select_var_name() -> syn::Ident {
    syn::Ident::new(SELECT_VAR_NAME, Span::call_site())
}

const BUILDER_NAME: &str = "Builder";
const HEADER_BUILDER_NAME: &str = "HeaderBuilder";

impl<'a> ToTokens for SelectIndexFnLabelled<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnLabelled {
            fields,
            body_fields,
            variant,
        } = self;
        let SegregatedStructModel {
            inner: StructModel { root, .. },
            fields,
            ..
        } = fields;
        let SegregatedFields { header, .. } = fields;
        let HeaderFields {
            tag_name,
            tag_body,
            header_fields,
            attributes,
        } = header;

        let mut offset: u32 = 0;

        let name_case = if tag_name.is_some() {
            offset += 1;
            Some(quote! {
                #root::structural::read::recognizer::LabelledFieldKey::Tag => ::core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_case = if tag_body.is_some() || !header_fields.is_empty() {
            let case = Some(quote! {
                #root::structural::read::recognizer::LabelledFieldKey::Header => ::core::option::Option::Some(#offset),
            });
            offset += 1;
            case
        } else {
            None
        };

        let attr_cases = attributes.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                #root::structural::read::recognizer::LabelledFieldKey::Attr(#name) => ::core::option::Option::Some(#n),
            })
        });

        offset += attributes.len() as u32;

        let body_cases = body_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                #root::structural::read::recognizer::LabelledFieldKey::Item(#name) => ::core::option::Option::Some(#n),
            })
        });

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(SELECT_INDEX_NAME, *var_ord)
        } else {
            syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            #[allow(non_snake_case)]
            fn #fn_name(key: #root::structural::read::recognizer::LabelledFieldKey<'_>) -> ::core::option::Option<u32> {
                match key {
                    #name_case
                    #header_case
                    #(#attr_cases)*
                    #(#body_cases)*
                    _ => ::core::option::Option::None,
                }
            }
        })
    }
}

struct SelectIndexFnOrdinal<'a> {
    fields: &'a SegregatedStructModel<'a>,
    variant: Option<usize>,
}

impl<'a> SelectIndexFnOrdinal<'a> {
    fn new(model: &'a SegregatedStructModel<'a>) -> Self {
        SelectIndexFnOrdinal {
            fields: model,
            variant: None,
        }
    }

    fn variant(model: &'a SegregatedStructModel<'a>, variant_ordinal: usize) -> Self {
        SelectIndexFnOrdinal {
            fields: model,
            variant: Some(variant_ordinal),
        }
    }
}

impl<'a> ToTokens for SelectIndexFnOrdinal<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnOrdinal { fields, variant } = self;
        let SegregatedStructModel {
            inner: StructModel { root, .. },
            fields,
            ..
        } = fields;
        let SegregatedFields { header, .. } = fields;
        let HeaderFields {
            tag_name,
            tag_body,
            header_fields,
            attributes,
        } = header;

        let mut offset: u32 = 0;

        let name_case = if tag_name.is_some() {
            offset += 1;
            Some(quote! {
                #root::structural::read::recognizer::OrdinalFieldKey::Tag => ::core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_case = if tag_body.is_some() || !header_fields.is_empty() {
            let case = Some(quote! {
                #root::structural::read::recognizer::OrdinalFieldKey::Header => ::core::option::Option::Some(#offset),
            });
            offset += 1;
            case
        } else {
            None
        };

        let attr_cases = attributes.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                #root::structural::read::recognizer::OrdinalFieldKey::Attr(#name) => ::core::option::Option::Some(#n),
            })
        });

        offset += attributes.len() as u32;

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(SELECT_INDEX_NAME, *var_ord)
        } else {
            syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            #[allow(non_snake_case)]
            fn #fn_name(key: #root::structural::read::recognizer::OrdinalFieldKey<'_>) -> ::core::option::Option<u32> {
                match key {
                    #name_case
                    #header_case
                    #(#attr_cases)*
                    #root::structural::read::recognizer::OrdinalFieldKey::FirstItem => ::core::option::Option::Some(#offset),
                    _ => ::core::option::Option::None,
                }
            }
        })
    }
}

struct SelectFeedFn<'a> {
    fields: &'a SegregatedStructModel<'a>,
}

impl<'a> SelectFeedFn<'a> {
    fn new(model: &'a SegregatedStructModel<'a>) -> Self {
        SelectFeedFn { fields: model }
    }
}

#[derive(Clone, Copy)]
enum FieldGroup<'a> {
    Tag(&'a FieldModel<'a>),
    Header {
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
    },
    Attribute(&'a FieldModel<'a>),
    Item(&'a FieldModel<'a>),
    DelegateBody(&'a FieldModel<'a>),
}

impl<'a> FieldGroup<'a> {
    fn single_field(&self) -> Option<&FieldModel<'a>> {
        match self {
            FieldGroup::Tag(fld) => Some(fld),
            FieldGroup::Header { .. } => None,
            FieldGroup::Attribute(fld) => Some(fld),
            FieldGroup::Item(fld) => Some(fld),
            FieldGroup::DelegateBody(fld) => Some(fld),
        }
    }
}

/// Enumerates the fields in a descriptor in the order in which the implementation exepects to
/// receive them.
fn enumerate_fields<'a>(
    model: &'a SegregatedFields<'a>,
) -> impl Iterator<Item = FieldGroup<'a>> + Clone + 'a {
    let SegregatedFields { header, body } = model;
    let HeaderFields {
        tag_name,
        tag_body,
        header_fields,
        attributes,
    } = header;

    let body_fields = match body {
        BodyFields::StdBody(vec) => Either::Left(vec.iter().copied().map(FieldGroup::Item)),
        BodyFields::ReplacedBody(fld) => {
            Either::Right(std::iter::once(FieldGroup::DelegateBody(fld)))
        }
    };

    let header = if tag_body.is_none() && header_fields.is_empty() {
        None
    } else {
        Some(FieldGroup::Header {
            tag_body: *tag_body,
            header_fields: header_fields.as_slice(),
        })
    };

    tag_name
        .iter()
        .copied()
        .map(FieldGroup::Tag)
        .chain(header)
        .chain(attributes.iter().copied().map(FieldGroup::Attribute))
        .chain(body_fields.into_iter())
}

impl<'a> ToTokens for SelectFeedFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectFeedFn { fields } = self;
        let SegregatedStructModel {
            inner: StructModel { root, .. },
            fields,
            ..
        } = fields;

        let it = enumerate_fields(fields);

        let cases = it.enumerate().map(|(i, grp)| {
            let name = if let Some(fld) = grp.single_field() {
                let fld_name = fld.resolve_name();
                quote!(#fld_name)
            } else {
                "__header".to_token_stream()
            };

            let idx = syn::Index::from(i);
            let case_index = i as u32;
            quote! {
                #case_index => #root::structural::read::recognizer::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            match index {
                #(#cases)*
                _ => ::core::option::Option::Some(::core::result::Result::Err(#root::structural::read::error::ReadError::InconsistentState)),
            }
        })
    }
}

struct OnDoneFn<'a> {
    fields: &'a SegregatedStructModel<'a>,
    constructor: syn::Path,
}

impl<'a> OnDoneFn<'a> {
    fn new(model: &'a SegregatedStructModel<'a>, constructor: syn::Path) -> Self {
        OnDoneFn {
            fields: model,
            constructor,
        }
    }
}

impl<'a> ToTokens for OnDoneFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let OnDoneFn {
            fields,
            constructor,
        } = self;
        let SegregatedStructModel { inner, fields, .. } = fields;

        let root = inner.root;
        let it = enumerate_fields(fields);

        let validators = it.clone().enumerate().map(|(i, grp)| {
            let idx = syn::Index::from(i);
            match grp {
                FieldGroup::Tag(fld) => {
                    let name = fld.resolve_name();
                    quote! {
                        if fields.#idx.is_none() {
                            missing.push(#root::model::Text::new(#name));
                        }
                    }
                }
                FieldGroup::Header { tag_body, header_fields } => {
                    match tag_body {
                        Some(fld) if header_fields.is_empty() => {
                            let name = fld.resolve_name();
                            let ty = fld.field_ty;

                            quote! {
                                if fields.#idx.is_none() {
                                    fields.#idx = <#ty as #root::structural::read::recognizer::RecognizerReadable>::on_absent();
                                    if fields.#idx.is_none() {
                                        missing.push(#root::model::Text::new(#name));
                                    }
                                }
                            }
                        }
                        ow => {
                            let mut acc = TokenStream::new();
                            acc.append_all(quote! {
                                let header = ::core::option::Option::get_or_insert_with(&mut fields.#idx, ::core::default::Default::default);
                            });
                            ow.iter().chain(header_fields.iter()).enumerate().fold(acc, |mut out, (j, fld)| {
                                let inner_idx = syn::Index::from(j);
                                let name = fld.resolve_name();
                                let ty = fld.field_ty;

                                out.append_all(quote! {
                                    if header.#inner_idx.is_none() {
                                        header.#inner_idx = <#ty as #root::structural::read::recognizer::RecognizerReadable>::on_absent();
                                        if header.#inner_idx.is_none() {
                                            missing.push(#root::model::Text::new(#name));
                                        }
                                    }
                                });
                                out
                            })
                        }
                    }
                }
                FieldGroup::Attribute(fld) | FieldGroup::Item(fld) | FieldGroup::DelegateBody(fld) => {
                    let name = fld.resolve_name();
                    let ty = fld.field_ty;

                    quote! {
                        if fields.#idx.is_none() {
                            fields.#idx = <#ty as #root::structural::read::recognizer::RecognizerReadable>::on_absent();
                            if fields.#idx.is_none() {
                                missing.push(#root::model::Text::new(#name));
                            }
                        }
                    }
                }
            }

        });

        let field_dest = it.clone().map(|grp| match grp {
            FieldGroup::Item(fld)
            | FieldGroup::DelegateBody(fld)
            | FieldGroup::Attribute(fld)
            | FieldGroup::Tag(fld) => {
                let name = &fld.selector;
                quote!(::core::option::Option::Some(#name))
            }
            FieldGroup::Header {
                tag_body,
                header_fields,
            } => match tag_body {
                Some(fld) if header_fields.is_empty() => {
                    let name = &fld.selector;
                    quote!(::core::option::Option::Some(#name))
                }
                ow => {
                    let header_fields = ow.iter().chain(header_fields.iter()).map(|fld| {
                        let name = &fld.selector;
                        quote!(::core::option::Option::Some(#name))
                    });
                    quote!(::core::option::Option::Some((#(#header_fields,)*)))
                }
            },
        });

        let num_fields = inner.fields_model.fields.len();
        let num_blocks = fields.num_field_blocks();
        let field_takes = (0..num_blocks).map(|i| {
            let idx = syn::Index::from(i);
            quote!(fields.#idx.take())
        });

        let make_result = match inner.fields_model.type_kind {
            CompoundTypeKind::Labelled => {
                let con_params = inner.fields_model.fields.iter().map(|fld| {
                    let bind = if fld.directive == FieldKind::Skip {
                        fld.model.selector.default_binder()
                    } else {
                        fld.model.selector.binder()
                    };
                    quote!(#bind)
                });
                quote! {
                    #constructor {
                        #(#con_params,)*
                    }
                }
            }
            CompoundTypeKind::Unit => {
                quote!(#constructor)
            }
            _ => {
                let name_map = inner
                    .fields_model
                    .fields
                    .iter()
                    .filter(|fld| fld.directive != FieldKind::Skip)
                    .map(|fld| (fld.model.ordinal, &fld.model.selector))
                    .collect::<HashMap<_, _>>();
                let con_params = (0..num_fields).map(|i| {
                    if let Some(name) = name_map.get(&i) {
                        quote!(#name)
                    } else {
                        quote!(::core::default::Default::default())
                    }
                });
                quote! {
                    #constructor(#(#con_params,)*)
                }
            }
        };

        tokens.append_all(quote! {

            let (fields, _, _) = state;
            let mut missing = ::std::vec![];

            #(#validators)*

            if let (#(#field_dest,)*) = (#(#field_takes,)*) {
                ::core::result::Result::Ok(#make_result)
            } else {
                ::core::result::Result::Err(#root::structural::read::error::ReadError::MissingFields(missing))
            }
        })
    }
}

struct ResetFn<'a> {
    root: &'a syn::Path,
    num_fields: usize,
}

impl<'a> ResetFn<'a> {
    pub fn new(root: &'a syn::Path, num_fields: usize) -> Self {
        ResetFn { root, num_fields }
    }
}

impl<'a> ToTokens for ResetFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResetFn { root, num_fields } = self;

        let field_resets = (0..*num_fields).map(|i| {
            let idx = syn::Index::from(i);
            quote! {
                fields.#idx = ::core::option::Option::None;
                #root::structural::read::recognizer::Recognizer::reset(&mut recognizers.#idx);
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            #(#field_resets)*
        })
    }
}

struct ConstructFieldRecognizers<'a> {
    fields: &'a SegregatedStructModel<'a>,
    variant: Option<usize>,
}

impl<'a> ToTokens for ConstructFieldRecognizers<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ConstructFieldRecognizers { fields, variant } = self;
        let SegregatedStructModel {
            inner: StructModel { root, .. },
            fields,
            ..
        } = fields;
        let initializers = enumerate_fields(fields)
            .map(|grp| {
                match grp {
                    FieldGroup::Tag(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#root::structural::read::recognizer::TagRecognizer<#ty> as ::core::default::Default>::default())
                    }
                    FieldGroup::Attribute(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::make_attr_recognizer())
                    }
                    FieldGroup::Item(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer())
                    }
                    FieldGroup::DelegateBody(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::make_body_recognizer())
                    }
                    FieldGroup::Header { tag_body, header_fields } => {
                        match tag_body {
                            Some(fld) if header_fields.is_empty() => {
                                let ty = fld.field_ty;
                                quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::make_attr_recognizer())
                            }
                            ow => {
                                let select_index_name = if let Some(i) = variant {
                                    suffix_ident(HEADER_SELECT_INDEX_NAME, *i)
                                } else {
                                    headerselect_index_name()
                                };
                                let (_, select_feed_name, on_reset_name) = header_identifiers(*variant);
                                let has_body = ow.is_some();
                                let num_slots = header_fields.len() as u32;

                                let recog_inits = ow.iter().chain(header_fields.iter()).map(|fld| {
                                    let ty = fld.field_ty;
                                    quote!(<#ty as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer())
                                });

                                let flds_constr= quote! {
                                    (::core::default::Default::default(), (#(#recog_inits,)*), ::core::marker::PhantomData)
                                };

                                quote! {
                                    #root::structural::read::recognizer::header_recognizer(
                                        #has_body,
                                        || #flds_constr,
                                        #num_slots,
                                        #root::structural::read::recognizer::HeaderVTable::new(
                                            #select_index_name,
                                            #select_feed_name,
                                            #root::structural::read::recognizer::take_fields,
                                            #on_reset_name
                                        )
                                    )
                                }
                            }
                        }
                    }
                }
            });
        tokens.append_all(quote! {
            (#(#initializers,)*)
        })
    }
}

struct StructReadableImpl<'a> {
    fields: &'a SegregatedStructModel<'a>,
    gen_params: &'a TypeGenerics<'a>,
}

impl<'a> StructReadableImpl<'a> {
    fn new(model: &'a SegregatedStructModel<'a>, gen_params: &'a TypeGenerics<'a>) -> Self {
        StructReadableImpl {
            fields: model,
            gen_params,
        }
    }
}

fn compound_recognizer(
    model: &SegregatedStructModel<'_>,
    target: &syn::Type,
    builder: &syn::Type,
) -> (syn::Type, syn::Type) {
    let (recog_ty_name, v_table_name) = if matches!(&model.fields.body, BodyFields::ReplacedBody(_))
    {
        (
            syn::Ident::new("DelegateStructRecognizer", Span::call_site()),
            syn::Ident::new("OrdinalVTable", Span::call_site()),
        )
    } else {
        let (r, v) = match (
            model.inner.fields_model.body_kind,
            model.inner.newtype_selector(),
        ) {
            (CompoundTypeKind::Labelled, Some(_)) => {
                ("LabelledNewtypeRecognizer", "LabelledVTable")
            }
            (CompoundTypeKind::Labelled, None) => ("LabelledStructRecognizer", "LabelledVTable"),
            (_, Some(_)) => ("OrdinalNewtypeRecognizer", "OrdinalVTable"),
            (_, None) => ("OrdinalStructRecognizer", "OrdinalVTable"),
        };
        (
            syn::Ident::new(r, Span::call_site()),
            syn::Ident::new(v, Span::call_site()),
        )
    };
    let root = model.inner.root;
    let recog_ty =
        parse_quote!(#root::structural::read::recognizer::#recog_ty_name<#target, #builder>);
    let vtable_ty =
        parse_quote!(#root::structural::read::recognizer::#v_table_name<#target, #builder>);
    (recog_ty, vtable_ty)
}

impl<'a> ToTokens for StructReadableImpl<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let StructReadableImpl { fields, gen_params } = self;
        let name = fields.inner.name;
        let root = fields.inner.root;

        let tag = if fields.fields.header.tag_name.is_some() {
            quote!(#root::structural::read::recognizer::TagSpec::Field)
        } else {
            let lit_name = fields.inner.resolve_name();
            quote!(#root::structural::read::recognizer::TagSpec::Fixed(#lit_name))
        };

        let make_fld_recog = ConstructFieldRecognizers {
            fields,
            variant: None,
        };
        let num_fields = fields.inner.fields_model.fields.len() as u32;

        let builder_name = builder_ident();

        let target = parse_quote!(#name #gen_params);
        let builder = parse_quote!(#builder_name #gen_params);
        let (recog_ty, vtable_ty) = compound_recognizer(fields, &target, &builder);

        let select_index = select_index_name();
        let select_feed = select_feed_name();
        let on_done = on_done_name();
        let on_reset = on_reset_name();

        let make_recognizer_fn_body = if fields.inner.newtype_selector().is_some() {
            quote! {
            <#recog_ty>::new(
                    (::core::default::Default::default(), #make_fld_recog, ::core::marker::PhantomData),
                    <#vtable_ty>::new(
                        #select_index,
                        #select_feed,
                        #on_done,
                        #on_reset,
                    )
                )
            }
        } else {
            quote! {
                <#recog_ty>::new(
                    #tag,
                    (::core::default::Default::default(), #make_fld_recog, ::core::marker::PhantomData),
                    #num_fields,
                    <#vtable_ty>::new(
                        #select_index,
                        #select_feed,
                        #on_done,
                        #on_reset,
                    )
                )
            }
        };

        tokens.append_all(quote! {
            type Rec = #recog_ty;
            type AttrRec = #root::structural::read::recognizer::SimpleAttrBody<
                #recog_ty,
            >;
            type BodyRec = Self::Rec;

            #[allow(non_snake_case)]
            #[inline]
            fn make_recognizer() -> Self::Rec {
               #make_recognizer_fn_body
            }

            #[inline]
            fn make_attr_recognizer() -> Self::AttrRec {
                #root::structural::read::recognizer::SimpleAttrBody::new(
                    <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                )
            }

            #[inline]
            fn make_body_recognizer() -> Self::BodyRec {
                <Self as #root::structural::read::recognizer::RecognizerReadable>::make_recognizer()
            }
        })
    }
}

struct SelectVariantFn<'a> {
    model: &'a SegregatedEnumModel<'a>,
    gen_params: &'a TypeGenerics<'a>,
}

impl<'a> SelectVariantFn<'a> {
    fn new(model: &'a SegregatedEnumModel<'a>, gen_params: &'a TypeGenerics<'a>) -> Self {
        SelectVariantFn { model, gen_params }
    }
}

impl<'a> ToTokens for SelectVariantFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectVariantFn {
            model: SegregatedEnumModel { inner, variants },
            gen_params,
        } = self;

        let name = inner.name;
        let root = inner.root;
        let enum_ty = parse_quote!(#name #gen_params);

        let cases = variants.iter().enumerate().map(|(i, var)| {
            let lit_name = var.inner.resolve_name();
            let constructor = if var.inner.fields_model.type_kind == CompoundTypeKind::Unit {
                let var_name = var.inner.name;
                parse_quote!(#root::structural::read::recognizer::UnitStructRecognizer::variant(|| #name::#var_name))
            } else {
                let builder_name = suffixed_builder_ident(i);
                let builder_ty = parse_quote!(#builder_name #gen_params);
                let (recognizer, vtable) = compound_recognizer(var, &enum_ty, &builder_ty);
                let make_fld_recog = ConstructFieldRecognizers { fields: var, variant: Some(i) };

                let num_fields = var.inner.fields_model.fields.len() as u32;
                let select_index = suffix_ident(SELECT_INDEX_NAME, i);
                let select_feed = suffix_ident(SELECT_FEED_NAME, i);
                let on_done = suffix_ident(ON_DONE_NAME, i);
                let on_reset = suffix_ident(ON_RESET_NAME, i);

                parse_quote! {
                    <#recognizer>::variant(
                        (::core::default::Default::default(), #make_fld_recog, ::core::marker::PhantomData),
                        #num_fields,
                        <#vtable>::new(
                            #select_index,
                            #select_feed,
                            #on_done,
                            #on_reset,
                        )
                    )
                }
            };

            let ccons_constructor = make_ccons(root, i, constructor);
            quote! {
                #lit_name => ::core::option::Option::Some(#ccons_constructor)
            }
        });

        tokens.append_all(quote! {
            match name {
                #(#cases,)*
                _ => None,
            }
        });
    }
}

struct EnumState<'a> {
    model: &'a SegregatedEnumModel<'a>,
    gen_params: &'a TypeGenerics<'a>,
}

impl<'a> EnumState<'a> {
    fn new(model: &'a SegregatedEnumModel<'a>, gen_params: &'a TypeGenerics<'a>) -> Self {
        EnumState { model, gen_params }
    }
}

impl<'a> ToTokens for EnumState<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let EnumState {
            model: SegregatedEnumModel { inner, variants },
            gen_params,
        } = self;
        let root = inner.root;
        let base: syn::Type = parse_quote!(#root::structural::generic::coproduct::CNil);

        let name = inner.name;

        let enum_ty = parse_quote!(#name #gen_params);

        let ccons_type = variants
            .iter()
            .enumerate()
            .rev()
            .fold(base, |acc, (i, var)| {
                let ty = if var.inner.fields_model.type_kind == CompoundTypeKind::Unit {
                    parse_quote!(#root::structural::read::recognizer::UnitStructRecognizer<#enum_ty>)
                } else {
                    let builder_name = suffixed_builder_ident(i);
                    let builder = parse_quote!(#builder_name #gen_params);
                    let (ty, _) = compound_recognizer(var, &enum_ty, &builder);
                    ty
                };
                parse_quote!(#root::structural::generic::coproduct::CCons<#ty, #acc>)
            });

        let builder_name = builder_ident();

        tokens.append_all(quote! {
            type #builder_name #gen_params = #ccons_type;
        })
    }
}

fn make_ccons(root: &syn::Path, n: usize, expr: syn::Expr) -> syn::Expr {
    let mut acc = parse_quote!(#root::structural::generic::coproduct::CCons::Head(#expr));
    for _ in 0..n {
        acc = parse_quote!(#root::structural::generic::coproduct::CCons::Tail(#acc));
    }
    acc
}

struct HeaderSelectIndexFn<'a> {
    root: &'a syn::Path,
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
    variant: Option<usize>,
}

impl<'a> HeaderSelectIndexFn<'a> {
    fn new(
        root: &'a syn::Path,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
        variant: Option<usize>,
    ) -> Self {
        HeaderSelectIndexFn {
            root,
            tag_body,
            header_fields,
            variant,
        }
    }
}

struct HeaderFeedFn<'a> {
    root: &'a syn::Path,
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
}

impl<'a> HeaderFeedFn<'a> {
    fn new(
        root: &'a syn::Path,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
    ) -> Self {
        HeaderFeedFn {
            root,
            tag_body,
            header_fields,
        }
    }
}

impl<'a> ToTokens for HeaderSelectIndexFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderSelectIndexFn {
            root,
            tag_body,
            header_fields,
            variant,
        } = self;

        let mut offset: u32 = 0;

        let body_case = if tag_body.is_some() {
            offset += 1;
            Some(quote! {
                #root::structural::read::recognizer::HeaderFieldKey::HeaderBody => ::core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_slot_cases = header_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                #root::structural::read::recognizer::HeaderFieldKey::HeaderSlot(#name) => ::core::option::Option::Some(#n),
            })
        });

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(HEADER_SELECT_INDEX_NAME, *var_ord)
        } else {
            headerselect_index_name()
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            #[allow(non_snake_case)]
            fn #fn_name(key: #root::structural::read::recognizer::HeaderFieldKey<'_>) -> ::core::option::Option<u32> {
                match key {
                    #body_case
                    #(#header_slot_cases)*
                    _ => ::core::option::Option::None,
                }
            }
        })
    }
}

impl<'a> ToTokens for HeaderFeedFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderFeedFn {
            root,
            tag_body,
            header_fields,
        } = self;

        let it = tag_body.iter().chain(header_fields.iter());

        let cases = it.enumerate().map(|(i, fld)| {
            let name = fld.resolve_name();

            let idx = syn::Index::from(i);
            let case_index = i as u32;
            quote! {
                #case_index => #root::structural::read::recognizer::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            match index {
                #(#cases)*
                _ => ::core::option::Option::Some(::core::result::Result::Err(#root::structural::read::error::ReadError::InconsistentState)),
            }
        })
    }
}

/// If the record has fields lifted into its header, a separate recognizer is required (reading
/// the header is ambiguous as the items can be flattened into the attribute, or not, which requires
/// two copies of the recognizer to be run in parallel). This subsidiary recognizer requires its
/// own v-table. This generates the functions required to populate that table.
struct HeaderRecognizerFns<'a> {
    root: &'a syn::Path,
    target: &'a syn::Type,
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
    variant: Option<usize>,
    generics: &'a Generics,
}

impl<'a> HeaderRecognizerFns<'a> {
    fn new(
        root: &'a syn::Path,
        target: &'a syn::Type,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
        generics: &'a Generics,
    ) -> Self {
        HeaderRecognizerFns {
            root,
            target,
            tag_body,
            header_fields,
            variant: None,
            generics,
        }
    }

    fn variant(
        root: &'a syn::Path,
        target: &'a syn::Type,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
        generics: &'a Generics,
        variant: usize,
    ) -> Self {
        HeaderRecognizerFns {
            root,
            target,
            tag_body,
            header_fields,
            variant: Some(variant),
            generics,
        }
    }
}

fn header_identifiers(variant: Option<usize>) -> (syn::Ident, syn::Ident, syn::Ident) {
    if let Some(i) = variant {
        (
            suffixed_header_builder_ident(i),
            suffix_ident(HEADER_SELECT_FEED_NAME, i),
            suffix_ident(HEADER_ON_RESET_NAME, i),
        )
    } else {
        (
            header_builder_ident(),
            header_select_feed_name(),
            header_on_reset_name(),
        )
    }
}

impl<'a> ToTokens for HeaderRecognizerFns<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderRecognizerFns {
            root,
            target,
            tag_body,
            header_fields,
            variant,
            generics,
        } = self;

        let (builder_name, select_feed_name, on_reset_name) = header_identifiers(*variant);

        let header_builder_type =
            HeaderRecognizerState::new(root, target, *tag_body, header_fields);
        let select_index = HeaderSelectIndexFn::new(root, *tag_body, header_fields, *variant);
        let select_feed = HeaderFeedFn::new(root, *tag_body, header_fields);
        let num_fields = if tag_body.is_some() {
            header_fields.len() + 1
        } else {
            header_fields.len()
        };
        let on_reset = ResetFn::new(root, num_fields);

        let (impl_gen, type_gen, where_clause) = generics.split_for_impl();

        tokens.append_all(quote! {

            type #builder_name #type_gen = #header_builder_type;
            #select_index

            #[automatically_derived]
            #[allow(non_snake_case)]
            fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: #root::structural::read::event::ReadEvent<'_>)
                -> ::core::option::Option<::core::result::Result<(), #root::structural::read::error::ReadError>>
            #where_clause
            {
                #select_feed
            }

            #[automatically_derived]
            #[allow(non_snake_case)]
            fn #on_reset_name #impl_gen(state: &mut #builder_name #type_gen)
            #where_clause
            {
                 #on_reset
            }

        });
    }
}
