// Copyright 2015-2021 SWIM.AI inc.
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

use macro_helpers::{CompoundTypeKind, FieldKind};

use crate::quote::TokenStreamExt;
use crate::structural::model::enumeration::SegregatedEnumModel;
use crate::structural::model::field::{BodyFields, FieldModel, HeaderFields, SegregatedFields};
use crate::structural::model::record::SegregatedStructModel;

/// Implements the StructuralReadable trait for either of [`SegregatedStructModel`] or
/// [`SegregatedEnumModel`].
pub struct DeriveStructuralReadable<'a, S>(pub S, pub &'a Generics);

impl<'a, 'b> ToTokens for DeriveStructuralReadable<'b, SegregatedStructModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model, generics) = self;
        let mut new_generics = (*generics).clone();
        add_bounds(*generics, &mut new_generics);

        let (impl_gen, type_gen, where_clause) = new_generics.split_for_impl();
        let name = model.inner.name;
        if model.inner.fields_model.type_kind == CompoundTypeKind::Unit {
            let lit_name = model.inner.resolve_name();

            tokens.append_all(quote! {
                impl swim_common::form::structural::read::recognizer::RecognizerReadable for #name {
                    type Rec = swim_common::form::structural::read::recognizer::UnitStructRecognizer<#name>;
                    type AttrRec = swim_common::form::structural::read::recognizer::SimpleAttrBody<
                        swim_common::form::structural::read::recognizer::UnitStructRecognizer<#name>
                    >;
                    type BodyRec = Self::Rec;

                    #[allow(non_snake_case)]
                    #[inline]
                    fn make_recognizer() -> Self::Rec {
                        swim_common::form::structural::read::recognizer::UnitStructRecognizer::new(
                            #lit_name,
                            || #name
                        )
                    }

                    #[inline]
                    fn make_attr_recognizer() -> Self::AttrRec {
                        swim_common::form::structural::read::recognizer::SimpleAttrBody::new(
                            <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                        )
                    }

                    #[inline]
                    fn make_body_recognizer() -> Self::BodyRec {
                        <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
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
                    SelectIndexFnLabelled::new(model, &body_fields).into_token_stream()
                }
                _ => SelectIndexFnOrdinal::new(model).into_token_stream(),
            };

            let read_impl = StructReadableImpl::new(model, &type_gen);

            let on_reset = ResetFn::new(model.fields.num_field_blocks());

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
                    fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: swim_common::form::structural::read::event::ReadEvent<'_>)
                        -> core::option::Option<core::result::Result<(), swim_common::form::structural::read::error::ReadError>>
                    #where_clause
                    {
                        #select_feed
                    }

                    #[automatically_derived]
                    fn #on_done_name #impl_gen(state: &mut #builder_name #type_gen) -> core::result::Result<#target, swim_common::form::structural::read::error::ReadError>
                    #where_clause
                    {
                        #on_done
                    }

                    #[automatically_derived]
                    fn #on_reset_name #impl_gen(state: &mut #builder_name #type_gen)
                    #where_clause
                    {
                         #on_reset
                    }

                    #header_recog_block

                    #[automatically_derived]
                    impl #impl_gen swim_common::form::structural::read::recognizer::RecognizerReadable for #name #type_gen #where_clause {
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

impl<'a, 'b> ToTokens for DeriveStructuralReadable<'b, SegregatedEnumModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model, generics) = self;
        let SegregatedEnumModel { inner, variants } = model;
        let name = inner.name;

        let mut new_generics = (*generics).clone();
        add_bounds(*generics, &mut new_generics);

        let (impl_gen, type_gen, where_clause) = new_generics.split_for_impl();

        let enum_ty: syn::Type = parse_quote!(#name #type_gen);

        if variants.is_empty() {
            tokens.append_all(quote! {
                #[automatically_derived]
                impl #impl_gen swim_common::form::structural::read::recognizer::RecognizerReadable for #name #type_gen
                #where_clause
                {
                    type Rec = swim_common::form::structural::read::recognizer::RecognizeNothing<#enum_ty>;
                    type AttrRec = swim_common::form::structural::read::recognizer::RecognizeNothing<#enum_ty>;
                    type BodyRec = Self::Rec;

                    #[inline]
                    fn make_recognizer() -> Self::Rec {
                        core::default::Default::default()
                    }

                    #[inline]
                    fn make_attr_recognizer() -> Self::AttrRec {
                        core::default::Default::default()
                    }

                    #[inline]
                    fn make_body_recognizer() -> Self::BodyRec {
                        <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
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
                                SelectIndexFnLabelled::variant(model, &body_fields, i).into_token_stream()
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

                    let on_reset = ResetFn::new(model.fields.num_field_blocks());

                    let header_recog_block = if model.fields.header.header_fields.is_empty() {
                        None
                    } else {
                        let HeaderFields { tag_body, header_fields, ..} = &model.fields.header;
                        Some(HeaderRecognizerFns::variant(
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
                        fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: swim_common::form::structural::read::event::ReadEvent<'_>)
                            -> core::option::Option<core::result::Result<(), swim_common::form::structural::read::error::ReadError>>
                        #where_clause
                        {
                            #select_feed
                        }

                        #[automatically_derived]
                        fn #on_done_name #impl_gen(state: &mut #builder_name #type_gen) -> core::result::Result<#enum_ty, swim_common::form::structural::read::error::ReadError>
                        #where_clause
                        {
                            #on_done
                        }

                        #[automatically_derived]
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

            let recog_ty = quote!(swim_common::form::structural::read::recognizer::TaggedEnumRecognizer<#builder_name #type_gen>);

            tokens.append_all(quote! {
                const _: () = {
                    #(#variant_functions)*

                    #state

                    #[automatically_derived]
                    fn #select_var_name #impl_gen(name: &str) -> core::option::Option<#builder_name #type_gen>
                    #where_clause
                    {
                         #select_var
                    }

                    #[automatically_derived]
                    impl #impl_gen swim_common::form::structural::read::recognizer::RecognizerReadable for #name #type_gen
                    #where_clause
                    {
                        type Rec = #recog_ty;
                        type AttrRec = swim_common::form::structural::read::recognizer::SimpleAttrBody<
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
                            swim_common::form::structural::read::recognizer::SimpleAttrBody::new(
                                <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                            )
                        }

                        #[inline]
                        fn make_body_recognizer() -> Self::BodyRec {
                            <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                        }
                    }
                };
            });
        }
    }
}

struct RecognizerState<'a, 'b> {
    target: &'b syn::Type,
    model: &'b SegregatedStructModel<'a, 'b>,
}

impl<'a, 'b> RecognizerState<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>, target: &'b syn::Type) -> Self {
        RecognizerState { target, model }
    }
}

impl<'a, 'b> ToTokens for RecognizerState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let RecognizerState {
            target,
            model: SegregatedStructModel { fields, .. },
        } = self;
        let it = enumerate_fields(fields);

        let builder_types = it.clone().map(|grp| match grp {
            FieldGroup::Tag(fld)
            | FieldGroup::Item(fld)
            | FieldGroup::DelegateBody(fld)
            | FieldGroup::Attribute(fld) => {
                let ty = fld.field_ty;
                quote!(core::option::Option<#ty>)
            }
            FieldGroup::Header {
                tag_body,
                header_fields,
            } => match tag_body {
                Some(fld) if header_fields.is_empty() => {
                    let ty = fld.field_ty;
                    quote!(core::option::Option<#ty>)
                }
                ow => {
                    let header_fields = HeaderFieldsState {
                        tag_body: ow,
                        header_fields,
                    };
                    quote!(core::option::Option<#header_fields>)
                }
            },
        });

        let recognizer_types = it.map(|grp| {
            match grp {
                FieldGroup::Tag(fld) => {
                    let ty = fld.field_ty;
                    quote!(swim_common::form::structural::read::recognizer::TagRecognizer<#ty>)
                }
                FieldGroup::Attribute(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::AttrRec)
                }
                FieldGroup::Item(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::Rec)
                }
                FieldGroup::DelegateBody(fld) => {
                    let ty = fld.field_ty;
                    quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::BodyRec)
                }
                FieldGroup::Header { tag_body, header_fields } => {
                    match tag_body {
                        Some(fld) if header_fields.is_empty() => {
                            let ty = fld.field_ty;
                            quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::AttrRec)
                        }
                        ow => {
                            let header_rep = HeaderFieldsState { tag_body: ow, header_fields };
                            let recog_state = HeaderRecognizerState {
                                target: *target,
                                tag_body: ow,
                                header_fields,
                            };
                            let header_recognizer = quote!(swim_common::form::structural::read::recognizer::HeaderRecognizer<#header_rep, #recog_state>);
                            quote!(swim_common::form::structural::read::recognizer::FirstOf<#header_recognizer, #header_recognizer>)
                        }
                    }
                }
            }

        });

        tokens.append_all(quote! {
            ((#(#builder_types,)*), (#(#recognizer_types,)*), core::marker::PhantomData<fn() -> #target>)
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
            quote!(core::option::Option<#ty>)
        });

        tokens.append_all(quote! {
            (#(#builder_types,)*)
        });
    }
}

struct HeaderRecognizerState<'a, 'b> {
    target: &'b syn::Type,
    tag_body: Option<&'a FieldModel<'a>>,
    header_fields: &'a [&'a FieldModel<'a>],
}

impl<'a, 'b> HeaderRecognizerState<'a, 'b> {
    fn new(
        target: &'b syn::Type,
        tag_body: Option<&'a FieldModel<'a>>,
        header_fields: &'a [&'a FieldModel<'a>],
    ) -> Self {
        HeaderRecognizerState {
            target,
            tag_body,
            header_fields,
        }
    }
}

impl<'a, 'b> ToTokens for HeaderRecognizerState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderRecognizerState {
            target,
            tag_body,
            header_fields,
        } = self;

        let it = tag_body.iter().chain(header_fields.iter());

        let fields_state = HeaderFieldsState {
            tag_body: *tag_body,
            header_fields: *header_fields,
        };

        let recognizer_types = it.map(|fld| {
            let ty = fld.field_ty;
            quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::Rec)
        });

        tokens.append_all(quote! {
            (#fields_state, (#(#recognizer_types,)*), core::marker::PhantomData<fn() -> #target>)
        });
    }
}

struct SelectIndexFnLabelled<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    body_fields: &'b [&'b FieldModel<'a>],
    variant: Option<usize>,
}

impl<'a, 'b> SelectIndexFnLabelled<'a, 'b> {
    fn new(
        model: &'b SegregatedStructModel<'a, 'b>,
        body_fields: &'b [&'b FieldModel<'a>],
    ) -> Self {
        SelectIndexFnLabelled {
            fields: model,
            body_fields,
            variant: None,
        }
    }

    fn variant(
        model: &'b SegregatedStructModel<'a, 'b>,
        body_fields: &'b [&'b FieldModel<'a>],
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

impl<'a, 'b> ToTokens for SelectIndexFnLabelled<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnLabelled {
            fields,
            body_fields,
            variant,
        } = self;
        let SegregatedStructModel { fields, .. } = fields;
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
                swim_common::form::structural::read::recognizer::LabelledFieldKey::Tag => core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_case = if tag_body.is_some() || !header_fields.is_empty() {
            let case = Some(quote! {
                swim_common::form::structural::read::recognizer::LabelledFieldKey::Header => core::option::Option::Some(#offset),
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
                swim_common::form::structural::read::recognizer::LabelledFieldKey::Attr(#name) => core::option::Option::Some(#n),
            })
        });

        offset += attributes.len() as u32;

        let body_cases = body_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::recognizer::LabelledFieldKey::Item(#name) => core::option::Option::Some(#n),
            })
        });

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(SELECT_INDEX_NAME, *var_ord)
        } else {
            syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(key: swim_common::form::structural::read::recognizer::LabelledFieldKey<'_>) -> core::option::Option<u32> {
                match key {
                    #name_case
                    #header_case
                    #(#attr_cases)*
                    #(#body_cases)*
                    _ => core::option::Option::None,
                }
            }
        })
    }
}

struct SelectIndexFnOrdinal<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    variant: Option<usize>,
}

impl<'a, 'b> SelectIndexFnOrdinal<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        SelectIndexFnOrdinal {
            fields: model,
            variant: None,
        }
    }

    fn variant(model: &'b SegregatedStructModel<'a, 'b>, variant_ordinal: usize) -> Self {
        SelectIndexFnOrdinal {
            fields: model,
            variant: Some(variant_ordinal),
        }
    }
}

impl<'a, 'b> ToTokens for SelectIndexFnOrdinal<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnOrdinal { fields, variant } = self;
        let SegregatedStructModel { fields, .. } = fields;
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
                swim_common::form::structural::read::recognizer::OrdinalFieldKey::Tag => core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_case = if tag_body.is_some() || !header_fields.is_empty() {
            let case = Some(quote! {
                swim_common::form::structural::read::recognizer::OrdinalFieldKey::Header => core::option::Option::Some(#offset),
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
                swim_common::form::structural::read::recognizer::OrdinalFieldKey::Attr(#name) => core::option::Option::Some(#n),
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
            fn #fn_name(key: swim_common::form::structural::read::recognizer::OrdinalFieldKey<'_>) -> core::option::Option<u32> {
                match key {
                    #name_case
                    #header_case
                    #(#attr_cases)*
                    swim_common::form::structural::read::recognizer::OrdinalFieldKey::FirstItem => core::option::Option::Some(#offset),
                    _ => core::option::Option::None,
                }
            }
        })
    }
}

struct SelectFeedFn<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
}

impl<'a, 'b> SelectFeedFn<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
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
    model: &'a SegregatedFields<'a, 'a>,
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
        .chain(header.into_iter())
        .chain(attributes.iter().copied().map(FieldGroup::Attribute))
        .chain(body_fields.into_iter())
}

impl<'a, 'b> ToTokens for SelectFeedFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectFeedFn { fields } = self;
        let SegregatedStructModel { fields, .. } = fields;

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
                #case_index => swim_common::form::structural::read::recognizer::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            match index {
                #(#cases)*
                _ => core::option::Option::Some(core::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState)),
            }
        })
    }
}

struct OnDoneFn<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    constructor: syn::Path,
}

impl<'a, 'b> OnDoneFn<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>, constructor: syn::Path) -> Self {
        OnDoneFn {
            fields: model,
            constructor,
        }
    }
}

impl<'a, 'b> ToTokens for OnDoneFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let OnDoneFn {
            fields,
            constructor,
        } = self;
        let SegregatedStructModel { inner, fields } = fields;

        let it = enumerate_fields(fields);

        let validators = it.clone().enumerate().map(|(i, grp)| {
            let idx = syn::Index::from(i);
            match grp {
                FieldGroup::Tag(fld) => {
                    let name = fld.resolve_name();
                    quote! {
                        if fields.#idx.is_none() {
                            missing.push(swim_common::model::text::Text::new(#name));
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
                                    fields.#idx = <#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::on_absent();
                                    if fields.#idx.is_none() {
                                        missing.push(swim_common::model::text::Text::new(#name));
                                    }
                                }
                            }
                        }
                        ow => {
                            ow.iter().chain(header_fields.iter()).enumerate().fold(TokenStream::new(), |mut out, (j, fld)| {
                                let inner_idx = syn::Index::from(j);
                                let name = fld.resolve_name();

                                out.append_all(quote! {
                                if fields.#idx.as_ref().and_then(|f| f.#inner_idx.as_ref()).is_none() {
                                    missing.push(swim_common::model::text::Text::new(#name));
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
                            fields.#idx = <#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::on_absent();
                            if fields.#idx.is_none() {
                                missing.push(swim_common::model::text::Text::new(#name));
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
                let name = &fld.name;
                quote!(core::option::Option::Some(#name))
            }
            FieldGroup::Header {
                tag_body,
                header_fields,
            } => match tag_body {
                Some(fld) if header_fields.is_empty() => {
                    let name = &fld.name;
                    quote!(core::option::Option::Some(#name))
                }
                ow => {
                    let header_fields = ow.iter().chain(header_fields.iter()).map(|fld| {
                        let name = &fld.name;
                        quote!(core::option::Option::Some(#name))
                    });
                    quote!(core::option::Option::Some((#(#header_fields,)*)))
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
                    let name = &fld.model.name;
                    if fld.directive == FieldKind::Skip {
                        quote!(#name: core::default::Default::default())
                    } else {
                        quote!(#name)
                    }
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
                    .map(|fld| (fld.model.ordinal, &fld.model.name))
                    .collect::<HashMap<_, _>>();
                let con_params = (0..num_fields).map(|i| {
                    if let Some(name) = name_map.get(&i) {
                        quote!(#name)
                    } else {
                        quote!(core::default::Default::default())
                    }
                });
                quote! {
                    #constructor(#(#con_params,)*)
                }
            }
        };

        tokens.append_all(quote! {

            let (fields, _, _) = state;
            let mut missing = std::vec![];

            #(#validators)*

            if let (#(#field_dest,)*) = (#(#field_takes,)*) {
                core::result::Result::Ok(#make_result)
            } else {
                core::result::Result::Err(swim_common::form::structural::read::error::ReadError::MissingFields(missing))
            }
        })
    }
}

struct ResetFn {
    num_fields: usize,
}

impl ResetFn {
    pub fn new(num_fields: usize) -> Self {
        ResetFn { num_fields }
    }
}

impl ToTokens for ResetFn {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResetFn { num_fields } = self;

        let field_resets = (0..*num_fields).map(|i| {
            let idx = syn::Index::from(i);
            quote! {
                fields.#idx = core::option::Option::None;
                swim_common::form::structural::read::recognizer::Recognizer::reset(&mut recognizers.#idx);
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            #(#field_resets)*
        })
    }
}

struct ConstructFieldRecognizers<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    variant: Option<usize>,
}

impl<'a, 'b> ToTokens for ConstructFieldRecognizers<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ConstructFieldRecognizers { fields, variant } = self;
        let SegregatedStructModel { fields, .. } = fields;
        let initializers = enumerate_fields(fields)
            .map(|grp| {
                match grp {
                    FieldGroup::Tag(fld) => {
                        let ty = fld.field_ty;
                        quote!(<swim_common::form::structural::read::recognizer::TagRecognizer<#ty> as core::default::Default>::default())
                    }
                    FieldGroup::Attribute(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_attr_recognizer())
                    }
                    FieldGroup::Item(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer())
                    }
                    FieldGroup::DelegateBody(fld) => {
                        let ty = fld.field_ty;
                        quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_body_recognizer())
                    }
                    FieldGroup::Header { tag_body, header_fields } => {
                        match tag_body {
                            Some(fld) if header_fields.is_empty() => {
                                let ty = fld.field_ty;
                                quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_attr_recognizer())
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
                                    quote!(<#ty as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer())
                                });

                                let flds_constr= quote! {
                                    (core::default::Default::default(), (#(#recog_inits,)*), core::marker::PhantomData)
                                };

                                quote! {
                                    swim_common::form::structural::read::recognizer::header_recognizer(
                                        #has_body,
                                        || #flds_constr,
                                        #num_slots,
                                        swim_common::form::structural::read::recognizer::HeaderVTable::new(
                                            #select_index_name,
                                            #select_feed_name,
                                            swim_common::form::structural::read::recognizer::take_fields,
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

struct StructReadableImpl<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    gen_params: &'b TypeGenerics<'b>,
}

impl<'a, 'b> StructReadableImpl<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>, gen_params: &'b TypeGenerics<'b>) -> Self {
        StructReadableImpl {
            fields: model,
            gen_params,
        }
    }
}

fn compound_recognizer(
    model: &SegregatedStructModel<'_, '_>,
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
        let (r, v) = if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled {
            ("LabelledStructRecognizer", "LabelledVTable")
        } else {
            ("OrdinalStructRecognizer", "OrdinalVTable")
        };
        (
            syn::Ident::new(r, Span::call_site()),
            syn::Ident::new(v, Span::call_site()),
        )
    };
    let recog_ty = parse_quote!(swim_common::form::structural::read::recognizer::#recog_ty_name<#target, #builder>);
    let vtable_ty = parse_quote!(swim_common::form::structural::read::recognizer::#v_table_name<#target, #builder>);
    (recog_ty, vtable_ty)
}

impl<'a, 'b> ToTokens for StructReadableImpl<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let StructReadableImpl { fields, gen_params } = self;
        let name = fields.inner.name;

        let tag = if fields.fields.header.tag_name.is_some() {
            quote!(swim_common::form::structural::read::recognizer::TagSpec::Field)
        } else {
            let lit_name = fields.inner.resolve_name();
            quote!(swim_common::form::structural::read::recognizer::TagSpec::Fixed(#lit_name))
        };

        let make_fld_recog = ConstructFieldRecognizers {
            fields: *fields,
            variant: None,
        };
        let num_fields = fields.inner.fields_model.fields.len() as u32;

        let builder_name = builder_ident();

        let target = parse_quote!(#name #gen_params);
        let builder = parse_quote!(#builder_name #gen_params);
        let (recog_ty, vtable_ty) = compound_recognizer(*fields, &target, &builder);

        let select_index = select_index_name();
        let select_feed = select_feed_name();
        let on_done = on_done_name();
        let on_reset = on_reset_name();

        tokens.append_all(quote! {
            type Rec = #recog_ty;
            type AttrRec = swim_common::form::structural::read::recognizer::SimpleAttrBody<
                #recog_ty,
            >;
            type BodyRec = Self::Rec;

            #[allow(non_snake_case)]
            #[inline]
            fn make_recognizer() -> Self::Rec {
                <#recog_ty>::new(
                    #tag,
                    (core::default::Default::default(), #make_fld_recog, core::marker::PhantomData),
                    #num_fields,
                    <#vtable_ty>::new(
                        #select_index,
                        #select_feed,
                        #on_done,
                        #on_reset,
                    )
                )
            }

            #[inline]
            fn make_attr_recognizer() -> Self::AttrRec {
                swim_common::form::structural::read::recognizer::SimpleAttrBody::new(
                    <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
                )
            }

            #[inline]
            fn make_body_recognizer() -> Self::BodyRec {
                <Self as swim_common::form::structural::read::recognizer::RecognizerReadable>::make_recognizer()
            }
        })
    }
}

struct SelectVariantFn<'a, 'b> {
    model: &'b SegregatedEnumModel<'a, 'b>,
    gen_params: &'b TypeGenerics<'b>,
}

impl<'a, 'b> SelectVariantFn<'a, 'b> {
    fn new(model: &'b SegregatedEnumModel<'a, 'b>, gen_params: &'b TypeGenerics<'b>) -> Self {
        SelectVariantFn { model, gen_params }
    }
}

impl<'a, 'b> ToTokens for SelectVariantFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectVariantFn {
            model: SegregatedEnumModel { inner, variants },
            gen_params,
        } = self;

        let name = inner.name;
        let enum_ty = parse_quote!(#name #gen_params);

        let cases = variants.iter().enumerate().map(|(i, var)| {
            let lit_name = var.inner.resolve_name();
            let constructor = if var.inner.fields_model.type_kind == CompoundTypeKind::Unit {
                let var_name = var.inner.name;
                parse_quote!(swim_common::form::structural::read::recognizer::UnitStructRecognizer::variant(|| #name::#var_name))
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
                        (core::default::Default::default(), #make_fld_recog, core::marker::PhantomData),
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

            let ccons_constructor = make_ccons(i, constructor);
            quote! {
                #lit_name => core::option::Option::Some(#ccons_constructor)
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

struct EnumState<'a, 'b> {
    model: &'b SegregatedEnumModel<'a, 'b>,
    gen_params: &'b TypeGenerics<'b>,
}

impl<'a, 'b> EnumState<'a, 'b> {
    fn new(model: &'b SegregatedEnumModel<'a, 'b>, gen_params: &'b TypeGenerics<'b>) -> Self {
        EnumState { model, gen_params }
    }
}

impl<'a, 'b> ToTokens for EnumState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let EnumState {
            model: SegregatedEnumModel { inner, variants },
            gen_params,
        } = self;
        let base: syn::Type = parse_quote!(swim_common::form::structural::generic::coproduct::CNil);

        let name = inner.name;
        let enum_ty = parse_quote!(#name #gen_params);

        let ccons_type = variants
            .iter()
            .enumerate()
            .rev()
            .fold(base, |acc, (i, var)| {
                let ty = if var.inner.fields_model.type_kind == CompoundTypeKind::Unit {
                    parse_quote!(swim_common::form::structural::read::recognizer::UnitStructRecognizer<#enum_ty>)
                } else {
                    let builder_name = suffixed_builder_ident(i);
                    let builder = parse_quote!(#builder_name #gen_params);
                    let (ty, _) = compound_recognizer(var, &enum_ty, &builder);
                    ty
                };
                parse_quote!(swim_common::form::structural::generic::coproduct::CCons<#ty, #acc>)
            });

        let builder_name = builder_ident();

        tokens.append_all(quote! {
            type #builder_name #gen_params = #ccons_type;
        })
    }
}

fn make_ccons(n: usize, expr: syn::Expr) -> syn::Expr {
    let mut acc =
        parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Head(#expr));
    for _ in 0..n {
        acc = parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Tail(#acc));
    }
    acc
}

fn add_bounds(original: &Generics, generics: &mut Generics) {
    let bounds = original.type_params().map(|param| {
        let id = &param.ident;
        parse_quote!(#id: swim_common::form::structural::read::recognizer::RecognizerReadable)
    });
    let where_clause = generics.make_where_clause();
    for bound in bounds.into_iter() {
        where_clause.predicates.push(bound);
    }
}

struct HeaderSelectIndexFn<'a, 'b> {
    tag_body: Option<&'b FieldModel<'a>>,
    header_fields: &'b [&'b FieldModel<'a>],
    variant: Option<usize>,
}

impl<'a, 'b> HeaderSelectIndexFn<'a, 'b> {
    fn new(
        tag_body: Option<&'b FieldModel<'a>>,
        header_fields: &'b [&'b FieldModel<'a>],
        variant: Option<usize>,
    ) -> Self {
        HeaderSelectIndexFn {
            tag_body,
            header_fields,
            variant,
        }
    }
}

struct HeaderFeedFn<'a, 'b> {
    tag_body: Option<&'b FieldModel<'a>>,
    header_fields: &'b [&'b FieldModel<'a>],
}

impl<'a, 'b> HeaderFeedFn<'a, 'b> {
    fn new(tag_body: Option<&'b FieldModel<'a>>, header_fields: &'b [&'b FieldModel<'a>]) -> Self {
        HeaderFeedFn {
            tag_body,
            header_fields,
        }
    }
}

impl<'a, 'b> ToTokens for HeaderSelectIndexFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderSelectIndexFn {
            tag_body,
            header_fields,
            variant,
        } = self;

        let mut offset: u32 = 0;

        let body_case = if tag_body.is_some() {
            offset += 1;
            Some(quote! {
                swim_common::form::structural::read::recognizer::HeaderFieldKey::HeaderBody => core::option::Option::Some(0),
            })
        } else {
            None
        };

        let header_slot_cases = header_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::recognizer::HeaderFieldKey::HeaderSlot(#name) => core::option::Option::Some(#n),
            })
        });

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(HEADER_SELECT_INDEX_NAME, *var_ord)
        } else {
            headerselect_index_name()
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(key: swim_common::form::structural::read::recognizer::HeaderFieldKey<'_>) -> core::option::Option<u32> {
                match key {
                    #body_case
                    #(#header_slot_cases)*
                    _ => core::option::Option::None,
                }
            }
        })
    }
}

impl<'a, 'b> ToTokens for HeaderFeedFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderFeedFn {
            tag_body,
            header_fields,
        } = self;

        let it = tag_body.iter().chain(header_fields.iter());

        let cases = it.enumerate().map(|(i, fld)| {
            let name = fld.resolve_name();

            let idx = syn::Index::from(i);
            let case_index = i as u32;
            quote! {
                #case_index => swim_common::form::structural::read::recognizer::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        tokens.append_all(quote! {
            let (fields, recognizers, _) = state;
            match index {
                #(#cases)*
                _ => core::option::Option::Some(core::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState)),
            }
        })
    }
}

struct HeaderRecognizerFns<'a, 'b> {
    target: &'b syn::Type,
    tag_body: Option<&'b FieldModel<'a>>,
    header_fields: &'b [&'b FieldModel<'a>],
    variant: Option<usize>,
    generics: &'a Generics,
}

impl<'a, 'b> HeaderRecognizerFns<'a, 'b> {
    fn new(
        target: &'b syn::Type,
        tag_body: Option<&'b FieldModel<'a>>,
        header_fields: &'b [&'b FieldModel<'a>],
        generics: &'a Generics,
    ) -> Self {
        HeaderRecognizerFns {
            target,
            tag_body,
            header_fields,
            variant: None,
            generics,
        }
    }

    fn variant(
        target: &'b syn::Type,
        tag_body: Option<&'b FieldModel<'a>>,
        header_fields: &'b [&'b FieldModel<'a>],
        generics: &'a Generics,
        variant: usize,
    ) -> Self {
        HeaderRecognizerFns {
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

impl<'a, 'b> ToTokens for HeaderRecognizerFns<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let HeaderRecognizerFns {
            target,
            tag_body,
            header_fields,
            variant,
            generics,
        } = self;

        let (builder_name, select_feed_name, on_reset_name) = header_identifiers(*variant);

        let header_builder_type = HeaderRecognizerState::new(*target, *tag_body, *header_fields);
        let select_index = HeaderSelectIndexFn::new(*tag_body, *header_fields, *variant);
        let select_feed = HeaderFeedFn::new(*tag_body, *header_fields);
        let num_fields = if tag_body.is_some() {
            header_fields.len() + 1
        } else {
            header_fields.len()
        };
        let on_reset = ResetFn::new(num_fields);

        let (impl_gen, type_gen, where_clause) = generics.split_for_impl();

        tokens.append_all(quote! {

            type #builder_name #type_gen = #header_builder_type;
            #select_index

            #[automatically_derived]
            fn #select_feed_name #impl_gen(state: &mut #builder_name #type_gen, index: u32, event: swim_common::form::structural::read::event::ReadEvent<'_>)
                -> core::option::Option<core::result::Result<(), swim_common::form::structural::read::error::ReadError>>
            #where_clause
            {
                #select_feed
            }

            #[automatically_derived]
            fn #on_reset_name #impl_gen(state: &mut #builder_name #type_gen)
            #where_clause
            {
                 #on_reset
            }

        });
    }
}
