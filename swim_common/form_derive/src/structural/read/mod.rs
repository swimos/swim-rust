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

use crate::quote::TokenStreamExt;
use crate::structural::model::enumeration::SegregatedEnumModel;
use crate::structural::model::field::{BodyFields, FieldModel, HeaderFields, SegregatedFields};
use crate::structural::model::record::SegregatedStructModel;
use either::Either;
use macro_helpers::CompoundTypeKind;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;

pub struct DeriveStructuralReadable<S>(pub S);

impl<'a, 'b> ToTokens for DeriveStructuralReadable<SegregatedStructModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model) = self;
        if model.inner.fields_model.type_kind == CompoundTypeKind::Unit {
            let name = model.inner.name;
            let lit_name = model.inner.resolve_name();

            tokens.append_all(quote! {
                impl swim_common::form::structural::read::improved::RecognizerReadable for #name {
                    type Rec = swim_common::form::structural::read::improved::UnitStructRecognizer<#name>;
                    type AttrRec = swim_common::form::structural::read::improved::SimpleAttrBody<
                        swim_common::form::structural::read::improved::UnitStructRecognizer<#name>
                    >;

                    fn make_recognizer() -> Self::Rec {
                        swim_common::form::structural::read::improved::UnitStructRecognizer::new(
                            #lit_name,
                            || #name
                        )
                    }

                    fn make_attr_recognizer() -> Self::AttrRec {
                        swim_common::form::structural::read::improved::SimpleAttrBody::new(
                            <Self as swim_common::form::structural::read::improved::RecognizerReadable>::make_recognizer()
                        )
                    }

                }
            })
        } else {
            let builder_type = RecognizerState::new(model);

            let select_feed = SelectFeedFn::new(model);
            let on_done = OnDoneFn::new(model);
            let select_index = match &model.fields.body {
                BodyFields::StdBody(body_fields)
                    if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled =>
                {
                    SelectIndexFnLabelled::new(model, &body_fields).into_token_stream()
                }
                _ => SelectIndexFnOrdinal::new(model).into_token_stream(),
            };

            let read_impl = StructReadableImpl::new(model);

            let on_reset = ResetFn::new(model.inner.fields_model.fields.len());

            tokens.append_all(quote! {
                const _: () = {

                    #builder_type
                    #select_index
                    #select_feed
                    #on_done
                    #on_reset

                    #read_impl

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

fn suffixed_builder_ident(suffix: usize) -> syn::Ident {
    format_ident!("{}{}", BUILDER_NAME, suffix)
}

impl<'a, 'b> ToTokens for DeriveStructuralReadable<SegregatedEnumModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model) = self;
        let SegregatedEnumModel { inner, variants } = model;
        let name = inner.name;

        let variant_functions = variants.iter().enumerate().map(|(i, model)| {
            let builder_type = RecognizerState::variant(model, i);

            let select_index = match &model.fields.body {
                BodyFields::StdBody(body_fields)
                    if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled =>
                {
                    SelectIndexFnLabelled::variant(model, &body_fields, i).into_token_stream()
                }
                _ => SelectIndexFnOrdinal::variant(model, i).into_token_stream(),
            };

            let select_feed = SelectFeedFn::variant(model, i);
            let on_done = OnDoneFn::variant(model, name, i);

            let on_reset = ResetFn::variant(model.inner.fields_model.fields.len(), i);

            quote! {
                #builder_type
                #select_index
                #select_feed
                #on_done
                #on_reset
            }
        });

        let state = EnumState::new(model);
        let select_var = SelectVariantFn::new(model);
        let read_impl = EnumReadableImpl::new(model);

        tokens.append_all(quote! {
            const _: () = {
                 #(#variant_functions)*

                #state
                #select_var
                #read_impl
            }
        });
    }
}

struct RecognizerState<'a, 'b> {
    model: &'b SegregatedStructModel<'a, 'b>,
    variant: Option<usize>,
}

impl<'a, 'b> RecognizerState<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        RecognizerState {
            model,
            variant: None,
        }
    }

    fn variant(model: &'b SegregatedStructModel<'a, 'b>, variant_ordinal: usize) -> Self {
        RecognizerState {
            model,
            variant: Some(variant_ordinal),
        }
    }
}

impl<'a, 'b> ToTokens for RecognizerState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let RecognizerState {
            model: SegregatedStructModel { fields, .. },
            variant,
        } = self;
        let it = enumerate_fields_discriminated(fields);

        let builder_types = it.clone().map(|(fld, _)| {
            let ty = fld.field_ty;
            quote!(std::option::Option<#ty>)
        });

        let recognizer_types = it.map(|(fld, is_attr)| {
            let ty = fld.field_ty;
            if is_attr {
                quote!(<#ty as swim_common::form::structural::read::improved::RecognizerReadable>::AttrRec)
            } else {
                quote!(<#ty as swim_common::form::structural::read::improved::RecognizerReadable>::Rec)
            }
        });

        let builder_name = if let Some(var_ord) = variant {
            suffixed_builder_ident(*var_ord)
        } else {
            builder_ident()
        };

        tokens.append_all(quote! {
            type #builder_name = ((#(#builder_types,)*), (#(#recognizer_types,)*));
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

fn select_index_name() -> syn::Ident {
    syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
}

const SELECT_FEED_NAME: &str = "select_feed";

fn select_feed_name() -> syn::Ident {
    syn::Ident::new(SELECT_FEED_NAME, Span::call_site())
}

const ON_DONE_NAME: &str = "on_done";

fn on_done_name() -> syn::Ident {
    syn::Ident::new(ON_DONE_NAME, Span::call_site())
}
const ON_RESET_NAME: &str = "on_reset";

fn on_reset_name() -> syn::Ident {
    syn::Ident::new(ON_RESET_NAME, Span::call_site())
}

const SELECT_VAR_NAME: &str = "select_variant";

fn select_var_name() -> syn::Ident {
    syn::Ident::new(SELECT_VAR_NAME, Span::call_site())
}

const BUILDER_NAME: &str = "Builder";

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
                swim_common::form::structural::read::improved::LabelledFieldKey::Tag => std::option::Option::Some(0),
            })
        } else {
            None
        };

        let body_case = if tag_body.is_some() {
            let case = Some(quote! {
                swim_common::form::structural::read::improved::LabelledFieldKey::HeaderBody => std::option::Option::Some(#offset),
            });
            offset += 1;
            case
        } else {
            None
        };

        let header_cases = header_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::improved::LabelledFieldKey::HeaderSlot(#name) => std::option::Option::Some(#n),
            })
        });

        offset += header_fields.len() as u32;

        let attr_cases = attributes.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::improved::LabelledFieldKey::Attr(#name) => std::option::Option::Some(#n),
            })
        });

        offset += attributes.len() as u32;

        let body_cases = body_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::improved::LabelledFieldKey::Item(#name) => std::option::Option::Some(#n),
            })
        });

        let fn_name = if let Some(var_ord) = variant {
            suffix_ident(SELECT_INDEX_NAME, *var_ord)
        } else {
            syn::Ident::new(SELECT_INDEX_NAME, Span::call_site())
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(key: swim_common::form::structural::read::improved::LabelledFieldKey<'_>) -> std::option::Option<u32> {
                match key {
                    #name_case
                    #body_case
                    #(#header_cases)*
                    #(#attr_cases)*
                    #(#body_cases)*
                    _ => std::option::Option::None,
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
                swim_common::form::structural::read::improved::OrdinalFieldKey::Tag => std::option::Option::Some(0),
            })
        } else {
            None
        };

        let body_case = if tag_body.is_some() {
            let case = Some(quote! {
                swim_common::form::structural::read::improved::OrdinalFieldKey::HeaderBody => std::option::Option::Some(#offset),
            });
            offset += 1;
            case
        } else {
            None
        };

        let header_cases = header_fields.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::improved::OrdinalFieldKey::HeaderSlot(#name) => std::option::Option::Some(#n),
            })
        });

        offset += header_fields.len() as u32;

        let attr_cases = attributes.iter().scan(offset, |off, fld| {
            let n = *off;
            *off += 1;
            let name = fld.resolve_name();
            Some(quote! {
                swim_common::form::structural::read::improved::OrdinalFieldKey::Attr(#name) => std::option::Option::Some(#n),
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
            fn #fn_name(key: swim_common::form::structural::read::improved::OrdinalFieldKey<'_>) -> std::option::Option<u32> {
                match key {
                    #name_case
                    #body_case
                    #(#header_cases)*
                    #(#attr_cases)*
                    swim_common::form::structural::read::improved::OrdinalFieldKey::FirstItem => std::option::Option::Some(#offset),
                    _ => std::option::Option::None,
                }
            }
        })
    }
}

struct SelectFeedFn<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    variant: Option<usize>,
}

impl<'a, 'b> SelectFeedFn<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        SelectFeedFn {
            fields: model,
            variant: None,
        }
    }

    fn variant(model: &'b SegregatedStructModel<'a, 'b>, variant_ordinal: usize) -> Self {
        SelectFeedFn {
            fields: model,
            variant: Some(variant_ordinal),
        }
    }
}

fn enumerate_fields<'a>(
    model: &'a SegregatedFields<'a, 'a>,
) -> impl Iterator<Item = &'a FieldModel<'a>> + Clone + 'a {
    let SegregatedFields { header, body } = model;
    let HeaderFields {
        tag_name,
        tag_body,
        header_fields,
        attributes,
    } = header;

    let body_fields = match body {
        BodyFields::StdBody(vec) => Either::Left(vec.iter()),
        BodyFields::ReplacedBody(fld) => Either::Right(std::iter::once(fld)),
    };

    tag_name
        .iter()
        .chain(tag_body.iter())
        .chain(header_fields.iter())
        .chain(attributes.iter())
        .chain(body_fields.into_iter())
        .copied()
}

fn enumerate_fields_discriminated<'a>(
    model: &'a SegregatedFields<'a, 'a>,
) -> impl Iterator<Item = (&'a FieldModel<'a>, bool)> + Clone + 'a {
    let SegregatedFields { header, body } = model;
    let HeaderFields {
        tag_name,
        tag_body,
        header_fields,
        attributes,
    } = header;

    let body_fields = match body {
        BodyFields::StdBody(vec) => Either::Left(vec.iter()),
        BodyFields::ReplacedBody(fld) => Either::Right(std::iter::once(fld)),
    };

    tag_name
        .iter()
        .map(|f| (*f, false))
        .chain(tag_body.iter().map(|f| (*f, false)))
        .chain(header_fields.iter().map(|f| (*f, false)))
        .chain(attributes.iter().map(|f| (*f, true)))
        .chain(body_fields.into_iter().map(|f| (*f, false)))
}

impl<'a, 'b> ToTokens for SelectFeedFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectFeedFn { fields, variant } = self;
        let SegregatedStructModel { fields, .. } = fields;

        let it = enumerate_fields(fields);

        let cases = it.enumerate().map(|(i, fld)| {
            let name = fld.resolve_name();
            let idx = syn::Index::from(i);
            let case_index = i as u32;
            quote! {
                #case_index => swim_common::form::structural::read::improved::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        let (fn_name, builder_name) = if let Some(var_ord) = variant {
            (
                suffix_ident(SELECT_FEED_NAME, *var_ord),
                suffixed_builder_ident(*var_ord),
            )
        } else {
            (
                syn::Ident::new(SELECT_FEED_NAME, Span::call_site()),
                builder_ident(),
            )
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(state: &mut #builder_name, index: u32, event: swim_common::form::structural::read::parser::ParseEvent<'_>)
                -> std::option::Option<std::result::Result<(), swim_common::form::structural::read::error::ReadError>> {
                let (fields, recognizers) = state;
                match index {
                    #(#cases)*
                    _ => std::option::Option::Some(std::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState)),
                }
            }
        })
    }
}

struct OnDoneFn<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    outer_enum: Option<(&'a syn::Ident, usize)>,
}

impl<'a, 'b> OnDoneFn<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        OnDoneFn {
            fields: model,
            outer_enum: None,
        }
    }

    fn variant(
        model: &'b SegregatedStructModel<'a, 'b>,
        enum_name: &'a syn::Ident,
        variant_ordinal: usize,
    ) -> Self {
        OnDoneFn {
            fields: model,
            outer_enum: Some((enum_name, variant_ordinal)),
        }
    }
}

impl<'a, 'b> ToTokens for OnDoneFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let OnDoneFn { fields, outer_enum } = self;
        let SegregatedStructModel { inner, fields } = fields;

        let it = enumerate_fields(fields);

        let names = it.clone().map(|fld| &fld.name);

        let validators = it.clone().enumerate().map(|(i, fld)| {
            let idx = syn::Index::from(i);
            let name = fld.resolve_name();
            let ty = fld.field_ty;
            quote! {
                if fields.#idx.is_none() {
                    fields.#idx = <#ty as swim_common::form::structural::read::improved::RecognizerReadable>::on_absent();
                    if fields.#idx.is_none() {
                        missing.push(swim_common::model::text::Text::new(#name));
                    }
                }
            }
        });

        let field_dest = names
            .clone()
            .map(|name| quote!(std::option::Option::Some(#name)));

        let num_fields = inner.fields_model.fields.len();
        let field_takes = (0..num_fields).map(|i| {
            let idx = syn::Index::from(i);
            quote!(fields.#idx.take())
        });

        let name = inner.name;

        let (path, fn_name, builder_name) = if let Some((enum_name, var_ord)) = outer_enum {
            let enum_path: syn::Path = parse_quote!(#enum_name::#name);
            (
                enum_path,
                suffix_ident(ON_DONE_NAME, *var_ord),
                suffixed_builder_ident(*var_ord),
            )
        } else {
            (
                name.clone().into(),
                syn::Ident::new(ON_DONE_NAME, Span::call_site()),
                builder_ident(),
            )
        };

        let constructor = match inner.fields_model.type_kind {
            CompoundTypeKind::Labelled => {
                quote! {
                    #path {
                        #(#names,)*
                    }
                }
            }
            CompoundTypeKind::Unit => {
                quote!(#path)
            }
            _ => {
                quote! {
                    #path(#(#names,)*)
                }
            }
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(state: &mut #builder_name) -> std::result::Result<#name, swim_common::form::structural::read::error::ReadError> {
                let (fields, _ ) = state;
                let mut missing = std::vec![];

                #(#validators)*

                if let (#(#field_dest,)*) = (#(#field_takes,)*) {
                    std::result::Result::Ok(#constructor)
                } else {
                    std::result::Result::Err(swim_common::form::structural::read::error::ReadError::MissingFields(missing))
                }
            }
        })
    }
}

struct ResetFn {
    num_fields: usize,
    variant: Option<usize>,
}

impl ResetFn {
    pub fn new(num_fields: usize) -> Self {
        ResetFn {
            num_fields,
            variant: None,
        }
    }

    pub fn variant(num_fields: usize, variant_ordinal: usize) -> Self {
        ResetFn {
            num_fields,
            variant: Some(variant_ordinal),
        }
    }
}

impl ToTokens for ResetFn {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResetFn {
            num_fields,
            variant,
        } = self;
        let field_resets = (0..*num_fields).map(|i| {
            let idx = syn::Index::from(i);
            quote! {
                fields.#idx = std::option::Option::None;
                swim_common::form::structural::read::improved::Recognizer::reset(&mut recognizers.#idx);
            }
        });

        let (fn_name, builder_name) = if let Some(var_ord) = variant {
            (
                suffix_ident(ON_RESET_NAME, *var_ord),
                suffixed_builder_ident(*var_ord),
            )
        } else {
            (
                syn::Ident::new(ON_RESET_NAME, Span::call_site()),
                builder_ident(),
            )
        };

        tokens.append_all(quote! {
            #[automatically_derived]
            fn #fn_name(state: &mut #builder_name) {
                let (fields, recognizers) = state;
                #(#field_resets)*
            }
        })
    }
}

struct ConstructFieldRecognizers<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
}

impl<'a, 'b> ToTokens for ConstructFieldRecognizers<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ConstructFieldRecognizers { fields } = self;
        let SegregatedStructModel { fields, .. } = fields;
        let initializers = enumerate_fields_discriminated(fields)
            .map(|(fld, is_attr)| {
                let ty = fld.field_ty;
                if is_attr {
                    quote!(<#ty as swim_common::form::structural::read::improved::RecognizerReadable>::make_attr_recognizer())
                } else {
                    quote!(<#ty as swim_common::form::structural::read::improved::RecognizerReadable>::make_recognizer())
                }
            });
        tokens.append_all(quote! {
            (#(#initializers,)*)
        })
    }
}

struct StructReadableImpl<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
}

impl<'a, 'b> StructReadableImpl<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        StructReadableImpl { fields: model }
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
    let recog_ty = parse_quote!(swim_common::form::structural::read::improved::#recog_ty_name<#target, #builder>);
    let vtable_ty = parse_quote!(swim_common::form::structural::read::improved::#v_table_name<#target, #builder>);
    (recog_ty, vtable_ty)
}

fn is_simple_param(body_fields: &BodyFields) -> Option<TokenStream> {
    if let BodyFields::ReplacedBody(fld) = body_fields {
        let ty = fld.field_ty;
        let extra = quote! {
            <#ty as swim_common::form::structural::read::improved::RecognizerReadable>::is_simple()
        };
        Some(extra)
    } else {
        None
    }
}

impl<'a, 'b> ToTokens for StructReadableImpl<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let StructReadableImpl { fields } = self;
        let name = fields.inner.name;

        let tag = if fields.fields.header.tag_name.is_some() {
            quote!(swim_common::form::structural::read::improved::TagSpec::Field)
        } else {
            let lit_name = fields.inner.resolve_name();
            quote!(swim_common::form::structural::read::improved::TagSpec::Fixed(#lit_name))
        };

        let has_header_body = fields.fields.header.tag_body.is_some();
        let make_fld_recog = ConstructFieldRecognizers { fields: *fields };
        let num_fields = fields.inner.fields_model.fields.len() as u32;

        let extra_params = is_simple_param(&fields.fields.body);

        let builder_name = builder_ident();

        let target = parse_quote!(#name);
        let builder = parse_quote!(#builder_name);
        let (recog_ty, vtable_ty) = compound_recognizer(*fields, &target, &builder);

        let select_index = select_index_name();
        let select_feed = select_feed_name();
        let on_done = on_done_name();
        let on_reset = on_reset_name();

        tokens.append_all(quote! {
            #[automatically_derived]
            impl swim_common::form::structural::read::improved::RecognizerReadable for #name {
                type Rec = #recog_ty;
                type AttrRec = swim_common::form::structural::read::improved::SimpleAttrBody<
                    #recog_ty,
                >;

                #[allow(non_snake_case)]
                #[inline]
                fn make_recognizer() -> Self::Rec {
                    <#recog_ty>::new(
                        #tag,
                        #has_header_body,
                        (std::default::Default::default(), #make_fld_recog),
                        #num_fields,
                        <#vtable_ty>::new(
                            #select_index,
                            #select_feed,
                            #on_done,
                            #on_reset,
                        ),
                        #extra_params
                    )
                }

                #[allow(non_snake_case)]
                #[inline]
                fn make_attr_recognizer() -> Self::AttrRec {
                    swim_common::form::structural::read::improved::SimpleAttrBody::new(
                        <Self as swim_common::form::structural::read::improved::RecognizerReadable>::make_recognizer()
                    )
                }
            }
        })
    }
}

struct SelectVariantFn<'a, 'b> {
    model: &'b SegregatedEnumModel<'a, 'b>,
}

impl<'a, 'b> SelectVariantFn<'a, 'b> {
    fn new(model: &'b SegregatedEnumModel<'a, 'b>) -> Self {
        SelectVariantFn { model }
    }
}

impl<'a, 'b> ToTokens for SelectVariantFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectVariantFn {
            model: SegregatedEnumModel { inner, variants },
        } = self;

        let name = inner.name;
        let enum_ty = parse_quote!(#name);

        let cases = variants.iter().enumerate().map(|(i, var)| {
            let builder_name = suffixed_builder_ident(i);
            let builder_ty = parse_quote!(#builder_name);
            let (recognizer, vtable) = compound_recognizer(var, &enum_ty, &builder_ty);
            let make_fld_recog = ConstructFieldRecognizers { fields: var };
            let extra_params = is_simple_param(&var.fields.body);

            let has_header_body = var.fields.header.tag_body.is_some();
            let num_fields = var.inner.fields_model.fields.len();
            let select_index = suffix_ident(SELECT_INDEX_NAME, i);
            let select_feed = suffix_ident(SELECT_FEED_NAME, i);
            let on_done = suffix_ident(ON_DONE_NAME, i);
            let on_reset = suffix_ident(ON_RESET_NAME, i);

            let lit_name = var.inner.resolve_name();

            let constructor = parse_quote! {
                <#recognizer>::variant(
                    #has_header_body,
                    (std::default::Default::default(), #make_fld_recog),
                    #num_fields,
                    <#vtable>::new(
                        #select_index,
                        #select_feed,
                        #on_done,
                        #on_reset,
                    ),
                    #extra_params
                )
            };
            let ccons_constructor = make_ccons(i, constructor);
            quote! {
                #lit_name => std::option::Option::Some(#ccons_constructor)
            }
        });

        let builder_name = builder_ident();

        tokens.append_all(quote! {
            fn #SELECT_VAR_NAME(name: &str) -> std::option::Option<#builder_name> {
                match name {
                    #(#cases,)*
                    _ => None,
                }
            }
        });
    }
}

struct EnumReadableImpl<'a, 'b> {
    model: &'b SegregatedEnumModel<'a, 'b>,
}

impl<'a, 'b> EnumReadableImpl<'a, 'b> {
    fn new(model: &'b SegregatedEnumModel<'a, 'b>) -> Self {
        EnumReadableImpl { model }
    }
}

impl<'a, 'b> ToTokens for EnumReadableImpl<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let EnumReadableImpl { model } = self;

        let name = model.inner.name;
        let builder_name = builder_ident();

        let recog_ty = quote!(swim_common::form::structural::read::improved::TaggedEnumRecognizer<#builder_name>);

        let select_var = select_var_name();

        tokens.append_all(quote! {

            impl swim_common::form::structural::read::improved::RecognizerReadable for #name {

                type Rec = #recog_ty;
                type AttrRec = swim_common::form::structural::read::improved::SimpleAttrBody<
                    #recog_ty,
                >;

                fn make_recognizer() -> Self::Rec {
                    <#recog_ty>::new(
                        #select_var
                    )
                }

                fn make_attr_recognizer() -> Self::AttrRec {
                    swim_common::form::structural::read::improved::SimpleAttrBody::new(
                        <Self as swim_common::form::structural::read::improved::RecognizerReadable>::make_recognizer()
                    )
                }
            }

        })
    }
}

struct EnumState<'a, 'b> {
    model: &'b SegregatedEnumModel<'a, 'b>,
}

impl<'a, 'b> EnumState<'a, 'b> {
    fn new(model: &'b SegregatedEnumModel<'a, 'b>) -> Self {
        EnumState { model }
    }
}

impl<'a, 'b> ToTokens for EnumState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let EnumState {
            model: SegregatedEnumModel { inner, variants },
        } = self;
        let base: syn::Type = parse_quote!(swim_common::form::strucutral::generic::coproduct::CNil);

        let name = inner.name;
        let enum_ty = parse_quote!(#name);

        let ccons_type = variants
            .iter()
            .enumerate()
            .rev()
            .fold(base, |acc, (i, var)| {
                let builder_name = suffixed_builder_ident(i);
                let builder = parse_quote!(#builder_name);
                let (ty, _) = compound_recognizer(var, &enum_ty, &builder);
                parse_quote!(swim_common::form::strucutral::generic::coproduct::CCons<#ty, #acc>)
            });

        let builder_name = builder_ident();

        tokens.append_all(quote! {
            type #builder_name = #ccons_type;
        })
    }
}

fn make_ccons(n: usize, expr: syn::Expr) -> syn::Expr {
    let mut pattern =
        parse_quote!(swim_common::form::strucutral::generic::coproduct::CCons::Head(#expr));
    for _ in 0..n {
        pattern =
            parse_quote!(swim_common::form::strucutral::generic::coproduct::CCons::Tail(#expr));
    }
    pattern
}
