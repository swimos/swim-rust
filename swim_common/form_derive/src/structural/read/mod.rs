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
use crate::structural::model::field::{BodyFields, FieldModel, HeaderFields, SegregatedFields};
use crate::structural::model::record::SegregatedStructModel;
use either::Either;
use macro_helpers::CompoundTypeKind;
use proc_macro2::TokenStream;
use quote::ToTokens;

pub struct DeriveStructuralReadable<S>(S);

impl<'a, 'b> ToTokens for DeriveStructuralReadable<SegregatedStructModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model) = self;
        let builder_type = RecognizerState::new(model);

        let select_index = match &model.fields.body {
            BodyFields::StdBody(body_fields)
                if model.inner.fields_model.body_kind == CompoundTypeKind::Labelled =>
            {
                SelectIndexFnLabelled::new(model, &body_fields).into_token_stream()
            }
            _ => SelectIndexFnOrdinal::new(model).into_token_stream(),
        };

        let select_feed = SelectFeedFn::new(model);
        let on_done = OnDoneFn::new(model);
        let read_impl = ReadableImpl::new(model);

        let on_reset = ResetFn(model.inner.fields_model.fields.len());

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

struct RecognizerState<'a, 'b>(&'b SegregatedStructModel<'a, 'b>);

impl<'a, 'b> RecognizerState<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        RecognizerState(model)
    }
}

impl<'a, 'b> ToTokens for RecognizerState<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let RecognizerState(SegregatedStructModel { fields, .. }) = self;
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

        tokens.append_all(quote! {
            type Builder = ((#(#builder_types,)*), (#(#recognizer_types,)*));
        });
    }
}

struct SelectIndexFnLabelled<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
    body_fields: &'b Vec<&'b FieldModel<'a>>,
}

impl<'a, 'b> SelectIndexFnLabelled<'a, 'b> {
    fn new(
        model: &'b SegregatedStructModel<'a, 'b>,
        body_fields: &'b Vec<&'b FieldModel<'a>>,
    ) -> Self {
        SelectIndexFnLabelled {
            fields: model,
            body_fields,
        }
    }
}

impl<'a, 'b> ToTokens for SelectIndexFnLabelled<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnLabelled {
            fields,
            body_fields,
        } = self;
        let SegregatedStructModel { fields, .. } = fields;
        let SegregatedFields { header, .. } = fields;
        let HeaderFields {
            tag_body,
            header_fields,
            attributes,
            ..
        } = header;

        let mut offset: u32 = 0;

        let body_case = if tag_body.is_some() {
            offset += 1;
            Some(quote! {
                swim_common::form::structural::read::improved::LabelledFieldKey::HeaderBody => std::option::Option::Some(0),
            })
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

        tokens.append_all(quote! {
            fn select_index(key: swim_common::form::structural::read::improved::LabelledFieldKey<'_>) -> std::option::Option<u32> {
                match key {
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
}

impl<'a, 'b> SelectIndexFnOrdinal<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        SelectIndexFnOrdinal { fields: model }
    }
}

impl<'a, 'b> ToTokens for SelectIndexFnOrdinal<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectIndexFnOrdinal { fields } = self;
        let SegregatedStructModel { fields, .. } = fields;
        let SegregatedFields { header, .. } = fields;
        let HeaderFields {
            tag_body,
            header_fields,
            attributes,
            ..
        } = header;

        let mut offset: u32 = 0;

        let body_case = if tag_body.is_some() {
            offset += 1;
            Some(quote! {
                swim_common::form::structural::read::improved::OrdinalFieldKey::HeaderBody => std::option::Option::Some(0),
            })
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

        tokens.append_all(quote! {
            fn select_index(key: swim_common::form::structural::read::improved::LabelledFieldKey<'_>) -> std::option::Option<u32> {
                match key {
                    #body_case
                    #(#header_cases)*
                    #(#attr_cases)*
                    swim_common::form::structural::read::improved::OrdinalFieldKey::FirstItem => Some(#offset),
                    _ => std::option::Option::None,
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

fn enumerate_fields<'a>(
    model: &'a SegregatedFields<'a, 'a>,
) -> impl Iterator<Item = &'a FieldModel<'a>> + Clone + 'a {
    let SegregatedFields { header, body } = model;
    let HeaderFields {
        tag_body,
        header_fields,
        attributes,
        ..
    } = header;

    let body_fields = match body {
        BodyFields::StdBody(vec) => Either::Left(vec.iter()),
        BodyFields::ReplacedBody(fld) => Either::Right(std::iter::once(fld)),
    };

    tag_body
        .iter()
        .chain(header_fields.iter())
        .chain(attributes.iter())
        .chain(body_fields.into_iter())
        .map(|f| *f)
}

fn enumerate_fields_discriminated<'a>(
    model: &'a SegregatedFields<'a, 'a>,
) -> impl Iterator<Item = (&'a FieldModel<'a>, bool)> + Clone + 'a {
    let SegregatedFields { header, body } = model;
    let HeaderFields {
        tag_body,
        header_fields,
        attributes,
        ..
    } = header;

    let body_fields = match body {
        BodyFields::StdBody(vec) => Either::Left(vec.iter()),
        BodyFields::ReplacedBody(fld) => Either::Right(std::iter::once(fld)),
    };

    tag_body
        .iter()
        .map(|f| (*f, false))
        .chain(header_fields.iter().map(|f| (*f, false)))
        .chain(attributes.iter().map(|f| (*f, true)))
        .chain(body_fields.into_iter().map(|f| (*f, false)))
}

impl<'a, 'b> ToTokens for SelectFeedFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SelectFeedFn { fields } = self;
        let SegregatedStructModel { fields, .. } = fields;

        let it = enumerate_fields(fields);

        let cases = it.zip(0..).map(|(fld, i)| {
            let name = fld.resolve_name();
            let idx = syn::Index::from(i);
            let case_index = i as u32;
            quote! {
                #case_index => swim_common::form::structural::read::improved::feed_field(#name, &mut fields.#idx, &mut recognizers.#idx, event),
            }
        });

        tokens.append_all(quote! {
            fn select_feed(state: &mut Builder, index: u32, event: swim_common::form::structural::read::parser::ParseEvent<'_>)
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
}

impl<'a, 'b> OnDoneFn<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        OnDoneFn { fields: model }
    }
}

impl<'a, 'b> ToTokens for OnDoneFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let OnDoneFn { fields } = self;
        let SegregatedStructModel { inner, fields } = fields;

        let it = enumerate_fields(fields);

        let names = it.clone().map(|fld| &fld.name);

        let validators = it.clone().zip(0..).map(|(fld, i)| {
            let idx = syn::Index::from(i);
            let name = fld.resolve_name();
            let ty = fld.field_ty;
            quote! {
                if fields.#idx.is_none() {
                    fields.#idx = <#ty as swim_common::form::structural::read::improved::RecognizerReadable>::on_absent();
                    if fields.#idx.is_none() {
                        missing.insert(#name);
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

        let constructor = if inner.fields_model.type_kind == CompoundTypeKind::Labelled {
            quote! {
                #name {
                    #(#names,)*
                }
            }
        } else {
            quote! {
                #name(#(#names,)*)
            }
        };

        tokens.append_all(quote! {
            fn on_done(state: &mut Builder) -> std::result::Result<#name, swim_common::form::structural::read::error::ReadError> {
                let (fields, _ ) = state;
                let mut missing = std::vec![];

                #(#validators)*

                if let (#(#field_dest),*) = (#(#field_takes),*) {
                    std::result::Result::Ok(#constructor)
                } else {
                    std::result::Result::Err(swim_common::form::structural::read::error::ReadError::MissingFields(missing))
                }
            }
        })
    }
}

struct ResetFn(usize);

impl ToTokens for ResetFn {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ResetFn(num_fields) = self;
        let field_resets = (0..*num_fields).map(|i| {
            let idx = syn::Index::from(i);
            quote! {
                fields.#idx = std::option::Option::None;
                recognizers.#idx.reset();
            }
        });

        tokens.append_all(quote! {
            fn on_reset(state: &mut Builder) {
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
            (#(#initializers),*)
        })
    }
}

struct ReadableImpl<'a, 'b> {
    fields: &'b SegregatedStructModel<'a, 'b>,
}

impl<'a, 'b> ReadableImpl<'a, 'b> {
    fn new(model: &'b SegregatedStructModel<'a, 'b>) -> Self {
        ReadableImpl { fields: model }
    }
}

impl<'a, 'b> ToTokens for ReadableImpl<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ReadableImpl { fields } = self;
        let name = fields.inner.name;
        let tag = fields.inner.resolve_name();
        let has_header_body = fields.fields.header.tag_body.is_some();

        let make_fld_recog = ConstructFieldRecognizers { fields: *fields };
        let num_fields = fields.inner.fields_model.fields.len() as u32;

        let (recog, extra_params) = if let BodyFields::ReplacedBody(fld) = &fields.fields.body {
            let recog: syn::Path = parse_quote!(
                swim_common::form::structural::read::improved::DelegateStructRecognizer
            );
            let ty = fld.field_ty;
            let extra = quote! {
                <#ty as swim_common::form::structural::read::improved::RecognizerReadable>::is_simple()
            };
            (recog, Some(extra))
        } else {
            let recog: syn::Path = if fields.inner.fields_model.body_kind
                == CompoundTypeKind::Labelled
            {
                parse_quote!(
                    swim_common::form::structural::read::improved::LabelledStructRecognizer
                )
            } else {
                parse_quote!(swim_common::form::structural::read::improved::OrdinalStructRecognizer)
            };
            (recog, None)
        };

        tokens.append_all(quote! {

            impl swim_common::form::structural::read::improved::RecognizerReadable for #name {

                type Rec = #recog<#name, Builder>;
                type AttrRec = swim_common::form::structural::read::improved::SimpleAttrBody<
                    #name,
                    #recog<#name, Builder>,
                >;

                fn make_recognizer() -> Self::Rec {
                    #recog::new(
                        #tag,
                        #has_header_body,
                        (Default::default(), #make_fld_recog),
                        #num_fields,
                        select_index,
                        select_feed,
                        on_done,
                        on_reset,
                        #extra_params
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
