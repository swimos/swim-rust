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

use crate::structural::model::field::{FieldModel, SegregatedFields, BodyFields, HeaderFields};
use crate::structural::model::record::{SegregatedStructModel, StructModel};
use macro_helpers::CompoundTypeKind;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use crate::quote::TokenStreamExt;

pub struct DeriveStructuralReadable<S>(S);

impl<'a, 'b> ToTokens for DeriveStructuralReadable<SegregatedStructModel<'a, 'b>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralReadable(model) = self;
        let SegregatedStructModel { fields, inner } = model;
        let SegregatedFields { header, body } = fields;
        let name = inner.name;
        let target: syn::Type = parse_quote!(#name);
        let state = builder_state_type(inner);
        let builder = builder_type(&target, &state);
        let header_builder = header_builder(&target, &state);

        tokens.append_all(value_readable_impl(&target));
        tokens.append_all(structural_readable_impl(&target, &builder, inner));

        match body {
            BodyFields::StdBody(body_fields) if !body_fields.is_empty() => {

                tokens.append_all(header_reader(&builder, &header_builder, model));

                let (assoc, body_reader) = if inner.fields_model.body_kind == CompoundTypeKind::Struct {
                    let assoc = builder_assoc(&builder,
                                              syn::Ident::new("has_more", Span::call_site()),
                                              syn::Ident::new("apply", Span::call_site()),
                                              body_fields.as_slice());

                    let body_reader = builder_body_reader(&builder, body_fields.as_slice());
                    (assoc, body_reader)
                } else {
                    todo!()
                };

                tokens.append_all(assoc);
                tokens.append_all(body_reader);
            }
            BodyFields::StdBody(_) => {
                tokens.append_all(header_reader(&builder, &header_builder, model));
                tokens.append_all(empty_body_reader(&builder));
            }
            BodyFields::ReplacedBody(_) => {
                todo!()
            }
        }

        if header.has_tag_fields() {
            let has_header_slots = !header.header_fields.is_empty();

            if has_header_slots {
                let assoc = builder_assoc(&header_builder,
                                          syn::Ident::new("has_more_header", Span::call_site()),
                                          syn::Ident::new("apply_header", Span::call_site()),
                                          header.header_fields.as_slice());
                tokens.append_all(assoc);
            }

            tokens.append_all(header_builder_assoc(&header_builder, header.tag_body, !has_header_slots));
            tokens.append_all(header_builder_body_reader(&header_builder, &header));
        }
    }
}


fn builder_state_type<'a, 'b>(model: &'b StructModel<'a>) -> syn::Type {
    let fields = model.fields_model.fields.iter().map(|fld| {
        let ty = fld.model.field_ty;
        quote!(std::option::Option<#ty>)
    });

    parse_quote! {
        (#(#fields,)*)
    }
}

fn builder_type(target: &syn::Type, state: &syn::Type) -> syn::Type {
    parse_quote!(swim_common::form::structural::read::builder::Builder<#target, #state>)
}

fn header_builder(target: &syn::Type, state: &syn::Type) -> syn::Type {
    parse_quote!(swim_common::form::structural::read::builder::HeaderBuilder<#target, #state>)
}

fn value_readable_impl(target: &syn::Type) -> proc_macro2::TokenStream {
    quote! {
        impl swim_common::form::structural::read::ValueReadable for #target {}
    }
}

fn constructor<'a, 'b>(model: &'b StructModel<'a>) -> proc_macro2::TokenStream {
    let name = model.name;
    let ids = model.fields_model.fields.iter().map(|fld| &fld.model.name);

    match model.fields_model.type_kind {
        CompoundTypeKind::Struct => {
            let binders = ids.map(|id| quote!(#id: #id));
            quote! {
                #name {
                    #(#binders,)*
                }
            }
        }
        CompoundTypeKind::Unit => quote!(#name),
        _ => {
            quote! {
                #name(
                    #(#ids),*
                )
            }
        }
    }
}

fn structural_readable_impl<'a, 'b>(
    target: &syn::Type,
    builder: &syn::Type,
    model: &'b StructModel<'a>,
) -> proc_macro2::TokenStream {
    let checks = model.fields_model.fields.iter().zip(0..).map(|(fld, i)| {
        let index = syn::Index::from(i);
        let name_lit = fld.model.resolve_name();
        let ty = fld.model.field_ty;
        quote! {
            if state.#index.is_none() {
                let on_missing = <#ty as swim_common::form::structural::read::StructuralReadable>::on_absent();
                if on_missing.is_none() {
                    missing.push(swim_common::model::text::Text::new(#name_lit));
                } else {
                    state.#index = on_missing;
                }
            }
        }
    });

    let patterns = model.fields_model.fields.iter().map(|fld| {
        let fld_index = &fld.model.name;
        quote!(std::option::Option::Some(#fld_index))
    });

    let con = constructor(model);

    quote! {

        #[automatically_derived]
        impl swim_common::form::structural::read::StructuralReadable for #target {
            type Reader = #builder;

            fn record_reader() -> std::result::Result<Self::Reader, swim_common::form::structural::read::error::ReadError> {
                std::result::Result::Ok(std::default::Default::default())
            }

            fn try_terminate(reader: <Self::Reader as swim_common::form::structural::read::HeaderReader>::Body)
                -> std::result::Result<Self, swim_common::form::structural::read::error::ReadError> {
                let swim_common::form::structural::read::builder::Builder { mut state, .. } = reader;

                #(#checks)*

                if let (#(#patterns),*) = state {
                    std::result::Result::Ok(#con)
                } else {
                    std::result::Result::Err(swim_common::form::structural::read::error::ReadError::MissingFields(missing))
                }
            }
        }

    }
}

fn wrap_ccons_ty(head: &syn::Type, tail: syn::Type) -> syn::Type {
    parse_quote!(swim_common::form::structural::generic::coproduct::CCons<#head, #tail>)
}

fn make_ccons_ty<It>(it: It) -> syn::Type
where
    It: DoubleEndedIterator<Item = syn::Type>,
{
    let nil = parse_quote!(swim_common::form::structural::generic::coproduct::CNil);
    it.rev().fold(nil, |acc, ty| wrap_ccons_ty(&ty, acc))
}

fn make_ccons_pat(n: usize, sub: syn::Pat) -> syn::Pat {
    (0..n).fold(
        parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Head(#sub)),
        |acc, _| parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Tail(#acc)),
    )
}

fn make_ccons_nil_pat(n: usize) -> syn::Pat {
    (0..n).fold(
        parse_quote!(nil),
        |acc, _| parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Tail(#acc)),
    )
}

fn make_ccons_expr(n: usize, name: &syn::Ident) -> syn::Expr {
    (0..n).fold(
        parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Head(#name)),
        |acc, _| parse_quote!(swim_common::form::structural::generic::coproduct::CCons::Tail(#acc)),
    )
}

fn header_reader<'a, 'b>(
    builder: &syn::Type,
    header_builder: &syn::Type,
    model: &SegregatedStructModel<'a, 'b>,
) -> proc_macro2::TokenStream {
    let SegregatedStructModel { inner, fields } = model;

    let lit_name = inner.resolve_name();

    let attr_reader_tys = fields.header.attributes.iter().map(|fld| {
        let ty = fld.field_ty;
        parse_quote!(swim_common::form::structural::read::builder::Wrapped<Self, swim_common::form::structural::read::builder::AttrReader<#ty>>)
    });

    let delegate_ty = wrap_ccons_ty(header_builder, make_ccons_ty(attr_reader_tys));

    let has_body = fields.header.tag_body.is_some();

    let wrapped_id: syn::Ident = parse_quote!(wrapped);

    let attr_cases = fields.header.attributes.iter().zip(0..).map(|(fld, i)| {
        let name = fld.resolve_name();
        let con = make_ccons_expr(i + 1, &wrapped_id);
        quote! {
            #name => {
                let #wrapped_id = swim_common::form::structural::read::builder::Wrapped {
                    payload: self,
                    reader: swim_common::form::structural::read::builder::AttrReader::default(),
                };
                std::result::Result::Ok(#con)
            }
        }
    });

    let attr_restore_cases = fields.header.attributes
        .iter()
        .zip(0..)
        .map(|(fld, i)| {
            let pat = make_ccons_pat(i + 1, parse_quote! {
                swim_common::form::structural::read::builder::Wrapped {
                    mut payload,
                    reader,
                }
            });
            let index = syn::Index::from(fld.ordinal);
            let name_lit = fld.resolve_name();
            quote! {
                #pat => {
                    if payload.state.#index.is_some() {
                        return std::result::Result::Err(swim_common::form::structural::read::error::ReadError::DuplicateField(
                            swim_common::model::text::Text::new(#name_lit)));
                    } else {
                        payload.state.#index = std::option::Option::Some(reader.try_get_value()?);
                    }
                    std::result::Result::Ok(payload)
                }
            }
        });

    let nil_pat = make_ccons_nil_pat(fields.header.attributes.len() + 1);

    quote! {
        #[automatically_derived]
        impl swim_common::form::structural::read::HeaderReader for #builder {
            type Body = Self;
            type Delegate = #delegate_ty;

            fn read_attribute(self, name: std::borrow::Cow<'_, str>) -> Result<Self::Delegate, swim_common::form::structural::read::error::ReadError> {
                if !self.read_tag {
                    if name == #lit_name {
                        let builder = swim_common::form::structural::read::builder::HeaderBuilder::new(self, #has_body);
                        std::result::Result::Ok(swim_common::form::structural::generic::coproduct::CCons::Head(builder))
                    } else {
                        std::result::Result::Err(swim_common::form::structural::read::error::ReadError::MissingTag)
                    }
                } else {
                    match name.borrow() {
                        #(#attr_cases)*
                        ow => std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedField(ow.into())),
                    }
                }
            }

            fn restore(delegate: Self::Delegate) -> std::result::Result<Self, swim_common::form::structural::read::error::ReadError> {
                match delegate {
                    swim_common::form::structural::generic::coproduct::CCons::Head(
                        swim_common::form::structural::read::builder::HeaderBuilderHeaderBuilder { mut inner, .. }) => {
                        inner.read_tag = true;
                        std::result::Result::Ok(inner)
                    }
                    #(#attr_restore_cases)*
                    #nil_pat => nil.explode(),
                }
            }

            fn start_body(self) -> srd::result::Result<Self::Body, swim_common::form::structural::read::error::ReadError> {
                std::result::Result::Ok(self)
            }
        }
    }
}

fn builder_assoc<'a, 'b>(
    builder: &syn::Type,
    has_more_name: syn::Ident,
    apply_name: syn::Ident,
    fields:  &[&'b FieldModel<'a>],
) -> proc_macro2::TokenStream {
    let has_more = fields.iter().map(|fld| {
        let index = syn::Index::from(fld.ordinal);
        quote!(self.state.#index.is_none())
    });

    let field_pushes = fields.iter().map(|fld| {
        let ord = fld.ordinal;
        let index = syn::Index::from(fld.ordinal);
        let name = fld.resolve_name();
        quote!(#ord => push.apply(&mut self.state.#index, #name),)
    });

    let name_matches = fields.iter().map(|fld| {
        let ord = fld.ordinal;
        let name = fld.resolve_name();
        quote! {
            #name => {
                self.current_field = std::option::Option::Some(#ord);
                std::result::Result::Ok(true)
            }
        }
    });

    quote! {
        impl #builder {

            fn #has_more_name(&self) -> bool {
                #(#has_more)||*
            }

            fn #apply_name<__P: swim_common::form::structural::read::builder::prim::PushPrimValue>(
                &mut self, push: __P) -> std::result::Result<bool, swim_common::form::structural::read::error::ReadError> {
                if self.reading_slot {
                    self.reading_slot = false;
                    match self
                        .current_field
                        .take()
                        .ok_or_else(|| swim_common::form::structural::read::error::ReadError::UnexpectedKind(push.kind()))?
                    {
                        #(#field_pushes)*
                        _ => std::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState),
                    }?;
                    std::result::Result::Ok(self.#has_more_name())
                } else {
                    if let std::option::Option::Some(name) = push.text() {
                        match name {
                            #(#name_matches)*
                            name => std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedField(
                                swim_common::model::text::Text::new(name))),
                        }
                    } else {
                        std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedKind(push.kind()))
                    }
                }
            }

        }
    }
}

fn basic_types() -> [(syn::Ident, syn::Type); 10] {
    [
        (
            syn::Ident::new("push_i32", Span::call_site()),
            parse_quote!(i32),
        ),
        (
            syn::Ident::new("push_i64", Span::call_site()),
            parse_quote!(i64),
        ),
        (
            syn::Ident::new("push_u32", Span::call_site()),
            parse_quote!(u32),
        ),
        (
            syn::Ident::new("push_u64", Span::call_site()),
            parse_quote!(u64),
        ),
        (
            syn::Ident::new("push_f64", Span::call_site()),
            parse_quote!(f64),
        ),
        (
            syn::Ident::new("push_bool", Span::call_site()),
            parse_quote!(bool),
        ),
        (
            syn::Ident::new("push_big_int", Span::call_site()),
            parse_quote!(num_bigint::bigint::BigInt),
        ),
        (
            syn::Ident::new("push_big_uint", Span::call_site()),
            parse_quote!(num_bigint::biguint::BigUint),
        ),
        (
            syn::Ident::new("push_blob", Span::call_site()),
            parse_quote!(std::vec::Vec<u8>),
        ),
        (
            syn::Ident::new("push_text", Span::call_site()),
            parse_quote!(std::borrow::Cow<'_, str>),
        ),
    ]
}

fn prim_push_methods<'a>(
    basic: &'a [(syn::Ident, syn::Type)],
) -> impl Iterator<Item = syn::TraitItem> + 'a {
    basic.iter().map(|(name, ty)| {
        parse_quote! {
            fn #name(&mut self, value: #ty) -> std::result::Result<bool, swim_common::form::structural::read::error::ReadError> {
                self.apply(value)
            }
        }
    })
}

fn builder_body_reader<'a, 'b>(
    builder: &syn::Type,
    fields: &[&'b FieldModel<'a>],
) -> proc_macro2::TokenStream {
    let wrapped_tys = fields.iter().map(|fld| {
        let ty = fld.field_ty;
        parse_quote! {
            swim_common::form::structural::read::builder::Wrapped<Self, <#ty as swim_common::form::structural::read::StructuralReadable>::Reader>
        }
    });

    let delegate_ty: syn::Type = make_ccons_ty(wrapped_tys);

    let basic = basic_types();
    let push_methods = prim_push_methods(&basic);

    let wrapper_id: syn::Ident = parse_quote!(wrapper);

    let field_matches = fields.iter().zip(0..).map(|(fld, i)| {
        let ord = fld.ordinal;
        let ty = fld.field_ty;

        let expr = make_ccons_expr(i, &wrapper_id);
        quote! {
            std::option::Option::Some(#ord) => {
                let #wrapper_id = swim_common::form::structural::read::builder::Wrapped {
                    payload: self,
                    reader: <#ty as swim_common::form::structural::read::StructuralReadable>::record_reader()?,
                };
                std::result::Result::Ok(#expr)
            }
        }
    });

    let field_restores = fields.iter().zip(0..).map(|(fld, i)| {
        let pat = make_ccons_pat(i, parse_quote! {
            swim_common::form::structural::read::builder::Wrapped {
                mut payload,
                reader,
            }
        });
        let index = syn::Index::from(fld.ordinal);
        let name = fld.resolve_name();
        let ty = fld.field_ty;
        quote! {
            #pat => {
                if payload.state.#index.is_some() {
                    return std::result::Result::Err(ReadError::DuplicateField(swim_common::model::text::Text::new(#name)));
                } else {
                    payload.state.#index = std::option::Option::Some(<#ty as swim_common::form::structural::read::StructuralReadable>::try_terminate(reader)?);
                }
                payload
            }
        }
    });

    let nil_pat = make_ccons_nil_pat(fields.len());

    quote! {

        #[automatically_derived]
        impl swim_common::form::structural::read::BodyReader for #builder {
            type Delegate = #delegate_ty;

            fn push_extant(&mut self) -> std::result::Result<bool, swim_common::form::structural::read::error::ReadError> {
                self.apply(())
            }

            #(#push_methods)*

            fn start_slot(&mut self) -> std::result::Result<(), swim_common::form::structural::read::error::ReadError> {
                if self.reading_slot {
                    std::result::Result::Err(swim_common::form::structural::read::error::ReadError::DoubleSlot)
                } else {
                    self.reading_slot = true;
                    std::result::Result::Ok(())
                }
            }

            fn push_record(mut self) -> std::result::Result<Self::Delegate, swim_common::form::structural::read::error::ReadError> {
                if self.reading_slot {
                    match self.current_field.take() {
                        #(#field_matches)*
                        _ => srd::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState),
                    }
                } else {
                    std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedKind(swim_common::model::ValueKind::Record))
                }
            }

            fn restore(delegate: <Self::Delegate as swim_common::form::structural::read::HeaderReader>::Body) -> Result<Self, ReadError> {
                let mut payload = match delegate {
                    #(#field_restores)*
                    #nil_pat => nil.explode(),
                };
                payload.reading_slot = false;
                std::result::Result::Ok(payload)
            }
        }

    }
}

fn header_builder_assoc<'a>(
    header_builder: &syn::Type,
    header_body: Option<&FieldModel<'a>>,
    has_slots: bool,
) -> proc_macro2::TokenStream {
    let apply_def = if let Some(fld) = header_body {
        let index = syn::Index::from(fld.ordinal);
        let name = fld.resolve_name();
        let after_body = if has_slots {
            quote!(self.inner.apply_header(push))
        } else {
            quote!(std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedItem))
        };
        quote! {
            if self.after_body {
                #after_body
            } else {
                push.apply(&mut self.inner.state.#index, #name)?;
                self.after_body = true;
                std::result::Result::Ok(self.inner.has_more_header())
            }
        }
    } else {
        quote! {
            self.inner.apply_header(push)
        }
    };
    quote! {
        impl #header_builder {
            fn apply<__P: swim_common::form::structural::read::builder::prim::PushPrimValue>(&mut self, push: __P)
                -> std::result::Result<bool, swim_common::form::structural::read::error::ReadError> {
                #apply_def
            }
        }
    }
}

fn empty_body_reader(builder: &syn::Type) -> proc_macro2::TokenStream {
    quote! {
        #[automatically_derived]
        impl swim_common::form::structural::read::BodyReader for #builder {

            type Delegate = utilities::never::Never;

            fn push_record(mut self) -> std::result::Result<Self::Delegate, swim_common::form::structural::read::error::ReadError> {
                std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedSlot)
            }

            fn restore(delegate: <Self::Delegate as swim_common::form::structural::read::HeaderReader>::Body) -> std::result::Result<Self, swim_common::form::structural::read::error::ReadError> {
                delegate.explode()
            }
        }
    }
}


fn header_builder_body_reader<'a, 'b>(
    header_builder: &syn::Type,
    fields: &HeaderFields<'a, 'b>
) -> proc_macro2::TokenStream {
    let HeaderFields { tag_body, header_fields, .. } = fields;

    let wrapped_tys = tag_body.iter().chain(header_fields.iter()).map(|fld| {
        let ty = fld.field_ty;
        parse_quote! {
            swim_common::form::structural::read::builder::Wrapped<Self, <#ty as swim_common::form::structural::read::StructuralReadable>::Reader>
        }
    });

    let delegate_ty: syn::Type = make_ccons_ty(wrapped_tys);

    let basic = basic_types();
    let push_methods = prim_push_methods(&basic);

    let start_slot_base = quote! {
        if self.inner.reading_slot {
            std::result::Result::Err(swim_common::form::structural::read::error::ReadError::DoubleSlot)
        } else {
            self.inner.reading_slot = true;
            std::result::Result::Ok(())
        }
    };

    let start_slot_body = if tag_body.is_some() {
        quote! {
            if !self.after_body {
                std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedSlot)
            } else #start_slot_base
        }
    } else {
        start_slot_base
    };

    let wrapper_id: syn::Ident = parse_quote!(wrapper);

    let offsets = if tag_body.is_some() { 1.. } else { 0.. };

    let push_record_matches = header_fields.iter().zip(offsets.clone()).map(|(fld, i)| {
        let index = fld.ordinal;
        let ty = fld.field_ty;
        let ccons_expr = make_ccons_expr(i, &wrapper_id);
        quote! {
            std::option::Option::Some(#index) => {
                let wrapper = swim_common::form::structural::read::builder::Wrapped {
                    payload: self,
                    reader: <#ty as swim_common::form::structural::read::StructuralReadable>::record_reader()?,
                };
                std::result::Result::Ok(#ccons_expr)
            }
        }
    });

    let push_record_base = quote! {
        if self.inner.reading_slot {
            match self.inner.current_field.take() {
                #(#push_record_matches)*
                _ => std::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState),
            }
        } else {
            std::result::Result::Err(swim_common::form::structural::read::error::ReadError::UnexpectedKind(swim_common::model::ValueKind::Record))
        }
    };

    let push_record_body = if let Some(fld) = tag_body {
        let ty = fld.field_ty;
        quote! {
            if !self.after_body {
                let wrapper = swim_common::form::structural::read::builder::Wrapped {
                    payload: self,
                    reader: <#ty as swim_common::form::structural::read::StructuralReadable>::record_reader()?,
                };
                std::result::Result::Ok(swim_common::form::structural::generic::coproduct::CCons::Head(wrapper))
            } else #push_record_base
        }
    } else {
        push_record_base
    };

    let restore_body_case = tag_body.map(|fld| {
        let index = syn::Index::from(fld.ordinal);
        let ty = fld.field_ty;
        quote! {
            swim_common::form::structural::generic::coproduct::CCons::Head(swim_common::form::structural::read::builder::Wrapped {
                mut payload,
                reader,
            }) => {
                if payload.inner.state.#index.is_some() {
                    return std::result::Result::Err(swim_common::form::structural::read::error::ReadError::InconsistentState);
                } else {
                    payload.inner.state.#index = Some(<#ty as swim_common::form::structural::read::StructuralReadable>::try_terminate(reader)?);
                }
                payload.after_body = true;
                std::result::Result::Ok(payload)
            }
        }
    });

    let inner_pat: syn::Pat = parse_quote! {
        swim_common::form::structural::read::builder::Wrapped {
            mut payload,
            reader,
        }
    };

    let restore_slot_cases = header_fields.iter().zip(offsets).map(|(fld, i)| {
        let index = syn::Index::from(fld.ordinal);
        let name = fld.resolve_name();
        let ty = fld.field_ty;
        let pat = make_ccons_pat(i, inner_pat.clone());

        quote! {
            #pat => {
                if payload.inner.state.#index.is_some() {
                    return std::result::Result::Err(swim_common::form::structural::read::error::ReadError::DuplicateField(swim_common::model::text::Text::new(#name)));
                } else {
                    payload.inner.state.#index = Some(<#ty as swim_common::form::structural::read::StructuralReadable>::try_terminate(reader)?);
                }
                payload.inner.reading_slot = false;
                std::result::Result::Ok(payload)
            }
        }
    });

    let num_options = if tag_body.is_some() {
        header_fields.len() + 1
    } else {
        header_fields.len()
    };

    let nil_pat = make_ccons_nil_pat(num_options);

    quote! {
        #[automatically_derived]
        impl swim_common::form::structural::read::BodyReader for #header_builder {

            type Delegate = #delegate_ty;

            fn push_extant(&mut self) -> Result<bool, ReadError> {
                self.apply(())
            }

            #(#push_methods)*

            fn start_slot(&mut self) -> std::result::Result<(), swim_common::form::structural::read::error::ReadError> {
                #start_slot_body
            }

            fn push_record(mut self) -> std::result::Result<Self::Delegate, swim_common::form::structural::read::error::ReadError> {
                #push_record_body
            }

            fn restore(delegate: <Self::Delegate as swim_common::form::structural::read::HeaderReader>::Body) -> std::result::Result<Self, swim_common::form::structural::read::error::ReadError> {
                match delegate {
                    #restore_body_case
                    #(#restore_slot_cases)*
                    #nil_pat => nil.explode(),
                }
            }
        }
    }
}
