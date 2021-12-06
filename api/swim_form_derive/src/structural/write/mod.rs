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

use crate::quote::TokenStreamExt;
use crate::structural::model::enumeration::{EnumModel, SegregatedEnumModel};
use crate::structural::model::field::{
    BodyFields, FieldModel, FieldSelector, HeaderFields, SegregatedFields,
};
use crate::structural::model::record::{SegregatedStructModel, StructModel};
use either::Either;
use macro_utilities::CompoundTypeKind;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{Generics, Pat, Path};

/// Implements the StructuralWritable trait for either of [`SegregatedStructModel`] or
/// [`SegregatedEnumModel`].
pub struct DeriveStructuralWritable<'a, S>(pub S, pub &'a Generics);

/// Context for generating a destructuring pattern for a struct or enum variant.
enum DestructureContext {
    /// Let binding for a struct.
    LetBinding,
    /// Variant match for an enum.
    VariantMatch,
}

struct Destructure<'a>(&'a StructModel<'a>, DestructureContext);

impl<'a> Destructure<'a> {
    fn assign(model: &'a StructModel<'a>) -> Self {
        Destructure(model, DestructureContext::LetBinding)
    }

    fn variant_match(model: &'a StructModel<'a>) -> Self {
        Destructure(model, DestructureContext::VariantMatch)
    }
}

struct WriteWithFn<'a>(&'a SegregatedStructModel<'a>);

struct WriteIntoFn<'a>(&'a SegregatedStructModel<'a>);

impl<'a> ToTokens for DeriveStructuralWritable<'a, SegregatedEnumModel<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralWritable(model, generics) = self;
        let SegregatedEnumModel { inner, variants } = model;
        let EnumModel { name, .. } = inner;
        let writer_trait = make_writer_trait();

        let mut new_generics = (*generics).clone();
        super::add_bounds(
            *generics,
            &mut new_generics,
            parse_quote!(swim_form::structural::write::StructuralWritable),
        );

        let (impl_lst, ty_params, where_clause) = new_generics.split_for_impl();

        let impl_block = if variants.is_empty() {
            quote! {

                #[automatically_derived]
                impl #impl_lst swim_form::structural::write::StructuralWritable for #name #ty_params #where_clause {

                    #[inline]
                    fn num_attributes(&self) -> usize {
                        match *self {}
                    }

                    #[allow(non_snake_case)]
                    #[inline]
                    fn write_with<__W: #writer_trait>(&self, _writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        match *self {}
                    }

                    #[allow(non_snake_case)]
                    #[inline]
                    fn write_into<__W: #writer_trait>(self, _writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        match self {}
                    }
                }
            }
        } else {
            let name = inner.name;
            let write_with_cases = variants.iter().map(|v| {
                let destructure = Destructure::variant_match(v.inner);

                let (write_with, num_attrs) = if let Some(selector) = v.inner.newtype_selector() {
                    (quote! { #selector.write_with(writer) }, quote! { 0 })
                } else {
                    (
                        WriteWithFn(v).to_token_stream(),
                        num_attributes_case(v, true).to_token_stream(),
                    )
                };
                quote! {
                    #name::#destructure => {
                        let num_attrs = #num_attrs;
                        #write_with
                    }
                }
            });

            let write_into_cases = variants.iter().map(|v| {
                let destructure = Destructure::variant_match(v.inner);

                let (write_into, num_attrs) = if let Some(selector) = v.inner.newtype_selector() {
                    (quote! { #selector.write_into(writer) }, quote! { 0 })
                } else {
                    (
                        WriteIntoFn(v).to_token_stream(),
                        num_attributes_case(v, false).to_token_stream(),
                    )
                };

                quote! {
                    #name::#destructure => {
                        let num_attrs = #num_attrs;
                        #write_into
                    }
                }
            });

            let num_attrs = NumAttrsEnum(model);

            quote! {

                #[automatically_derived]
                impl #impl_lst swim_form::structural::write::StructuralWritable for #name #ty_params #where_clause {

                    #[inline]
                    fn num_attributes(&self) -> usize {
                        #num_attrs
                    }

                    #[allow(non_snake_case, unused_variables)]
                    #[inline]
                    fn write_with<__W: #writer_trait>(&self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        use swim_form::structural::write::HeaderWriter;
                        use swim_form::structural::write::BodyWriter;
                        match self {
                            #(#write_with_cases)*
                        }
                    }

                    #[allow(non_snake_case, unused_variables)]
                    #[inline]
                    fn write_into<__W: #writer_trait>(self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        use swim_form::structural::write::HeaderWriter;
                        use swim_form::structural::write::BodyWriter;
                        match self {
                            #(#write_into_cases)*
                        }
                    }
                }
            }
        };
        tokens.append_all(impl_block);
    }
}

impl<'a> ToTokens for DeriveStructuralWritable<'a, SegregatedStructModel<'a>> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let DeriveStructuralWritable(inner, generics) = self;
        let mut new_generics = (*generics).clone();
        super::add_bounds(
            *generics,
            &mut new_generics,
            parse_quote!(swim_form::structural::write::StructuralWritable),
        );

        let (impl_lst, ty_params, where_clause) = new_generics.split_for_impl();

        let destructure = Destructure::assign(inner.inner);

        let name = inner.inner.name;
        let writer_trait = make_writer_trait();

        let (write_with, write_into, num_attrs) =
            if let Some(selector) = inner.inner.newtype_selector() {
                (
                    quote! { #selector.write_with(writer) },
                    quote! { #selector.write_into(writer) },
                    quote! { 0 },
                )
            } else {
                (
                    WriteWithFn(inner).to_token_stream(),
                    WriteIntoFn(inner).to_token_stream(),
                    num_attributes(inner).to_token_stream(),
                )
            };

        let writable_impl = quote! {

            #[automatically_derived]
            impl #impl_lst swim_form::structural::write::StructuralWritable for #name #ty_params #where_clause {

                #[inline]
                fn num_attributes(&self) -> usize {
                    #num_attrs
                }

                #[allow(non_snake_case, unused_variables)]
                #[inline]
                fn write_with<__W: #writer_trait>(&self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                    use swim_form::structural::write::HeaderWriter;
                    use swim_form::structural::write::BodyWriter;
                    let num_attrs = #num_attrs;
                    let #destructure = self;
                    #write_with
                }

                #[allow(non_snake_case, unused_variables)]
                #[inline]
                fn write_into<__W: #writer_trait>(self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                    use swim_form::structural::write::HeaderWriter;
                    use swim_form::structural::write::BodyWriter;
                    let num_attrs = #num_attrs;
                    let #destructure = self;
                    #write_into
                }
            }
        };

        tokens.append_all(writable_impl);
    }
}

fn make_writer_trait() -> Path {
    parse_quote!(swim_form::structural::write::StructuralWriter)
}

fn write_attr_ref(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    let literal_name = field.resolve_name();
    quote! {
        rec_writer = rec_writer.write_attr(std::borrow::Cow::Borrowed(#literal_name), #field_index)?;
    }
}

fn write_attr_into(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    let literal_name = field.resolve_name();
    quote! {
        rec_writer = rec_writer.write_attr_into(#literal_name, #field_index)?;
    }
}

fn write_slot_ref(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    let literal_name = field.resolve_name();
    quote! {
        if !swim_form::structural::write::StructuralWritable::omit_as_field(#field_index) {
            body_writer = body_writer.write_slot(&#literal_name, #field_index)?;
        }
    }
}

fn write_value_ref(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    quote! {
        body_writer = body_writer.write_value(#field_index)?;
    }
}

fn write_value_into(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    quote! {
        body_writer = body_writer.write_value_into(#field_index)?;
    }
}

fn write_slot_into(field: &FieldModel) -> TokenStream {
    let field_index = &field.selector;
    let literal_name = field.resolve_name();
    quote! {
        if !swim_form::structural::write::StructuralWritable::omit_as_field(&#field_index) {
            body_writer = body_writer.write_slot_into(#literal_name, #field_index)?;
        }
    }
}

fn compute_num_slots(fields: &[&FieldModel], by_ref: bool) -> TokenStream {
    let increments = fields.iter().map(|field| {
        let field_index = &field.selector;
        let fld = if by_ref {
            quote!(&#field_index)
        } else {
            field_index.to_token_stream()
        };
        quote! {
            if !swim_form::structural::write::StructuralWritable::omit_as_field(#fld) {
                num_slots += 1;
            }
        }
    });
    quote! {
        let mut num_slots: usize = 0;
        #(#increments)*
    }
}

impl<'a> ToTokens for WriteWithFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let WriteWithFn(model) = self;
        let SegregatedStructModel { inner, fields } = model;
        let StructModel { fields_model, .. } = inner;
        let SegregatedFields { header, body } = fields;
        let HeaderFields {
            tag_body,
            header_fields,
            attributes,
            ..
        } = header;

        let tag = if let Some(fld) = header.tag_name {
            let name = &fld.selector;
            quote! {
                core::convert::AsRef::<str>::as_ref(#name)
            }
        } else {
            inner.resolve_name().to_token_stream()
        };

        let tag_statement = if header_fields.is_empty() {
            if let Some(tag_field) = tag_body.as_ref() {
                let field_index = &tag_field.selector;
                quote! {
                    rec_writer = rec_writer.write_attr(std::borrow::Cow::Borrowed(#tag), #field_index)?;
                }
            } else {
                quote! {
                    rec_writer = rec_writer.write_extant_attr(#tag)?;
                }
            }
        } else {
            let header = make_header(tag_body, header_fields.as_slice(), true);
            quote! {
                rec_writer = rec_writer.write_attr_into(#tag, #header)?;
            }
        };

        let attr_statements = attributes.iter().map(|f| write_attr_ref(*f));

        let body_block = match body {
            BodyFields::ReplacedBody(field) => {
                let field_index = &field.selector;
                quote! {
                     rec_writer.delegate(#field_index)
                }
            }
            BodyFields::StdBody(fields) => {
                let num_slots = compute_num_slots(fields, false);

                let (body_kind, statements) =
                    if fields_model.body_kind == CompoundTypeKind::Labelled {
                        (
                            quote!(swim_form::structural::write::RecordBodyKind::MapLike),
                            Either::Left(fields.iter().map(|f| write_slot_ref(*f))),
                        )
                    } else {
                        (
                            quote!(swim_form::structural::write::RecordBodyKind::ArrayLike),
                            Either::Right(fields.iter().map(|f| write_value_ref(*f))),
                        )
                    };

                quote! {
                    #num_slots
                    let mut body_writer = rec_writer.complete_header(#body_kind, num_slots)?;
                    #(#statements)*
                    body_writer.done()
                }
            }
        };

        let body = quote! {
            let mut rec_writer = writer.record(num_attrs)?;
            #tag_statement
            #(#attr_statements)*
            #body_block
        };
        tokens.append_all(body);
    }
}

fn make_header(
    tag_body: &Option<&FieldModel>,
    header_fields: &[&FieldModel],
    by_ref: bool,
) -> TokenStream {
    let prepend = if by_ref {
        quote!(prepend_ref)
    } else {
        quote!(prepend)
    };

    let base_expr = quote!(swim_form::structural::generic::header::NoSlots);
    let header_expr = header_fields.iter().rev().fold(base_expr, |expr, field| {
        let field_index = &field.selector;
        let literal_name = field.resolve_name();
        quote! {
            #expr.#prepend(#literal_name, #field_index)
        }
    });
    if let Some(body) = tag_body {
        let with_body = if by_ref {
            quote!(with_body_ref)
        } else {
            quote!(with_body)
        };
        let field_index = &body.selector;
        quote! {
            #header_expr.#with_body(#field_index)
        }
    } else {
        quote! {
            #header_expr.simple()
        }
    }
}

impl<'a> ToTokens for WriteIntoFn<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let WriteIntoFn(model) = self;
        let SegregatedStructModel { inner, fields } = model;
        let StructModel { fields_model, .. } = inner;
        let SegregatedFields { header, body } = fields;
        let HeaderFields {
            tag_body,
            header_fields,
            attributes,
            ..
        } = header;

        let tag = if let Some(fld) = header.tag_name {
            let ty = fld.field_ty;
            let name = &fld.selector;
            quote!(<#ty as core::convert::AsRef::<str>>::as_ref(&#name))
        } else {
            inner.resolve_name().to_token_stream()
        };

        let tag_statement = if header_fields.is_empty() {
            if let Some(tag_field) = tag_body.as_ref() {
                let field_index = &tag_field.selector;
                quote! {
                    rec_writer = rec_writer.write_attr_into(#tag, #field_index)?;
                }
            } else {
                quote! {
                    rec_writer = rec_writer.write_extant_attr(#tag)?;
                }
            }
        } else {
            let header = make_header(tag_body, header_fields.as_slice(), false);
            quote! {
                rec_writer = rec_writer.write_attr_into(#tag, #header)?;
            }
        };

        let attr_statements = attributes.iter().map(|f| write_attr_into(*f));

        let body_block = match body {
            BodyFields::ReplacedBody(field) => {
                let field_index = &field.selector;
                quote! {
                     rec_writer.delegate_into(#field_index)
                }
            }
            BodyFields::StdBody(fields) => {
                let num_slots = compute_num_slots(fields, true);

                let (body_kind, statements) =
                    if fields_model.body_kind == CompoundTypeKind::Labelled {
                        (
                            quote!(swim_form::structural::write::RecordBodyKind::MapLike),
                            Either::Left(fields.iter().map(|f| write_slot_into(*f))),
                        )
                    } else {
                        (
                            quote!(swim_form::structural::write::RecordBodyKind::ArrayLike),
                            Either::Right(fields.iter().map(|f| write_value_into(*f))),
                        )
                    };

                quote! {
                    #num_slots
                    let mut body_writer = rec_writer.complete_header(#body_kind, num_slots)?;
                    #(#statements)*
                    body_writer.done()
                }
            }
        };

        let body = quote! {
            let mut rec_writer = writer.record(num_attrs)?;
            #tag_statement
            #(#attr_statements)*
            #body_block
        };
        tokens.append_all(body);
    }
}

impl<'a> ToTokens for Destructure<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Destructure(
            StructModel {
                name, fields_model, ..
            },
            context,
        ) = self;
        let indexers = fields_model
            .fields
            .iter()
            .map(|f| f.model.selector.binder());
        match fields_model.type_kind {
            CompoundTypeKind::Unit => {
                if matches!(context, DestructureContext::VariantMatch) {
                    tokens.append_all(name.to_token_stream());
                } else {
                    let pat: Pat = parse_quote!(_);
                    tokens.append_all(pat.to_token_stream());
                }
            }
            CompoundTypeKind::Labelled => {
                let statement = quote!(#name { #(#indexers),* });
                tokens.append_all(statement);
            }
            _ => {
                let statement = quote!(#name(#(#indexers),*));
                tokens.append_all(statement);
            }
        }
    }
}

fn num_attributes<'a>(model: &'a SegregatedStructModel<'a>) -> TokenStream {
    let base_attrs = model.fields.header.attributes.len() + 1;
    if let BodyFields::ReplacedBody(fld) = model.fields.body {
        let body_fld = match &fld.selector {
            FieldSelector::Named(id) => quote!(&self.#id),
            FieldSelector::Ordinal(i) => {
                let idx = syn::Index::from(*i);
                quote!(&self.#idx)
            }
        };
        quote!(#base_attrs + swim_form::structural::write::StructuralWritable::num_attributes(#body_fld))
    } else {
        quote!(#base_attrs)
    }
}

fn num_attributes_case<'a>(model: &'a SegregatedStructModel<'a>, by_ref: bool) -> TokenStream {
    let base_attrs = model.fields.header.attributes.len() + 1;
    if let BodyFields::ReplacedBody(fld) = model.fields.body {
        let name = &fld.selector;
        let body_fld = if by_ref {
            quote!(#name)
        } else {
            quote!(&#name)
        };
        quote!(#base_attrs + swim_form::structural::write::StructuralWritable::num_attributes(#body_fld))
    } else {
        quote!(#base_attrs)
    }
}

pub struct NumAttrsEnum<'a>(&'a SegregatedEnumModel<'a>);

impl<'a> ToTokens for NumAttrsEnum<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let NumAttrsEnum(SegregatedEnumModel { inner, variants }) = self;

        let enum_name = inner.name;

        let cases = variants.iter().map(|v| {
            let var_name = v.inner.name;
            let base_attrs = v.fields.header.attributes.len() + 1;
            if let BodyFields::ReplacedBody(fld) = v.fields.body {
                let fld_name = &fld.selector;
                let binder = fld_name.binder();
                let pat = match fld_name {
                    FieldSelector::Named(_) => quote!(#enum_name::#var_name { #binder, .. }),
                    FieldSelector::Ordinal(i) => {
                        let ignore = (0..*i).map(|_| quote!(_));
                        quote!(#enum_name::#var_name(#(#ignore,)* #binder, ..))
                    }
                };
                quote!(#pat => #base_attrs + swim_form::structural::write::StructuralWritable::num_attributes(#fld_name))
            } else {
                let pat = match v.inner.fields_model.type_kind {
                    CompoundTypeKind::Unit => quote!(#enum_name::#var_name),
                    CompoundTypeKind::Labelled => quote!(#enum_name::#var_name { .. }),
                    _ => quote!(#enum_name::#var_name(..)),
                };
                quote!(#pat => #base_attrs)
            }
        });
        tokens.append_all(quote! {
            match self {
                #(#cases,)*
            }
        });
    }
}
