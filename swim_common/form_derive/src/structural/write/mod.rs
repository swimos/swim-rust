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
use crate::structural::model::enumeration::{EnumModel, SegregatedEnumModel};
use crate::structural::model::field::{BodyFields, FieldModel, HeaderFields, SegregatedFields};
use crate::structural::model::record::{SegregatedStructModel, StructModel};
use crate::structural::model::NameTransform;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{Ident, Pat, Path};
use utilities::CompoundTypeKind;

struct Destructure<'a>(&'a StructModel<'a>, bool);

impl<'a> Destructure<'a> {
    fn assign(model: &'a StructModel<'a>) -> Self {
        Destructure(model, false)
    }

    fn variant_match(model: &'a StructModel<'a>) -> Self {
        Destructure(model, true)
    }
}

struct WriteWithFn<'a, 'b>(&'b SegregatedStructModel<'a, 'b>);
struct WriteIntoFn<'a, 'b>(&'b SegregatedStructModel<'a, 'b>);

impl<'a, 'b> ToTokens for SegregatedEnumModel<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let SegregatedEnumModel { inner, variants } = self;
        let EnumModel { name, .. } = inner;
        let writer_trait = make_writer_trait();

        let impl_block = if variants.is_empty() {
            quote! {

                #[automatically_derived]
                impl swim_common::form::structural::write::StructuralWritable for #name {

                    #[allow(non_snake_case)]
                    #[inline]
                    fn write_with<__W: #writer_trait>(&self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        writer.write_extant()
                    }

                    #[allow(non_snake_case)]
                    #[inline]
                    fn write_into<__W: #writer_trait>(self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        writer.write_extant()
                    }
                }
            }
        } else {
            let write_with_cases = variants.iter().map(|v| {
                let name = v.inner.name;
                let destructure = Destructure::variant_match(v.inner);
                let write_with = WriteWithFn(v);

                quote! {
                    #name::#destructure => {
                        #write_with
                    }
                }
            });

            let write_into_cases = variants.iter().map(|v| {
                let name = v.inner.name;
                let destructure = Destructure::variant_match(v.inner);
                let write_into = WriteIntoFn(v);

                quote! {
                    #name::#destructure => {
                        #write_into
                    }
                }
            });

            quote! {

                #[automatically_derived]
                impl swim_common::form::structural::write::StructuralWritable for #name {

                    #[allow(non_snake_case, unused_variables)]
                    #[inline]
                    fn write_with<__W: #writer_trait>(&self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                        match self {
                                #(#write_with_cases)*
                            }
                    }

                    #[allow(non_snake_case, unused_variables)]
                    #[inline]
                    fn write_into<__W: #writer_trait>(self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
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

impl<'a, 'b> ToTokens for SegregatedStructModel<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let destructure = Destructure::assign(&self.inner);
        let name = self.inner.name;
        let write_with = WriteWithFn(self);
        let write_into = WriteIntoFn(self);
        let writer_trait = make_writer_trait();
        let writable_impl = quote! {

            #[automatically_derived]
            impl swim_common::form::structural::write::StructuralWritable for #name {

                #[allow(non_snake_case, unused_variables)]
                #[inline]
                fn write_with<__W: #writer_trait>(&self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                    let #destructure = self;
                    #write_with
                }

                #[allow(non_snake_case, unused_variables)]
                #[inline]
                fn write_into<__W: #writer_trait>(self, writer: __W) -> core::result::Result<__W::Repr, __W::Error> {
                    let #destructure = self;
                    #write_into
                }
            }
        };

        tokens.append_all(writable_impl);
    }
}

fn make_writer_trait() -> Path {
    parse_quote!(swim_common::form::structural::write::StructuralWriter)
}

fn write_attr_ref(field: &FieldModel) -> TokenStream {
    let field_index = &field.name;
    let literal_name = field.resolve_name();
    quote! {
        rec_writer = rec_writer.write_attr(std::borrow::Cow::Borrowed(#literal_name), &#field_index)?;
    }
}

fn write_attr_into(field: &FieldModel) -> TokenStream {
    let field_index = &field.name;
    let literal_name = field.resolve_name();
    quote! {
        rec_writer = rec_writer.write_into(#literal_name, &#field_index)?;
    }
}

fn write_slot_ref(field: &FieldModel) -> TokenStream {
    let field_index = &field.name;
    let literal_name = field.resolve_name();
    quote! {
        body_writer = body_writer.write_slot(std::borrow::Cow::Borrowed(#literal_name), &#field_index)?;
    }
}

fn write_slot_into(field: &FieldModel) -> TokenStream {
    let field_index = &field.name;
    let literal_name = field.resolve_name();
    quote! {
        body_writer = body_writer.write_slot_into(#literal_name, #field_index)?;
    }
}

fn tag_expr(
    name: &Ident,
    transform: Option<&NameTransform>,
    tag_name: &Option<&FieldModel>,
) -> TokenStream {
    if let Some(field) = tag_name {
        let field_index = &field.name;
        quote!(#field_index.as_str())
    } else if let Some(trans) = transform {
        match trans {
            NameTransform::Rename(name) => proc_macro2::Literal::string(&name).into_token_stream(),
        }
    } else {
        name.to_token_stream()
    }
}

impl<'a, 'b> ToTokens for WriteWithFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let WriteWithFn(SegregatedStructModel {
            inner:
                StructModel {
                    name,
                    fields_model,
                    transform,
                },
            fields,
        }) = self;
        let SegregatedFields { header, body } = fields;
        let HeaderFields {
            tag_name,
            tag_body,
            header_fields,
            attributes,
        } = header;

        let num_attrs = attributes.len() + 1;
        let tag = tag_expr(*name, transform.as_ref(), tag_name);

        let tag_statement = if header_fields.is_empty() {
            if let Some(tag_field) = tag_body.as_ref() {
                let field_index = &tag_field.name;
                quote! {
                    rec_writer = rec_writer.write_attr(std::borrow::Cow::Borrowed(#tag), &#field_index)?;
                }
            } else {
                quote! {
                    rec_writer = rec_writer.write_extant_attr(#tag)?;
                }
            }
        } else {
            let header = make_header(tag_body, header_fields.as_slice());
            quote! {
                rec_writer = rec_writer.write_attr_into(#tag, #header)?;
            }
        };

        let attr_statements = attributes.iter().map(|f| write_attr_ref(*f));

        let body_block = match body {
            BodyFields::ReplacedBody(field) => {
                let field_index = &field.name;
                quote! {
                     rec_writer.delegate(&#field_index)
                }
            }
            BodyFields::SlotBody(fields) => {
                let num_slots = fields.len();

                let body_kind = if fields_model.kind == CompoundTypeKind::Struct {
                    quote!(swim_common::form::structural::write::RecordBodyKind::MapLike)
                } else {
                    quote!(swim_common::form::structural::write::RecordBodyKind::ArrayLike)
                };

                let slot_statements = fields.iter().map(|f| write_slot_ref(*f));

                quote! {
                    let mut body_writer = rec_writer.complete_header(#body_kind, #num_slots)?;
                    #(#slot_statements)*
                    body_writer.done()
                }
            }
        };

        let body = quote! {
            let mut rec_writer = writer.record(#num_attrs)?;
            #tag_statement
            #(#attr_statements)*
            #body_block
        };
        tokens.append_all(body);
    }
}

fn make_header(tag_body: &Option<&FieldModel>, header_fields: &[&FieldModel]) -> TokenStream {
    let base_expr = quote!(swim_common::form::structural::write::header::NoSlots);
    let header_expr = header_fields.iter().rev().fold(base_expr, |expr, field| {
        let field_index = &field.name;
        let literal_name = field.resolve_name();
        quote! {
            #expr.prepend(#literal_name, #field_index)
        }
    });
    if let Some(body) = tag_body {
        let field_index = &body.name;
        quote! {
            #header_expr.with_body(#field_index)
        }
    } else {
        quote! {
            #header_expr.simple()
        }
    }
}

impl<'a, 'b> ToTokens for WriteIntoFn<'a, 'b> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let WriteIntoFn(SegregatedStructModel {
            inner:
                StructModel {
                    name,
                    fields_model,
                    transform,
                },
            fields,
        }) = self;
        let SegregatedFields { header, body } = fields;
        let HeaderFields {
            tag_name,
            tag_body,
            header_fields,
            attributes,
        } = header;

        let num_attrs = attributes.len() + 1;

        let tag = tag_expr(*name, transform.as_ref(), tag_name);

        let tag_statement = if header_fields.is_empty() {
            if let Some(tag_field) = tag_body.as_ref() {
                let field_index = &tag_field.name;
                quote! {
                    rec_writer = rec_writer.write_attr_into(#tag, #field_index)?;
                }
            } else {
                quote! {
                    rec_writer = rec_writer.write_extant_attr(#tag)?;
                }
            }
        } else {
            let header = make_header(tag_body, header_fields.as_slice());
            quote! {
                rec_writer = rec_writer.write_attr_into(#tag, #header)?;
            }
        };

        let attr_statements = attributes.iter().map(|f| write_attr_into(*f));

        let body_block = match body {
            BodyFields::ReplacedBody(field) => {
                let field_index = &field.name;
                quote! {
                     rec_writer.delegate_into(#field_index)
                }
            }
            BodyFields::SlotBody(fields) => {
                let num_slots = fields.len();

                let body_kind = if fields_model.kind == CompoundTypeKind::Struct {
                    quote!(swim_common::form::structural::write::RecordBodyKind::MapLike)
                } else {
                    quote!(swim_common::form::structural::write::RecordBodyKind::ArrayLike)
                };

                let slot_statements = fields.iter().map(|f| write_slot_into(*f));

                quote! {
                    let mut body_writer = rec_writer.complete_header(#body_kind, #num_slots)?;
                    #(#slot_statements)*
                    body_writer.done()
                }
            }
        };

        let body = quote! {
            let mut rec_writer = writer.record(#num_attrs)?;
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
            is_match,
        ) = self;
        let kind = fields_model.kind;
        let indexers = fields_model.fields.iter().map(|f| &f.model.name);
        match kind {
            CompoundTypeKind::Unit => {
                if *is_match {
                    tokens.append_all(name.to_token_stream());
                } else {
                    let pat: Pat = parse_quote!("_");
                    tokens.append_all(pat.to_token_stream());
                }
            }
            CompoundTypeKind::Struct => {
                let statement = quote!(#name(#(#indexers),*));
                tokens.append_all(statement);
            }
            _ => {
                let statement = quote!(#name { #(#indexers),* });
                tokens.append_all(statement);
            }
        }
    }
}
