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

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

use syn::export::TokenStream2;
use syn::{Data, DeriveInput, Fields};

use macro_helpers::{
    deconstruct_type, fields_from_ast, fold_quote, to_compile_errors, CompoundType, Context,
    FieldName,
};

use crate::manifest::{
    compute_field_manifest, FieldKind, FieldManifest, FieldWrapper, FormDescriptor,
};

#[allow(warnings)]
mod manifest;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let context = Context::default();
    let mut descriptor = FormDescriptor::from_ast(&context, &input);
    let mut field_bindings = match HconFields::from(&context, &input, &mut descriptor) {
        Some(cont) => cont,
        None => return to_compile_errors(context.check().unwrap_err()).into(),
    };

    let structure_name = descriptor.name.original_ident.clone();
    let structure_name_str = descriptor.name.tag_ident.to_string();
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let hcon_elems = field_bindings.build_hcons();

    let field_wrappers = &field_bindings.fields;
    let fields = &field_wrappers.iter().map(|f| &f.field).collect();
    let self_deconstruction = deconstruct_type(&field_bindings.compound_type, fields);

    let (headers, attributes, items) = compute_record(
        &field_wrappers,
        &mut descriptor,
        &mut field_bindings.field_manifest,
    );

    if let Err(e) = context.check() {
        return to_compile_errors(e).into();
    }

    let ts = quote! {
        impl #impl_generics crate::form::Form for #structure_name #ty_generics #where_clause
        {
            fn as_value(&self) -> Value {
                let #structure_name #self_deconstruction = self;
                let hlist = #hcon_elems;
                let mut attrs = vec![Attr::of((#structure_name_str #headers))];
                attrs.append(&mut #attributes);

                Value::Record(attrs, #items)
            }

            fn try_from_value(value: &Value) -> Result<Self, FormErr> {
                unimplemented!()
            }
        }
    };

    // println!("{}", ts.to_string());

    ts.into()
}

struct HconFields<'t> {
    pub compound_type: CompoundType,
    pub fields: Vec<FieldWrapper<'t>>,
    pub field_manifest: FieldManifest,
}

impl<'t> HconFields<'t> {
    pub fn from(
        context: &Context,
        input: &'t syn::DeriveInput,
        _descriptor: &mut FormDescriptor,
    ) -> Option<Self> {
        let (fields, compound_type) = match &input.data {
            Data::Enum(_data) => unimplemented!(),
            Data::Struct(data) => match &data.fields {
                Fields::Named(fields) => (fields_from_ast(&fields.named), CompoundType::Struct),
                Fields::Unnamed(fields) => (fields_from_ast(&fields.unnamed), CompoundType::Tuple),
                Fields::Unit => (Vec::new(), CompoundType::Unit),
            },
            Data::Union(_) => {
                context.error_spanned_by(input, "Unions are not supported");
                return None;
            }
        };

        let (fields, field_manifest) = compute_field_manifest(&context, fields);

        Some(Self {
            compound_type,
            fields,
            field_manifest,
        })
    }

    pub fn build_hcons(&self) -> TokenStream2 {
        fold_quote(
            quote!(crate::datastructures::hlist::HNil),
            self.fields.iter().map(|f| {
                let ty = &f.field.name;
                quote!(#ty)
            }),
            |item, result| quote! { crate::datastructures::hlist::HCons { head: #item, tail: #result }},
        )
    }
}

fn compute_record(
    fields: &[FieldWrapper],
    descriptor: &mut FormDescriptor,
    manifest: &mut FieldManifest,
) -> (TokenStream2, TokenStream2, TokenStream2) {
    let (mut headers, attrs, mut items) = fields
        .iter()
        // Headers, attributes, items
        .fold((TokenStream2::new(), TokenStream2::new(), TokenStream2::new()), |(headers, attrs, items), f| {
            let name = &f.name;

            match &f.kind {
                FieldKind::Slot if !manifest.replaces_body => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            (headers, attrs, quote!(#items Item::Slot(Value::Text(#name_str.to_string()), #ident.as_value()),))
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            (headers, attrs, quote!(#items Item::Slot(Value::Text(#name_str.to_string()), #old.as_value()),))
                        }
                        FieldName::Unnamed(_index) => {
                            (headers, attrs, quote!(#items Item::ValueItem(#name.as_value()),))
                        }
                    }
                }
                FieldKind::Slot if manifest.replaces_body => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            (quote!(#headers Item::Slot(Value::Text(#name_str.to_string()), #ident.as_value()),), attrs, items)
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            (quote!(#headers Item::Slot(Value::Text(#name_str.to_string()), #old.as_value()),), attrs, items)
                        }
                        un @ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            (quote!(#headers Item::ValueItem(#ident.as_value()),), attrs, items)
                        }
                    }
                }
                FieldKind::Attr => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            (headers, quote!(#attrs Attr::of((#name_str.to_string(), #ident.as_value())),), items)
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            (headers, quote!(#attrs Attr::of((#name_str.to_string(), #old.as_value())),), items)
                        }
                        FieldName::Unnamed(_index) => unimplemented!(),
                    }
                }
                FieldKind::Body => {
                    descriptor.body_replaced = true;
                    let ident = f.name.as_ident();

                    (headers, attrs, quote!({
                        match #ident.as_value() {
                            Value::Record(_attrs, items) => items,
                            v => vec![Item::ValueItem(v)]
                        }
                    }))
                }
                FieldKind::HeaderBody => {
                    if manifest.has_header_fields {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                (quote!(#headers Item::ValueItem(#ident.as_value()),), attrs, items)
                            }
                            FieldName::Unnamed(index) => {
                                (quote!(#headers Item::ValueItem(#index.as_value()),), attrs, items)
                            }
                        }
                    } else {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                (quote!(, #ident.as_value()), attrs, items)
                            }
                            FieldName::Unnamed(index) => {
                                (quote!(, #index.as_value()), attrs, items)
                            }
                        }
                    }
                }
                _ => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            (quote!(#headers Item::Slot(Value::Text(#name_str.to_string()), #ident.as_value()),), attrs, items)
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            (quote!(#headers Item::Slot(Value::Text(#name_str.to_string()), #old.as_value()),), attrs, items)
                        }
                        FieldName::Unnamed(_index) => {
                            (quote!(#headers Item::ValueItem(#name.as_value()),), attrs, items)
                        }
                    }
                }
            }
        });

    if manifest.has_header_fields || manifest.replaces_body {
        headers = quote!(, Value::Record(Vec::new(), vec![#headers]));
    }

    if !descriptor.has_body_replaced() {
        items = quote!(vec![#items]);
    }

    (headers, quote!(vec![#attrs]), items)
}
