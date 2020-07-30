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

use proc_macro2::Ident;
use syn::export::TokenStream2;
use syn::DeriveInput;

#[allow(warnings)]
mod from_value;
use from_value::from_value;

use macro_helpers::{deconstruct_type, to_compile_errors, CompoundType, Context, FieldName};

use crate::parser::{
    EnumVariant, Field, FieldKind, FieldManifest, FormDescriptor, StructRepr, TypeContents,
};

mod parser;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let context = Context::default();
    let mut descriptor = FormDescriptor::from_ast(&context, &input);
    let structure_name = descriptor.name.original_ident.clone();
    let type_contents = match TypeContents::from(&context, &input, &mut descriptor) {
        Some(cont) => cont,
        None => return to_compile_errors(context.check().unwrap_err()).into(),
    };

    let from_value_body = from_value(&type_contents, &structure_name, &descriptor);

    let as_value_body = match type_contents {
        TypeContents::Struct(StructRepr {
            compound_type,
            fields,
            manifest,
        }) => build_struct_as_value(
            descriptor,
            manifest,
            &structure_name,
            &compound_type,
            &fields,
        ),
        TypeContents::Enum(variants) => {
            let arms = variants
                .into_iter()
                .fold(Vec::new(), |mut as_value_arms, variant| {
                    let EnumVariant {
                        name,
                        compound_type,
                        fields,
                        manifest,
                    } = variant;

                    let as_value = build_variant_as_value(
                        descriptor.clone(),
                        manifest,
                        &name,
                        &compound_type,
                        &fields,
                    );

                    as_value_arms.push(as_value);

                    as_value_arms
                });

            quote! {
                match *self {
                    #(#arms)*
                }
            }
        }
    };

    if let Err(e) = context.check() {
        return to_compile_errors(e).into();
    }

    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let ts = quote! {
        impl #impl_generics crate::form::Form for #structure_name #ty_generics #where_clause
        {
            #[inline]
            #[allow(non_snake_case)]
            fn as_value(&self) -> crate::model::Value {
                #as_value_body
            }

            #[inline]
            #[allow(non_snake_case)]
            fn try_from_value(value: &crate::model::Value) -> Result<Self, crate::form::FormErr> {
                #from_value_body
            }
        }
    };

    ts.into()
}

fn build_struct_as_value(
    mut descriptor: FormDescriptor,
    mut manifest: FieldManifest,
    structure_name: &Ident,
    compound_type: &CompoundType,
    fields: &[Field],
) -> TokenStream2 {
    let structure_name_str = descriptor.name.tag_ident.to_string();
    let RecordTokenStreams {
        headers,
        attributes,
        items,
    } = compute_as_value(&fields, &mut descriptor, &mut manifest);
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        let #structure_name #self_deconstruction = self;
        let mut attrs = vec![crate::model::Attr::of((#structure_name_str #headers)), #attributes];
        crate::model::Value::Record(attrs, #items)
    }
}

fn build_variant_as_value(
    mut descriptor: FormDescriptor,
    mut manifest: FieldManifest,
    variant_name: &FieldName,
    compound_type: &CompoundType,
    fields: &[Field],
) -> TokenStream2 {
    let variant_name_str = variant_name.to_string();
    let RecordTokenStreams {
        headers,
        attributes,
        items,
    } = compute_as_value(&fields, &mut descriptor, &mut manifest);
    let structure_name = &descriptor.name.original_ident;
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        #structure_name::#variant_name #self_deconstruction => {
            let mut attrs = vec![crate::model::Attr::of((#variant_name_str #headers)), #attributes];
            crate::model::Value::Record(attrs, #items)
        },
    }
}

#[derive(Default)]
struct RecordTokenStreams {
    headers: TokenStream2,
    attributes: TokenStream2,
    items: TokenStream2,
}

impl RecordTokenStreams {
    fn transform_items<F>(&mut self, f: F)
    where
        F: FnOnce(&TokenStream2) -> TokenStream2,
    {
        let items = &self.items;
        self.items = f(items);
    }

    fn transform_attrs<F>(&mut self, f: F)
    where
        F: FnOnce(&TokenStream2) -> TokenStream2,
    {
        let attrs = &self.attributes;
        self.attributes = f(attrs);
    }

    fn transform_headers<F>(&mut self, f: F)
    where
        F: FnOnce(&TokenStream2) -> TokenStream2,
    {
        let headers = &self.headers;
        self.headers = f(headers);
    }
}

fn compute_as_value(
    fields: &[Field],
    descriptor: &mut FormDescriptor,
    manifest: &mut FieldManifest,
) -> RecordTokenStreams {
    let mut as_value_ts = fields
        .iter()
        .fold(RecordTokenStreams::default(), |mut as_value_ts, f| {
            let name = &f.name;

            match &f.kind {
                FieldKind::Skip => {}
                FieldKind::Slot if !manifest.replaces_body => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            as_value_ts.transform_items(|items| quote!(#items crate::model::Item::Slot(crate::model::Value::Text(#name_str.to_string()), #ident.as_value()),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_items(|items| quote!(#items crate::model::Item::Slot(crate::model::Value::Text(#name_str.to_string()), #old.as_value()),));
                        }
                        un @ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            as_value_ts.transform_items(|items| quote!(#items crate::model::Item::ValueItem(#ident.as_value()),));
                        }
                    }
                }
                FieldKind::Attr => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            as_value_ts.transform_attrs(|attrs| quote!(#attrs crate::model::Attr::of((#name_str.to_string(), #ident.as_value())),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_attrs(|attrs| quote!(#attrs crate::model::Attr::of((#name_str.to_string(), #old.as_value())),));
                        }
                        FieldName::Unnamed(_index) => {
                            // This has bene checked already when parsing the AST.
                            unreachable!()
                        }
                    }
                }
                FieldKind::Body => {
                    descriptor.body_replaced = true;
                    let ident = f.name.as_ident();

                    as_value_ts.transform_items(|_items| quote!({
                        match #ident.as_value() {
                            crate::model::Value::Record(_attrs, items) => items,
                            v => vec![crate::model::Item::ValueItem(v)]
                        }
                    }));
                }
                FieldKind::HeaderBody => {
                    if manifest.has_header_fields {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                as_value_ts.transform_headers(|headers| quote!(#headers crate::model::Item::ValueItem(#ident.as_value()),));
                            }
                            un @ FieldName::Unnamed(_) => {
                                let ident = un.as_ident();
                                as_value_ts.transform_headers(|headers| quote!(#headers crate::model::Item::ValueItem(#ident.as_value()),));
                            }
                        }
                    } else {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                as_value_ts.transform_headers(|_headers| quote!(, #ident.as_value()));
                            }
                            un @ FieldName::Unnamed(_) => {
                                let ident = un.as_ident();
                                as_value_ts.transform_headers(|_headers| quote!(, #ident.as_value()));
                            }
                        }
                    }
                }
                _ => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            as_value_ts.transform_headers(|headers|quote!(#headers crate::model::Item::Slot(crate::model::Value::Text(#name_str.to_string()), #ident.as_value()),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_headers(|headers| quote!(#headers crate::model::Item::Slot(crate::model::Value::Text(#name_str.to_string()), #old.as_value()),));
                        }
                        un @ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            as_value_ts.transform_headers(|headers|quote!(#headers crate::model::Item::ValueItem(#ident.as_value()),));
                        }
                    }
                }
            }

            as_value_ts
        });

    if manifest.has_header_fields || manifest.replaces_body {
        as_value_ts.transform_headers(
            |headers| quote!(, crate::model::Value::Record(Vec::new(), vec![#headers])),
        );
    }

    if !descriptor.has_body_replaced() {
        as_value_ts.transform_items(|items| quote!(vec![#items]));
    }

    as_value_ts
}
