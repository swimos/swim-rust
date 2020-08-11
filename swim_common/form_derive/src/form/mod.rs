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

use crate::form::form_parser::{build_type_contents, FormDescriptor};
use crate::parser::{EnumVariant, FieldKind, FieldManifest, FormField, StructRepr, TypeContents};
use macro_helpers::{deconstruct_type, CompoundTypeKind, Context, Identity};
use proc_macro2::Ident;
use syn::export::TokenStream2;
use syn::DeriveInput;

pub mod form_parser;

pub fn build_derive_form(input: DeriveInput) -> Result<proc_macro2::TokenStream, Vec<syn::Error>> {
    let mut context = Context::default();
    let type_contents = match build_type_contents(&mut context, &input) {
        Some(cont) => cont,
        None => return Err(context.check().unwrap_err()),
    };

    let structure_name = &input.ident;
    let as_value_body = to_value(type_contents, structure_name);

    context.check()?;

    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let ts = quote! {
        impl #impl_generics swim_common::form::Form for #structure_name #ty_generics #where_clause
        {
            #[inline]
            #[allow(non_snake_case)]
            fn as_value(&self) -> swim_common::model::Value {
                #as_value_body
            }

            #[inline]
            #[allow(non_snake_case)]
            fn try_from_value(value: &swim_common::model::Value) -> Result<Self, swim_common::form::FormErr> {
                unimplemented!()
            }
        }
    };

    Ok(ts)
}

fn to_value(
    type_contents: TypeContents<FormDescriptor, FormField<'_>>,
    structure_name: &Ident,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(StructRepr {
            compound_type,
            fields,
            manifest,
            descriptor,
            ..
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
                        descriptor,
                        ..
                    } = variant;

                    let as_value = build_variant_as_value(
                        descriptor.clone(),
                        manifest,
                        &name,
                        &compound_type,
                        &fields,
                        structure_name,
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
    }
}

fn build_struct_as_value(
    mut descriptor: FormDescriptor,
    mut manifest: FieldManifest,
    structure_name: &Ident,
    compound_type: &CompoundTypeKind,
    fields: &[FormField],
) -> TokenStream2 {
    let structure_name_str = descriptor.name.to_string();
    let (headers, attributes, items) = compute_as_value(&fields, &mut descriptor, &mut manifest);
    let field_names: Vec<_> = fields.iter().map(|f| &f.identity).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        let #structure_name #self_deconstruction = self;
        let mut attrs = vec![swim_common::model::Attr::of((#structure_name_str #headers)), #attributes];
        swim_common::model::Value::Record(attrs, #items)
    }
}

fn build_variant_as_value(
    mut descriptor: FormDescriptor,
    mut manifest: FieldManifest,
    variant_name: &Identity,
    compound_type: &CompoundTypeKind,
    fields: &[FormField],
    structure_name: &Ident,
) -> TokenStream2 {
    let variant_name_str = variant_name.to_string();
    let (headers, attributes, items) = compute_as_value(&fields, &mut descriptor, &mut manifest);
    let field_names: Vec<_> = fields.iter().map(|f| &f.identity).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        #structure_name::#variant_name #self_deconstruction => {
            let mut attrs = vec![swim_common::model::Attr::of((#variant_name_str #headers)), #attributes];
            swim_common::model::Value::Record(attrs, #items)
        },
    }
}

fn compute_as_value(
    fields: &[FormField],
    descriptor: &mut FormDescriptor,
    manifest: &mut FieldManifest,
) -> (TokenStream2, TokenStream2, TokenStream2) {
    let (mut headers, mut items, attributes) = fields
        .iter()
        .fold((TokenStream2::new(), TokenStream2::new(), TokenStream2::new()), |(mut headers,mut items,mut attributes), f| {
            let name = &f.identity;

            match &f.kind {
                FieldKind::Skip => {}
                FieldKind::Slot if !manifest.replaces_body => {
                    match name {
                        Identity::Named(ident) => {
                            let name_str = ident.to_string();
                            items = quote!(#items swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #ident.as_value()),) ;
                        }
                        Identity::Renamed{new_identity, old_identity} => {
                            let name_str = new_identity.to_string();
                            items = quote!(#items swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #old_identity.as_value()),);
                        }
                        un @ Identity::Anonymous(_) => {
                            let ident = un.as_ident();
                            items =  quote!(#items swim_common::model::Item::ValueItem(#ident.as_value()),);
                        }
                    }
                }
                FieldKind::Attr => {
                    match name {
                        Identity::Named(ident) => {
                            let name_str = ident.to_string();
                            attributes = quote!(#attributes swim_common::model::Attr::of((#name_str.to_string(), #ident.as_value())),);
                        }
                        Identity::Renamed{new_identity, old_identity} => {
                            let name_str = new_identity.to_string();
                            attributes = quote!(#attributes swim_common::model::Attr::of((#name_str.to_string(), #old_identity.as_value())),);
                        }
                        Identity::Anonymous(_index) => {
                            // This has been checked already when parsing the AST.
                            unreachable!()
                        }
                    }
                }
                FieldKind::Body => {
                    descriptor.body_replaced = true;
                    let ident = f.identity.as_ident();

                    items = quote!({
                        match #ident.as_value() {
                            swim_common::model::Value::Record(_attrs, items) => items,
                            v => vec![swim_common::model::Item::ValueItem(v)]
                        }
                    });
                }
                FieldKind::HeaderBody => {
                    if manifest.has_header_fields {
                        match name {
                            Identity::Renamed{old_identity:ident,..} | Identity::Named(ident) => {
                                headers = quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),);
                            }
                            un @ Identity::Anonymous(_) => {
                                let ident = un.as_ident();
                                headers= quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),);
                            }
                        }
                    } else {
                        match name {
                            Identity::Renamed{old_identity:ident,..} | Identity::Named(ident) => {
                                headers = quote!(, #ident.as_value());
                            }
                            un @ Identity::Anonymous(_) => {
                                let ident = un.as_ident();
                                headers = quote!(, #ident.as_value());
                            }
                        }
                    }
                }
                _ => {
                    match name {
                        Identity::Named(ident) => {
                            let name_str = ident.to_string();
                            headers = quote!(#headers swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #ident.as_value()),);
                        }
                        Identity::Renamed{new_identity, old_identity} => {
                            let name_str = new_identity.to_string();
                            headers = quote!(#headers swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #old_identity.as_value()),);
                        }
                        un @ Identity::Anonymous(_) => {
                            let ident = un.as_ident();
                            headers = quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),);
                        }
                    }
                }
            }

            (headers, items, attributes)
        });

    if manifest.has_header_fields || manifest.replaces_body {
        headers = quote!(, swim_common::model::Value::Record(Vec::new(), vec![#headers]));
    }

    if !descriptor.has_body_replaced() {
        items = quote!(vec![#items]);
    }

    (headers, attributes, items)
}
