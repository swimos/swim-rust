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

use crate::parser::{
    EnumVariant, Field, FieldKind, FieldManifest, FormDescriptor, RecordTokenStreams, StructRepr,
    TypeContents,
};
use macro_helpers::{deconstruct_type, CompoundType, FieldName};
use proc_macro2::Ident;
use syn::export::TokenStream2;

pub fn to_value(
    type_contents: TypeContents,
    structure_name: &Ident,
    descriptor: FormDescriptor,
) -> TokenStream2 {
    match type_contents {
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
    }
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
        let mut attrs = vec![swim_common::model::Attr::of((#structure_name_str #headers)), #attributes];
        swim_common::model::Value::Record(attrs, #items)
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
            let mut attrs = vec![swim_common::model::Attr::of((#variant_name_str #headers)), #attributes];
            swim_common::model::Value::Record(attrs, #items)
        },
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
                            as_value_ts.transform_items(|items| quote!(#items swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #ident.as_value()),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_items(|items| quote!(#items swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #old.as_value()),));
                        }
                        un @ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            as_value_ts.transform_items(|items| quote!(#items swim_common::model::Item::ValueItem(#ident.as_value()),));
                        }
                    }
                }
                FieldKind::Attr => {
                    match name {
                        FieldName::Named(ident) => {
                            let name_str = ident.to_string();
                            as_value_ts.transform_attrs(|attrs| quote!(#attrs swim_common::model::Attr::of((#name_str.to_string(), #ident.as_value())),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_attrs(|attrs| quote!(#attrs swim_common::model::Attr::of((#name_str.to_string(), #old.as_value())),));
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
                            swim_common::model::Value::Record(_attrs, items) => items,
                            v => vec![swim_common::model::Item::ValueItem(v)]
                        }
                    }));
                }
                FieldKind::HeaderBody => {
                    if manifest.has_header_fields {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                as_value_ts.transform_headers(|headers| quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),));
                            }
                            un @ FieldName::Unnamed(_) => {
                                let ident = un.as_ident();
                                as_value_ts.transform_headers(|headers| quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),));
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
                            as_value_ts.transform_headers(|headers|quote!(#headers swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #ident.as_value()),));
                        }
                        FieldName::Renamed(new, old) => {
                            let name_str = new.to_string();
                            as_value_ts.transform_headers(|headers| quote!(#headers swim_common::model::Item::Slot(swim_common::model::Value::Text(#name_str.to_string()), #old.as_value()),));
                        }
                        un @ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            as_value_ts.transform_headers(|headers|quote!(#headers swim_common::model::Item::ValueItem(#ident.as_value()),));
                        }
                    }
                }
            }

            as_value_ts
        });

    if manifest.has_header_fields || manifest.replaces_body {
        as_value_ts.transform_headers(
            |headers| quote!(, swim_common::model::Value::Record(Vec::new(), vec![#headers])),
        );
    }

    if !descriptor.has_body_replaced() {
        as_value_ts.transform_items(|items| quote!(vec![#items]));
    }

    as_value_ts
}
