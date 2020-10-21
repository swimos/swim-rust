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

use crate::form::form_parser::FormDescriptor;
use crate::parser::{EnumVariant, FieldKind, FormField, StructRepr, TypeContents};
use macro_helpers::{deconstruct_type, CompoundTypeKind, Label};
use proc_macro2::Ident;
use syn::export::TokenStream2;

pub fn to_value(
    type_contents: TypeContents<FormDescriptor, FormField<'_>>,
    structure_name: &Ident,
    fn_factory: fn(&Ident) -> TokenStream2,
    requires_deref: bool,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(StructRepr {
            compound_type,
            fields,
            descriptor,
            ..
        }) => build_struct_as_value(
            descriptor,
            &structure_name,
            &compound_type,
            &fields,
            fn_factory,
            requires_deref,
        ),
        TypeContents::Enum(variants) => {
            let arms = variants
                .into_iter()
                .fold(Vec::new(), |mut as_value_arms, variant| {
                    let EnumVariant {
                        name,
                        compound_type,
                        fields,
                        descriptor,
                        ..
                    } = variant;

                    let as_value = build_variant_as_value(
                        descriptor,
                        &name,
                        &compound_type,
                        &fields,
                        fn_factory,
                        requires_deref,
                        structure_name,
                    );

                    as_value_arms.push(as_value);
                    as_value_arms
                });

            if requires_deref {
                quote! {
                    match *self {
                        #(#arms)*
                    }
                }
            } else {
                quote! {
                    match self {
                        #(#arms)*
                    }
                }
            }
        }
    }
}

fn build_struct_as_value(
    mut descriptor: FormDescriptor,
    structure_name: &Ident,
    compound_type: &CompoundTypeKind,
    fields: &[FormField],
    fn_factory: fn(&Ident) -> TokenStream2,
    requires_deref: bool,
) -> TokenStream2 {
    let structure_name_str = descriptor.label.to_name();
    let (headers, attributes, items) = compute_as_value(&fields, &mut descriptor, fn_factory);
    let field_names: Vec<_> = fields.iter().map(|f| &f.label).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names, requires_deref);

    quote! {
        let #structure_name #self_deconstruction = self;
        let mut attrs = vec![swim_common::model::Attr::of((#structure_name_str #headers)), #attributes];
        swim_common::model::Value::Record(attrs, #items)
    }
}

fn build_variant_as_value(
    mut descriptor: FormDescriptor,
    variant_name: &Label,
    compound_type: &CompoundTypeKind,
    fields: &[FormField],
    fn_factory: fn(&Ident) -> TokenStream2,
    requires_deref: bool,
    structure_name: &Ident,
) -> TokenStream2 {
    let variant_original_ident = variant_name.original();
    let variant_name_str = variant_name.to_name();
    let (headers, attributes, items) = compute_as_value(&fields, &mut descriptor, fn_factory);
    let field_names: Vec<_> = fields.iter().map(|f| &f.label).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names, requires_deref);

    quote! {
        #structure_name::#variant_original_ident #self_deconstruction => {
            let mut attrs = vec![swim_common::model::Attr::of((#variant_name_str #headers)), #attributes];
            swim_common::model::Value::Record(attrs, #items)
        },
    }
}

fn compute_as_value(
    fields: &[FormField],
    descriptor: &mut FormDescriptor,
    fn_factory: fn(&Ident) -> TokenStream2,
) -> (TokenStream2, TokenStream2, TokenStream2) {
    let (mut headers, mut items, attributes) = fields
        .iter()
        .fold((TokenStream2::new(), TokenStream2::new(), TokenStream2::new()), |(mut headers, mut items, mut attributes), f| {
            let label = &f.label;
            let manifest = &descriptor.manifest;

            match &f.kind {
                FieldKind::Skip => {}
                FieldKind::Slot if !manifest.replaces_body => {
                    match label {
                        un @ Label::Anonymous(_) => {
                            let ident = un.as_ident();
                            let func = fn_factory(&ident);
                            items = quote!(#items swim_common::model::Item::ValueItem(#func),);
                        }
                        fi => {
                            let name_str = fi.to_string();
                            let func = fn_factory(&fi.as_ident());
                            items = quote!(#items swim_common::model::Item::Slot(swim_common::model::Value::text(#name_str), #func),);
                        }
                    }
                }
                FieldKind::Attr => {
                    let name_str = label.to_string();
                    let func = fn_factory(&label.as_ident());

                    attributes = quote!(#attributes swim_common::model::Attr::of((#name_str.to_string(), #func)),);
                }
                FieldKind::Body => {
                    descriptor.body_replaced = true;
                    let ident = f.label.as_ident();
                    let func = fn_factory(&ident);

                    items = quote!({
                        match #func {
                            swim_common::model::Value::Record(_attrs, items) => items,
                            v => vec![swim_common::model::Item::ValueItem(v)]
                        }
                    });
                }
                FieldKind::HeaderBody => {
                    let func = fn_factory(&label.as_ident());

                    if manifest.has_header_fields {
                        headers = quote!(#headers swim_common::model::Item::ValueItem(#func),);
                    } else {
                        headers = quote!(, #func);
                    }
                }
                FieldKind::Tagged => {
                    // no-op
                }
                _ => {
                    match label {
                        un @ Label::Anonymous(_) => {
                            let ident = un.as_ident();
                            let func = fn_factory(&ident);

                            headers = quote!(#headers swim_common::model::Item::ValueItem(#func),);
                        }
                        fi => {
                            let name_str = fi.to_string();
                            let func = fn_factory(&fi.as_ident());

                            headers = quote!(#headers swim_common::model::Item::Slot(swim_common::model::Value::text(#name_str), #func),);
                        }
                    }
                }
            }

            (headers, items, attributes)
        });

    let manifest = &descriptor.manifest;

    if manifest.has_header_fields || manifest.replaces_body {
        headers = quote!(, swim_common::model::Value::Record(Vec::new(), vec![#headers]));
    }

    if !descriptor.has_body_replaced() {
        items = quote!(vec![#items]);
    }

    (headers, attributes, items)
}
