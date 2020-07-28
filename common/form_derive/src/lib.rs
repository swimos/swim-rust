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
use syn::{Data, DeriveInput, Lit, Meta, NestedMeta};

use macro_helpers::{
    deconstruct_type, fold_quote, to_compile_errors, CompoundType, Context, FieldName,
};

use crate::parser::{
    parse_struct, Attributes, EnumVariant, Field, FieldKind, FieldManifest, FormDescriptor,
    StructRepr, TypeContents, FORM_PATH, TAG_PATH,
};
use proc_macro2::Ident;

mod parser;

#[proc_macro_derive(Form, attributes(form))]
pub fn derive_form(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let context = Context::default();
    let mut descriptor = FormDescriptor::from_ast(&context, &input);
    let field_bindings = match FormRepr::from(&context, &input, &mut descriptor) {
        Some(cont) => cont,
        None => return to_compile_errors(context.check().unwrap_err()).into(),
    };

    let structure_name = descriptor.name.original_ident.clone();
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

    let as_value_body = match field_bindings.type_contents {
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
            let arms = variants.into_iter().fold(Vec::new(), |mut ts, variant| {
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

                ts.push(as_value);

                ts
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

    let ts = quote! {
        impl #impl_generics crate::form::Form for #structure_name #ty_generics #where_clause
        {
            fn as_value(&self) -> Value {
                #as_value_body
            }

            fn try_from_value(value: &Value) -> Result<Self, FormErr> {
                unimplemented!()
            }
        }
    };

    // println!("{}", ts.to_string());

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
    let (headers, attributes, items) = compute_record(&fields, &mut descriptor, &mut manifest);
    let hcon_elems = build_hcons(fields);
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        let #structure_name #self_deconstruction = self;
        let hlist = #hcon_elems;
        let mut attrs = vec![Attr::of((#structure_name_str #headers))];
        attrs.append(&mut #attributes);

        Value::Record(attrs, #items)
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
    let (headers, attributes, items) = compute_record(&fields, &mut descriptor, &mut manifest);
    let structure_name = &descriptor.name.original_ident;
    let hcon_elems = build_hcons(fields);
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let self_deconstruction = deconstruct_type(compound_type, &field_names);

    quote! {
        #structure_name::#variant_name #self_deconstruction => {
            let hlist = #hcon_elems;
            let mut attrs = vec![Attr::of((#variant_name_str #headers))];
            attrs.append(&mut #attributes);

            Value::Record(attrs, #items)
        },
    }
}

struct FormRepr<'t> {
    pub type_contents: TypeContents<'t>,
}

impl<'t> FormRepr<'t> {
    pub fn from(
        context: &Context,
        input: &'t syn::DeriveInput,
        _descriptor: &mut FormDescriptor,
    ) -> Option<Self> {
        let type_contents = match &input.data {
            Data::Enum(data) => {
                let variants = data
                    .variants
                    .iter()
                    .map(|variant| {
                        let mut name_opt = None;

                        variant
                            .attrs
                            .get_attributes(context, FORM_PATH)
                            .iter()
                            .for_each(|meta| match meta {
                                NestedMeta::Meta(Meta::NameValue(name))
                                    if name.path == TAG_PATH =>
                                {
                                    match &name.lit {
                                        Lit::Str(s) => {
                                            name_opt = Some(FieldName::Renamed(
                                                s.value(),
                                                variant.ident.clone(),
                                            ));
                                        }
                                        _ => context
                                            .error_spanned_by(meta, "Expected string argument"),
                                    }
                                }
                                _ => context.error_spanned_by(meta, "Unknown attribute"),
                            });

                        let (compound_type, fields, manifest) =
                            parse_struct(context, &variant.fields);

                        EnumVariant {
                            name: name_opt
                                .unwrap_or_else(|| FieldName::Named(variant.ident.clone())),
                            compound_type,
                            fields,
                            manifest,
                        }
                    })
                    .collect();

                TypeContents::Enum(variants)
            }
            Data::Struct(data) => {
                let (compound_type, fields, manifest) = parse_struct(context, &data.fields);

                TypeContents::Struct(StructRepr {
                    compound_type,
                    fields,
                    manifest,
                })
            }
            Data::Union(_) => {
                context.error_spanned_by(input, "Unions are not supported");
                return None;
            }
        };

        Some(Self { type_contents })
    }
}

fn build_hcons(fields: &[Field]) -> TokenStream2 {
    fold_quote(
        quote!(crate::datastructures::hlist::HNil),
        fields.iter().map(|f| {
            let ty = &f.name;
            quote!(#ty)
        }),
        |item, result| quote! { crate::datastructures::hlist::HCons { head: #item, tail: #result }},
    )
}

fn compute_record(
    fields: &[Field],
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
                        un@ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            (headers, attrs, quote!(#items Item::ValueItem(#ident.as_value()),))
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
                        FieldName::Unnamed(_index) => {
                            // This has bene checked already when parsing the AST.
                            unreachable!()
                        },
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
                            un @ FieldName::Unnamed(_) => {
                                let ident = un.as_ident();
                                (quote!(#headers Item::ValueItem(#ident.as_value()),), attrs, items)
                            }
                        }
                    } else {
                        match name {
                            FieldName::Renamed(_, ident) | FieldName::Named(ident) => {
                                (quote!(, #ident.as_value()), attrs, items)
                            }
                            un@ FieldName::Unnamed(_) => {
                                let ident = un.as_ident();
                                (quote!(, #ident.as_value()), attrs, items)
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
                        un@ FieldName::Unnamed(_) => {
                            let ident = un.as_ident();
                            (quote!(#headers Item::ValueItem(#ident.as_value()),), attrs, items)
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
