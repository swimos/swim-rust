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

use proc_macro2::Ident;
use syn::export::TokenStream2;
use syn::spanned::Spanned;

use macro_helpers::{CompoundTypeKind, FieldIdentity};

use crate::parser::{Field, FieldKind, FieldManifest, FormDescriptor, TypeContents};

pub fn from_value(
    type_contents: &TypeContents,
    structure_name: &Ident,
    descriptor: &FormDescriptor,
    fn_factory: fn(TokenStream2) -> TokenStream2,
    into: bool,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => {
            let structure_name_str = descriptor.name.tag_ident.to_string();
            let field_manifest = &repr.manifest;
            let (field_opts, field_assignments) = parse_fields(&repr.fields, &repr.compound_type);
            let (headers, header_body, items, attributes) =
                parse_elements(&repr.fields, field_manifest, fn_factory, into);

            let self_members = match &repr.compound_type {
                CompoundTypeKind::Struct => {
                    quote! {
                        Ok(#structure_name {
                            #field_assignments
                        })
                    }
                }
                CompoundTypeKind::Unit => quote!(Ok(#structure_name)),
                _ => quote!(Ok(#structure_name(#field_assignments))),
            };

            let attrs = build_attr_quote(
                &structure_name_str,
                &headers,
                header_body,
                &attributes,
                into,
            );

            quote! {
                match value {
                    swim_common::model::Value::Record(attrs, items) => {
                        #field_opts
                        #attrs
                        #items
                        #self_members
                    }
                    _ => return Err(swim_common::form::FormErr::Message(String::from("Expected record"))),
                }
            }
        }
        TypeContents::Enum(variants) => {
            let arms = variants.iter().fold(TokenStream2::new(), |ts, variant| {
                let variant_name_str = variant.name.to_string();
                let variant_ident = variant.name.as_ident();
                let (field_opts, field_assignments) =
                    parse_fields(&variant.fields, &variant.compound_type);

                let (headers, header_body, items, attributes) =
                    parse_elements(&variant.fields, &variant.manifest, fn_factory, into);

                let self_members = match &variant.compound_type {
                    CompoundTypeKind::Struct => {
                        quote! {{#field_assignments}}
                    }
                    CompoundTypeKind::Unit => quote!(),
                    _ => quote!((#field_assignments)),
                };

                let attrs =
                    build_attr_quote(&variant_name_str, &headers, header_body, &attributes, into);

                quote! {
                    #ts
                    Some(swim_common::model::Attr { name, value }) if name == #variant_name_str => {
                        #field_opts
                        #attrs
                        #items
                        Ok(#structure_name::#variant_ident#self_members)
                    },
                }
            });

            quote! {
                match value {
                    swim_common::model::Value::Record(attrs, items) => match attrs.first() {
                        #arms
                        _ => return Err(swim_common::form::FormErr::MismatchedTag),
                    }
                    _ => return Err(swim_common::form::FormErr::Message(String::from("Expected record"))),
                }
            }
        }
    }
}

fn build_attr_quote(
    name_str: &str,
    headers: &TokenStream2,
    mut header_body: TokenStream2,
    attributes: &TokenStream2,
    into: bool,
) -> TokenStream2 {
    if header_body.is_empty() {
        header_body = quote!(_ => return Err(swim_common::form::FormErr::Malformatted),);
    }

    if into {
        quote! {
            let mut attr_it = attrs.into_iter();
            while let Some(Attr { name, value }) = attr_it.next() {
                match name.as_ref() {
                     #name_str => match value {
                        swim_common::model::Value::Record(_attrs, items) => {
                            let mut iter_items = items.into_iter();
                            while let Some(item) = iter_items.next() {
                                match item {
                                    #headers
                                    i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item in tag body: {:?}", i))),
                                }
                            }
                        }
                        swim_common::model::Value::Extant => {},
                        #header_body
                    },
                    #attributes
                    _ => return Err(swim_common::form::FormErr::MismatchedTag),
                }
            }
        }
    } else {
        quote! {
            let mut attr_it = attrs.iter();
            while let Some(Attr { name, ref value }) = attr_it.next() {
                match name.as_ref() {
                     #name_str => match value {
                        swim_common::model::Value::Record(_attrs, items) => {
                            let mut iter_items = items.iter();
                            while let Some(item) = iter_items.next() {
                                match item {
                                    #headers
                                    i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item in tag body: {:?}", i))),
                                }
                            }
                        }
                        swim_common::model::Value::Extant => {},
                        #header_body
                    },
                    #attributes
                    _ => return Err(swim_common::form::FormErr::MismatchedTag),
                }
            }
        }
    }
}

fn parse_fields(
    fields: &[Field],
    compound_type: &CompoundTypeKind,
) -> (TokenStream2, TokenStream2) {
    fields.iter().fold(
        (TokenStream2::new(), TokenStream2::new()),
        |(field_opts, field_assignments), f| {
            let ident = f.name.as_ident();
            let name = f.name.as_ident().to_string();
            let opt_name = Ident::new(&format!("__opt_{}", name), f.original.span());

            match &f.kind {
                FieldKind::Skip => {
                    match compound_type {
                        CompoundTypeKind::Struct => {
                            (field_opts,
                             quote! {
                                #field_assignments
                                #ident: std::default::Default::default(),
                            })
                        }
                        _ => {
                            (field_opts,
                             quote! {
                                #field_assignments
                                std::default::Default::default(),
                            })
                        }
                    }
                }
                _ => {
                    let ty = &f.original.ty;
                    let field_opts = quote! {
                        #field_opts
                        let mut #opt_name: std::option::Option<#ty> = None;
                    };

                    let field_assignment = match compound_type {
                        CompoundTypeKind::Struct => {
                            let name_str = format!("Missing field: {}", name);

                            quote! {
                                #field_assignments
                                #ident: #opt_name.ok_or(swim_common::form::FormErr::Message(String::from(#name_str)))?,
                            }
                        }
                        _ => {
                            quote! {
                                #field_assignments
                                #opt_name.ok_or(swim_common::form::FormErr::Malformatted)?,
                            }
                        }
                    };

                    (field_opts, field_assignment)
                }
            }
        },
    )
}

fn parse_elements(
    fields: &[Field],
    field_manifest: &FieldManifest,
    fn_factory: fn(TokenStream2) -> TokenStream2,
    into: bool,
) -> (TokenStream2, TokenStream2, TokenStream2, TokenStream2) {
    let (headers, header_body, mut items, attributes) = fields
        .iter()
        .filter(|f| f.kind != FieldKind::Skip)
        .fold((TokenStream2::new(), TokenStream2::new(), TokenStream2::new(), TokenStream2::new()), |(mut headers, mut header_body, mut items, mut attrs), f| {
            let name = f.name.as_ident();
            let ident = Ident::new(&format!("__opt_{}", name), f.original.span());

            match f.kind {
                FieldKind::Attr => {
                    let name_str = f.name.to_string();
                    let fn_call = fn_factory(quote!(value));

                    attrs = quote! {
                        #attrs

                        #name_str => {
                            if #ident.is_some() {
                                return Err(swim_common::form::FormErr::DuplicateField(String::from(#name_str)));
                            } else {
                                #ident = std::option::Option::Some(#fn_call?);
                            }
                        }
                    };
                }
                FieldKind::Slot if !field_manifest.replaces_body => {
                    let mut build_named_ident = |name_str, ident| {
                        let fn_call = fn_factory(quote!(v));

                        items = quote! {
                            #items
                            swim_common::model::Item::Slot(swim_common::model::Value::Text(name), v) if name == #name_str => {
                                if #ident.is_some() {
                                    return Err(swim_common::form::FormErr::DuplicateField(String::from(#name_str)));
                                } else {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                            }
                        }
                    };

                    match &f.name {
                        FieldIdentity::Anonymous(_) => {
                            let fn_call = fn_factory(quote!(v));

                            items = quote! {
                                #items
                                swim_common::model::Item::ValueItem(v) if #ident.is_none() => {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                            };
                        }
                        fi => {
                            build_named_ident(fi.to_string(), ident);
                        }
                    }
                }
                FieldKind::HeaderBody => {
                    let fn_call = fn_factory(quote!(v));

                    if field_manifest.has_header_fields {
                        let field_name_str = f.name.to_string();

                        headers = quote! {
                            swim_common::model::Item::ValueItem(v) => {
                                if #ident.is_some() {
                                    return Err(swim_common::form::FormErr::DuplicateField(String::from(#field_name_str)));
                                } else {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                            }
                        };
                    } else {
                        header_body = quote! {
                            v => {
                                #ident = std::option::Option::Some(#fn_call?);
                            }
                        };
                    }
                }
                FieldKind::Body => {
                    let fn_call = if into {
                        fn_factory(quote!(rec))
                    } else {
                        fn_factory(quote!(&rec))
                    };

                    items = quote! {
                        #ident = {
                            let rec = swim_common::model::Value::Record(Vec::new(), items.to_vec());
                            std::option::Option::Some(#fn_call?)
                        };
                    };
                }
                _ => {
                    let fn_call = fn_factory(quote!(v));

                    match &f.name {
                        FieldIdentity::Anonymous(_) => {
                            headers = quote! {
                                swim_common::model::Item::ValueItem(v) => {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                                #headers
                            };
                        }
                        fi => {
                            let ident_str = fi.to_string();
                            headers = quote! {
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), v) if name == #ident_str => {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                                #headers
                            };
                        }
                    }
                }
            }
            (headers, header_body, items, attrs)
        });

    if !items.is_empty() && !field_manifest.replaces_body {
        if into {
            items = quote! {
                let mut items_iter = items.into_iter();
                while let Some(item) = items_iter.next() {
                    match item {
                        #items
                        i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item: {:?}", i))),
                    }
                }
            };
        } else {
            items = quote! {
                let mut items_iter = items.iter();
                while let Some(item) = items_iter.next() {
                    match item {
                        #items
                        i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item: {:?}", i))),
                    }
                }
            };
        }
    }

    (headers, header_body, items, attributes)
}
