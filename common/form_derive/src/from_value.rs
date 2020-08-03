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

use macro_helpers::{CompoundType, FieldName};

use crate::parser::{
    Field, FieldKind, FieldManifest, FormDescriptor, RecordTokenStreams, TypeContents,
};

pub fn from_value(
    type_contents: &TypeContents,
    structure_name: &Ident,
    descriptor: &FormDescriptor,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => {
            let structure_name_str = descriptor.name.tag_ident.to_string();
            let field_manifest = &repr.manifest;
            let (field_opts, field_assignments) = parse_fields(&repr.fields, &repr.compound_type);
            let RecordTokenStreams {
                headers,
                attributes,
                items,
            } = parse_elements(&repr.fields, field_manifest);

            let self_members = match &repr.compound_type {
                CompoundType::Struct => {
                    quote! {
                        Ok(#structure_name {
                            #field_assignments
                        })
                    }
                }
                CompoundType::Unit => quote!(Ok(#structure_name)),
                _ => quote!(Ok(#structure_name(#field_assignments))),
            };

            let attrs = build_attr_quote(&structure_name_str, &headers, &attributes);

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

                let RecordTokenStreams {
                    headers,
                    attributes,
                    items,
                } = parse_elements(&variant.fields, &variant.manifest);

                let self_members = match &variant.compound_type {
                    CompoundType::Struct => {
                        quote! {{#field_assignments}}
                    }
                    CompoundType::Unit => quote!(),
                    _ => quote!((#field_assignments)),
                };

                let attrs = build_attr_quote(&variant_name_str, &headers, &attributes);

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
    attributes: &TokenStream2,
) -> TokenStream2 {
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
                                i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item: {:?}", i))),
                            }
                        }
                    }
                    swim_common::model::Value::Extant => {},
                    _ => return Err(swim_common::form::FormErr::Malformatted),
                },
                #attributes
                _ => return Err(swim_common::form::FormErr::MismatchedTag),
            }
        }
    }
}

fn parse_fields(fields: &[Field], compound_type: &CompoundType) -> (TokenStream2, TokenStream2) {
    fields.iter().fold(
        (TokenStream2::new(), TokenStream2::new()),
        |(field_opts, field_assignments), f| {
            let ident = f.name.as_ident();
            let name = f.name.as_ident().to_string();
            let name = Ident::new(&format!("__opt_{}", name), f.original.span());

            match &f.kind {
                FieldKind::Skip => {
                    match compound_type {
                        CompoundType::Struct => {
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
                        let mut #name: std::option::Option<#ty> = None;
                    };

                    let field_assignment = match compound_type {
                        CompoundType::Struct => {
                            let name_str = format!("Missing field: {}", name);

                            quote! {
                                #field_assignments
                                #ident: #name.ok_or(swim_common::form::FormErr::Message(String::from(#name_str)))?,
                            }
                        }
                        _ => {
                            quote! {
                                #field_assignments
                                #name.ok_or(swim_common::form::FormErr::Malformatted)?,
                            }
                        }
                    };

                    (field_opts, field_assignment)
                }
            }
        },
    )
}

fn parse_elements(fields: &[Field], field_manifest: &FieldManifest) -> RecordTokenStreams {
    let mut streams = fields
        .iter()
        .fold(RecordTokenStreams::default(), |mut streams, f| {
            let name = f.name.as_ident();
            let ident = Ident::new(&format!("__opt_{}", name), f.original.span());

            match f.kind {
                FieldKind::Attr => {
                    // Unnamed fields won't compile so there's no need in checking the name variant
                    let name_str = f.name.to_string();

                    streams.transform_attrs(|attrs| quote! {
                        #attrs

                         #name_str => {
                            if #ident.is_some() {
                                return Err(swim_common::form::FormErr::DuplicateField(String::from(#name_str)));
                            } else {
                                #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(value)?);
                            }
                        }
                    });
                }
                FieldKind::Slot if !field_manifest.replaces_body => {
                    let mut build_named_ident = |name_str, ident| {
                        streams.transform_items(|items| {
                            quote! {
                                #items
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), v) if name == #name_str => {
                                    if #ident.is_some() {
                                        return Err(swim_common::form::FormErr::DuplicateField(String::from(#name_str)));
                                    } else {
                                        #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                    }
                                }
                            }
                        });
                    };

                    match &f.name {
                        FieldName::Named(name) => {
                            build_named_ident(name.to_string(), ident);
                        }
                        FieldName::Renamed(name, _) => {
                            build_named_ident(name.to_string(), ident);
                        }
                        FieldName::Unnamed(_) => {

                            // todo: remove from iterator block to remove option checks.
                            streams.transform_items(|items| {
                                quote! {
                                    #items
                                    swim_common::model::Item::ValueItem(v) if #ident.is_none() => {
                                        #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                    }
                                }
                            });
                        }
                    }
                }
                FieldKind::HeaderBody => {
                    let field_name_str = f.name.to_string();

                    streams.transform_headers(|_headers|
                        quote! {
                            swim_common::model::Item::ValueItem(v) => {
                                if #ident.is_some() {
                                    return Err(swim_common::form::FormErr::DuplicateField(String::from(#field_name_str)));
                                } else {
                                    #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                }
                            }
                    });
                }
                FieldKind::Body => {
                    streams.transform_items(|_items| quote! {
                        #ident = {
                            let rec = swim_common::model::Value::Record(Vec::new(), items.to_vec());
                            std::option::Option::Some(swim_common::form::Form::try_from_value(&rec)?)
                        };
                    })
                }
                FieldKind::Skip => {}
                _ => {
                    // Unnamed fields won't compile so there's no need in checking the name variant
                    let field_name_str = f.name.to_string();

                    match &f.name {
                        FieldName::Renamed(name, _) => {
                            streams.transform_headers(|headers| quote! {
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), ref v) if name == #name => {
                                    #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                }
                                #headers
                            });
                        }
                        FieldName::Named(field_ident) => {
                            let ident_str = field_ident.to_string();
                            streams.transform_headers(|headers| quote! {
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), ref v) if name == #ident_str => {
                                    #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                }
                                #headers
                            });
                        }
                         FieldName::Unnamed(_) => {
                            streams.transform_headers(|headers| quote! {
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), ref v) if name == #field_name_str => {
                                    #ident = std::option::Option::Some(swim_common::form::Form::try_from_value(v)?);
                                }
                                #headers
                            });
                        }
                    }
                }
            }
            streams
        });

    if !streams.items.is_empty() && !field_manifest.replaces_body {
        streams.transform_items(|items| {
            quote! {
                let mut items_iter = items.iter();
                while let Some(item) = items_iter.next() {
                    match item {
                        #items
                        i => return Err(swim_common::form::FormErr::Message(format!("Unexpected item: {:?}", i))),
                    }
                }
            }
        });
    }

    streams
}
