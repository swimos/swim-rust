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

use macro_helpers::{CompoundType, FieldName};

use crate::parser::{FieldKind, FormDescriptor, StructRepr, TypeContents};
use crate::RecordTokenStreams;

pub fn from_value(
    type_contents: &TypeContents,
    structure_name: &Ident,
    descriptor: &FormDescriptor,
) -> TokenStream2 {
    match type_contents {
        TypeContents::Struct(repr) => {
            let streams = RecordTokenStreams::default();
            let structure_name_str = descriptor.name.tag_ident.to_string();

            let (field_opts, field_assignments) = repr.fields.iter().fold(
                (TokenStream2::new(), TokenStream2::new()),
                |(field_opts, field_assignments), f| {
                    let name = f.name.as_ident();
                    match &f.kind {
                        FieldKind::Skip => {
                            match &repr.compound_type {
                                CompoundType::Struct => {
                                    (field_opts, quote! {
                                        #field_assignments
                                       #name: std::default::Default::default(),
                                    })
                                }
                                _ => {
                                    (field_opts, quote! {
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

                            let field_assignment = match &repr.compound_type {
                                CompoundType::Struct => {
                                    let name_str = format!("Missing field: {}", name);

                                    quote! {
                                        #field_assignments
                                        #name : #name.ok_or(crate::form::FormErr::Message(String::from(#name_str)))?,
                                    }
                                }
                                _ => {
                                    quote! {
                                        #field_assignments
                                        #name.ok_or(crate::form::FormErr::Malformatted)?,
                                    }
                                }
                            };

                            (field_opts, field_assignment)
                        }
                    }
                },
            );

            let RecordTokenStreams {
                mut headers,
                mut attributes,
                mut items,
            } = parse_kinds(repr);

            if !items.is_empty() {
                items = quote! {
                    let mut items_iter = items.iter();
                    while let Some(item) = items_iter.next() {
                        match item {
                            #items
                            _ => return Err(crate::form::FormErr::Malformatted),
                        }
                    }
                };
            }

            let self_construction = match &repr.compound_type {
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

            quote! {
                match value {
                    crate::model::Value::Record(attrs, items) => match attrs.first() {
                        Some(attr) if &attr.name == #structure_name_str => {
                            #field_opts

                            #items

                            #self_construction
                        },
                        _ => return Err(crate::form::FormErr::MismatchedTag),
                    }
                    _ => return Err(crate::form::FormErr::Message(String::from("Expected record"))),
                }
            }
        }
        TypeContents::Enum(_variants) => TokenStream2::new(),
    }
}

fn parse_kinds(repr: &StructRepr) -> RecordTokenStreams {
    repr.fields
        .iter()
        .fold(RecordTokenStreams::default(), |mut streams, f| {
            let ty = &f.original.ty;

            match f.kind {
                FieldKind::Attr => {
                    match &f.name {
                        FieldName::Named(ident) => {}
                        FieldName::Renamed(_, _) => {}
                        FieldName::Unnamed(_) => {}
                    }
                }
                FieldKind::Skip => {}
                FieldKind::Slot => match &f.name {
                    FieldName::Named(ident) => {
                        let name_str = ident.to_string();

                        streams.transform_items(|items| {
                            quote! {
                                #items
                                crate::model::Item::Slot(crate::model::Value::Text(name), v) if name == #name_str => {
                                    #ident = std::option::Option::Some(crate::form::Form::try_from_value(v)?);
                                }
                            }
                        });
                    }
                    FieldName::Renamed(name, ident) => {
                        streams.transform_items(|items| {
                            quote! {
                                #items
                                crate::model::Item::Slot(crate::model::Value::Text(name), v) if name == #name => {
                                    #ident = std::option::Option::Some(crate::form::Form::try_from_value(v)?);
                                }
                            }
                        });
                    }
                    un @ FieldName::Unnamed(_) => {
                        let ident = un.as_ident();

                        streams.transform_items(|items| {
                            // todo: don't iterate over unnamed fields
                            quote! {
                                #items
                                crate::model::Item::ValueItem(v) if #ident.is_none() => {
                                    #ident = std::option::Option::Some(crate::form::Form::try_from_value(v)?);
                                }
                            }
                        });
                    }
                },
                _ => {}
            }
            streams
        })
}
