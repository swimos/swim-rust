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

use proc_macro2::Ident;
use syn::spanned::Spanned;

use macro_helpers::CompoundTypeKind;

use crate::form::form_parser::FormDescriptor;
use crate::parser::FieldManifest;
use macro_helpers::Label;
use macro_helpers::{EnumRepr, FieldKind, FormField, TypeContents};
use proc_macro2::TokenStream as TokenStream2;

pub fn from_value(
    type_contents: &TypeContents<FormDescriptor, FormField<'_>>,
    structure_name: &Ident,
    fn_factory: fn(TokenStream2) -> TokenStream2,
    into: bool,
) -> TokenStream2 {
    let maybe_mut_items = if into {
        quote!(mut items)
    } else {
        quote!(items)
    };

    match type_contents {
        TypeContents::Struct(repr) => {
            let descriptor = &repr.descriptor;
            let field_manifest = &descriptor.manifest;
            let (field_opts, field_assignments) = parse_fields(&repr.fields, &repr.compound_type);
            let (headers, header_body, items, attributes) =
                parse_elements(&repr.fields, field_manifest, fn_factory, into);

            let self_members = match &repr.compound_type {
                CompoundTypeKind::Labelled => {
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
                &descriptor.label,
                &headers,
                header_body,
                &attributes,
                into,
                &descriptor.manifest,
            );

            quote! {
                match value {
                    swim_common::model::Value::Record(attrs, #maybe_mut_items) => {
                        #field_opts
                        #attrs
                        #items
                        #self_members
                    }
                    _ => return Err(swim_common::form::FormErr::message("Expected record")),
                }
            }
        }
        TypeContents::Enum(EnumRepr { variants, .. }) => {
            let arms = variants.iter().fold(TokenStream2::new(), |ts, variant| {
                let original_ident = variant.name.original();
                let variant_name_str = variant.name.to_string();
                let (field_opts, field_assignments) =
                    parse_fields(&variant.fields, &variant.compound_type);

                let (headers, header_body, items, attributes) = parse_elements(
                    &variant.fields,
                    &variant.descriptor.manifest,
                    fn_factory,
                    into,
                );

                let self_members = match &variant.compound_type {
                    CompoundTypeKind::Labelled => {
                        quote! {{#field_assignments}}
                    }
                    CompoundTypeKind::Unit => quote!(),
                    _ => quote!((#field_assignments)),
                };

                let attrs = build_attr_quote(
                    &variant.name,
                    &headers,
                    header_body,
                    &attributes,
                    into,
                    &variant.descriptor.manifest,
                );

                quote! {
                    #ts
                    Some(swim_common::model::Attr { name, value }) if name == #variant_name_str => {
                        #field_opts
                        #attrs
                        #items
                        Ok(#structure_name::#original_ident#self_members)
                    },
                }
            });

            quote! {
                match value {
                    swim_common::model::Value::Record(attrs, #maybe_mut_items) => match attrs.first() {
                        #arms
                        _ => return Err(swim_common::form::FormErr::MismatchedTag),
                    }
                    _ => return Err(swim_common::form::FormErr::message("Expected record")),
                }
            }
        }
    }
}

fn build_attr_quote(
    tag: &Label,
    headers: &TokenStream2,
    mut header_body: TokenStream2,
    attributes: &TokenStream2,
    into: bool,
    manifest: &FieldManifest,
) -> TokenStream2 {
    if header_body.is_empty() {
        header_body = quote!(_ => return Err(swim_common::form::FormErr::MismatchedTag),);
    }

    let iterator = if into {
        quote!(into_iter)
    } else {
        quote!(iter)
    };

    let value_match_expr = if headers.is_empty() {
        quote! {
            match value {
                swim_common::model::Value::Record(_attrs, items) => {
                    if !items.is_empty() {
                        return Err(swim_common::form::FormErr::message(format!("Expected an empty record as header body")))
                    }
                }
                swim_common::model::Value::Extant => {},
                #header_body
            }
        }
    } else {
        quote! {
            match value {
                swim_common::model::Value::Record(_attrs, items) => {
                    let mut iter_items = items.#iterator();
                    while let Some(item) = iter_items.next() {
                        match item {
                            #headers
                            i => return Err(swim_common::form::FormErr::message(format!("Unexpected item in tag body: {:?}", i))),
                        }
                    }
                }
                swim_common::model::Value::Extant => {},
                #header_body
            }
        }
    };

    let name_check = match tag {
        Label::Foreign(new_ident, ty, _old_ident) => {
            let opt_name = Ident::new(
                &format!("__opt_{}", new_ident.to_string()),
                new_ident.span(),
            );

            quote! {
                let mut attr_it = attrs.#iterator().peekable();

                match attr_it.next() {
                    Some(swim_common::model::Attr { name, value })=> {
                        let __tag_name: #ty = swim_common::form::Tag::from_string(name.as_str().to_string()).map_err(|_| swim_common::form::FormErr::MismatchedTag)?;

                        #opt_name = Some(__tag_name);

                        #value_match_expr
                    },
                    None=> {
                        return Err(swim_common::form::FormErr::MismatchedTag);
                    }
                }
            }
        }
        _ => {
            quote! {
                let mut attr_it = attrs.#iterator().peekable();

                match attr_it.next() {
                    Some(swim_common::model::Attr { name, value }) => {
                        #value_match_expr
                    },
                    _ => {
                        return Err(swim_common::form::FormErr::MismatchedTag);
                    }
                }
            }
        }
    };
    if manifest.replaces_body {
        quote! {
            #name_check
            #attributes
        }
    } else if attributes.is_empty() {
        quote! {
            #name_check

            if let Some(_) = attr_it.next() {
                return Err(swim_common::form::FormErr::Malformatted);
            }
        }
    } else {
        quote! {
            #name_check

            while let Some(swim_common::model::Attr { name, value }) = attr_it.next() {
                match name.as_str() {
                    #attributes
                    _ => return Err(swim_common::form::FormErr::Malformatted),
                }
            }
        }
    }
}

fn parse_fields(
    fields: &[FormField],
    compound_type: &CompoundTypeKind,
) -> (TokenStream2, TokenStream2) {
    fields.iter().fold(
        (TokenStream2::new(), TokenStream2::new()),
        |(field_opts, field_assignments), f| {
            let ident = f.label.as_ident();
            let name = f.label.as_ident().to_string();
            let opt_name = Ident::new(&format!("__opt_{}", name), f.original.span());

            match &f.kind {
                FieldKind::Skip => {
                    match compound_type {
                        CompoundTypeKind::Labelled => {
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
                        CompoundTypeKind::Labelled => {
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
    fields: &[FormField],
    field_manifest: &FieldManifest,
    fn_factory: fn(TokenStream2) -> TokenStream2,
    into: bool,
) -> (TokenStream2, TokenStream2, TokenStream2, TokenStream2) {
    let (headers, header_body, mut items, attributes) = fields
        .iter()
        .filter(|f| f.kind != FieldKind::Skip)
        .fold((TokenStream2::new(), TokenStream2::new(), TokenStream2::new(), TokenStream2::new()), |(mut headers, mut header_body, mut items, mut attrs), f| {
            let name = f.label.as_ident();
            let ident = Ident::new(&format!("__opt_{}", name), f.original.span());

            match f.kind {
                FieldKind::Attr => {
                    let name_str = f.label.to_string();
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

                    match &f.label {
                        Label::Anonymous(_) => {
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
                        let field_name_str = f.label.to_string();

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
                    attrs = if into {
                        quote !{
                            #attrs
                            let has_more = attr_it.peek().is_some();

                            let update_value = if !has_more && items.len() < 2 {
                                match items.pop() {
                                    Some(swim_common::model::Item::ValueItem(single)) => single,
                                    _ => swim_common::model::Value::record(items),
                                }
                            } else {
                                swim_common::model::Value::Record(attr_it.collect(), items)
                            };

                            #ident = std::option::Option::Some(swim_common::form::Form::try_convert(update_value)?);
                        }
                    } else {
                        quote! {
                            #attrs
                            let has_more = attr_it.peek().is_some();

                            let update_value = if !has_more && items.len() < 2 {
                                match items.first() {
                                    Some(swim_common::model::Item::ValueItem(single)) => single.clone(),
                                    _ => swim_common::model::Value::record(items.clone()),
                                }
                            } else {
                                swim_common::model::Value::Record(attr_it.cloned().collect(), items.clone())
                            };

                            #ident = std::option::Option::Some(swim_common::form::Form::try_convert(update_value)?);
                        }
                    };
                }
                FieldKind::Tagged => {
                    // no-op as the field is ignored and marked as a tag 
                },
                _ => {
                    let fn_call = fn_factory(quote!(v));

                    match &f.label {
                        Label::Anonymous(_) => {
                            headers = quote! {
                                #headers
                                swim_common::model::Item::ValueItem(v) if #ident.is_none() => {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                            };
                        }
                        fi => {
                            let ident_str = fi.to_string();
                            headers = quote! {
                                #headers
                                swim_common::model::Item::Slot(swim_common::model::Value::Text(name), v) if name == #ident_str => {
                                    #ident = std::option::Option::Some(#fn_call?);
                                }
                            };
                        }
                    }
                }
            }
            (headers, header_body, items, attrs)
        });

    if !field_manifest.replaces_body && !items.is_empty() {
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
