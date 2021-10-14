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

use crate::parse::{parse_attr_input, stringify_attributes};
use macro_utilities::Context;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use quote::ToTokens;
use syn::visit_mut::VisitMut;
use syn::{DataEnum, DataStruct, DataUnion, Field, Meta, NestedMeta, Path, Variant};

const ERR_UNION: &str = "Unions are not supported";

pub struct Visitor {
    pub context: Context,
}

impl Visitor {
    pub fn new(context: Context) -> Visitor {
        Visitor { context }
    }
}

impl VisitMut for Visitor {
    fn visit_data_enum_mut(&mut self, data: &mut DataEnum) {
        data.variants
            .iter_mut()
            .for_each(|v| self.visit_variant_mut(v));
    }

    fn visit_data_struct_mut(&mut self, data: &mut DataStruct) {
        self.visit_fields_mut(&mut data.fields)
    }

    fn visit_data_union_mut(&mut self, data: &mut DataUnion) {
        self.context.error_spanned_by(data.union_token, ERR_UNION);
    }

    fn visit_field_mut(&mut self, field: &mut Field) {
        stringify_attributes(&mut self.context, &mut field.attrs);
    }

    fn visit_variant_mut(&mut self, variant: &mut Variant) {
        let Variant { attrs, fields, .. } = variant;

        self.visit_fields_mut(fields);
        stringify_attributes(&mut self.context, attrs);
    }
}

/// An attribute from the derive input.
///
/// ```compile_fail
/// #[stringify(serde(rename(A)))]
///             ^^^^^
///             Path    
///                   ^^^^^^^^^
///                     Input
/// ```
pub struct Attr {
    /// The path of this attribute.
    path: Path,
    /// The attribute's token tree.
    input: Vec<Meta>,
}

/// A pre-processed outer attribute from the derive input. Either representing the format
/// `#[stringify(...)]` or `#[stringify_raw(...)]`.
pub enum Preprocessed {
    /// The attribute was written as `#[stringify(...)]`
    ///
    /// ```compile_fail
    /// #[stringify(serde(rename(A)))]
    ///             ^^^^^
    ///             path    
    ///                   ^^^^^^^^^
    ///                   stringify
    /// ```
    StringifyAll {
        /// The path of the target attribute.
        path: Path,
        /// The token tree to be stringified.
        stringify: Vec<NestedMeta>,
    },
    /// The attribute was written as `#[stringify_raw(...)]`.
    ///
    /// ```compile_fail
    /// #[stringify_raw(path = "serde", in(rename(A)), raw(alias = "B"))]
    ///                 ^^^^^           ^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^
    ///                 path              stringify          raw
    /// ```
    StringifyPartial {
        /// The path of the target attribute.
        path: Path,
        /// The token tree to be stringified.
        stringify: Vec<NestedMeta>,
        /// A raw token tree to be written to the output token stream.
        raw: TokenStream2,
    },
}

impl Preprocessed {
    /// Folds the nested meta in `meta` and attempts to parse it into an `Attr`ibute which can
    /// then be stringified.
    fn parse_attr(ctx: &mut Context, path: Path, meta: Vec<NestedMeta>) -> Result<Attr, ()> {
        let seed = Vec::with_capacity(meta.len());
        let items = meta.into_iter().try_fold(seed, |mut output, nested| {
            let attrs = parse_attr_input(ctx, nested)?;
            output.extend(attrs);

            Ok(output)
        })?;

        Ok(Attr { path, input: items })
    }

    /// Consumes this `Preprocessed` instance and attempts to process all of the nested metadata
    /// into valid attributes.
    ///
    /// Returns a result containing a `Processed` attribute and path which can then be written to
    /// the output `TokenStream` or an error if this `Preprocessed` instance is an invlaid format.
    pub fn parse(self, ctx: &mut Context) -> Result<Processed, ()> {
        match self {
            Preprocessed::StringifyAll { path, stringify } => {
                let processed = Preprocessed::parse_attr(ctx, path, stringify)?;
                Ok(Processed::StringifyAll(processed))
            }
            Preprocessed::StringifyPartial {
                path,
                stringify,
                raw,
            } => {
                let processed = Preprocessed::parse_attr(ctx, path, stringify)?;
                Ok(Processed::StringifyPartial {
                    stringify: processed,
                    raw,
                })
            }
        }
    }
}

/// A processed attribute which can be written directly to a token stream.
pub enum Processed {
    /// The attribute was written as `#[stringify(...)]`
    StringifyAll(Attr),
    /// The attribute was written as `#[stringify_raw(...)]`.
    StringifyPartial {
        /// The `in(...)` part of the attribute.
        stringify: Attr,
        /// The `raw(...)` part of the attribute.
        raw: TokenStream2,
    },
}

impl ToTokens for Processed {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        let output = match self {
            Processed::StringifyAll(Attr { path, input }) => {
                quote!(#[#path(#(#input),*)])
            }
            Processed::StringifyPartial { stringify, raw } => {
                let Attr { path, input } = stringify;
                quote!(#[#path(#(#input),*, #raw)])
            }
        };
        output.to_tokens(tokens);
    }
}
