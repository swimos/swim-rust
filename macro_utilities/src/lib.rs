// Copyright 2015-2021 Swim Inc.
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

#![allow(clippy::result_unit_err)]

mod form;
mod generics;
mod label;
mod utilities;

pub use form::*;
pub use generics::*;
pub use label::Label;
pub use utilities::*;

extern crate proc_macro;
extern crate proc_macro2;

#[allow(unused_imports)]
#[macro_use]
extern crate quote;

#[allow(unused_imports)]
#[macro_use]
extern crate syn;

use core::fmt;
use std::fmt::{Debug, Display};

use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use syn::{NestedMeta, Path};

use syn::{Attribute, Data};
pub use utilities::*;

#[derive(Copy, Clone)]
pub struct Symbol(pub &'static str);

impl ToTokens for Symbol {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Symbol(inner) = self;
        let quote = quote!(#inner);
        quote.to_tokens(tokens);
    }
}

impl PartialEq<Symbol> for Ident {
    fn eq(&self, symbol: &Symbol) -> bool {
        self == symbol.0
    }
}

impl<'a> PartialEq<Symbol> for &'a Ident {
    fn eq(&self, symbol: &Symbol) -> bool {
        *self == symbol.0
    }
}

impl PartialEq<Symbol> for Path {
    fn eq(&self, symbol: &Symbol) -> bool {
        self.is_ident(symbol.0)
    }
}

impl<'a> PartialEq<Symbol> for &'a Path {
    fn eq(&self, symbol: &Symbol) -> bool {
        self.is_ident(symbol.0)
    }
}

impl<'a> PartialEq<Symbol> for &'a str {
    fn eq(&self, other: &Symbol) -> bool {
        self == &other.0
    }
}

impl<'a> PartialEq<Symbol> for String {
    fn eq(&self, other: &Symbol) -> bool {
        self == other.0
    }
}

impl Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Eq, Ord, PartialOrd)]
pub enum StructureKind {
    Enum,
    Union,
    Struct,
}

impl StructureKind {
    pub fn is_struct(&self) -> bool {
        matches!(self, StructureKind::Struct)
    }
    pub fn is_enum(&self) -> bool {
        matches!(self, StructureKind::Enum)
    }
    pub fn is_union(&self) -> bool {
        matches!(self, StructureKind::Union)
    }
}

impl From<&syn::Data> for StructureKind {
    fn from(data: &Data) -> Self {
        match &data {
            Data::Enum(_) => StructureKind::Enum,
            Data::Struct(_) => StructureKind::Struct,
            Data::Union(_) => StructureKind::Union,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum CompoundTypeKind {
    Labelled,
    Tuple,
    NewType,
    Unit,
}

/// An error context for building errors while parsing a token stream.
#[derive(Default)]
pub struct Context {
    errors: Vec<syn::Error>,
}

impl Context {
    /// Pushes an error into the context.
    pub fn error_spanned_by<A: ToTokens, T: Display>(&mut self, location: A, msg: T) {
        self.errors
            .push(syn::Error::new_spanned(location.into_token_stream(), msg));
    }

    /// Consumes the context and returns the underlying errors.
    pub fn check(self) -> Result<(), Vec<syn::Error>> {
        let errors = self.errors;
        match errors.len() {
            0 => Ok(()),
            _ => Err(errors),
        }
    }
}

/// A trait for retrieving attributes on a field or compound type that are prefixed by the provided
/// `symbol`. For example calling this on a [`syn::DeriveInput`] that represents the following:
///```compile_fail
///struct Person {
///    #[form(skip)]
///    name: String,
///    age: i32,
/// }
///```
/// will return a [`Vec`] that contains the [`NestedMeta`] for the field.
pub trait Attributes {
    /// Returns a vector of [`NestedMeta`] for all attributes that contain a path that matches the
    /// provided symbol or an empty vector if there are no matches.
    fn get_attributes(&self, ctx: &mut Context, symbol: Symbol) -> Vec<NestedMeta>;
}

impl Attributes for Vec<Attribute> {
    fn get_attributes(&self, ctx: &mut Context, symbol: Symbol) -> Vec<NestedMeta> {
        self.iter()
            .flat_map(|a| get_attribute_meta(ctx, a, symbol))
            .flatten()
            .collect()
    }
}

pub trait SynOriginal {
    fn original(&self) -> &syn::Field;
}

pub fn str_to_ident(s: &str) -> Ident {
    Ident::new(s, Span::call_site())
}

pub fn string_to_ident(s: String) -> Ident {
    str_to_ident(&s)
}

pub fn as_const(trait_name: &str, typ: Ident, exec: TokenStream) -> TokenStream {
    let const_name = format_ident!(
        "__IMPL_{}__FOR__{}__",
        trait_name,
        typ.to_string().trim_start_matches("r#").to_string()
    );

    quote! {
        #[doc(hidden)]
        const #const_name: () = {
            #exec
        };
    }
}
