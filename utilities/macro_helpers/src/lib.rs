extern crate proc_macro;
extern crate proc_macro2;
#[allow(unused_imports)]
#[macro_use]
extern crate quote;
#[allow(unused_imports)]
#[macro_use]
extern crate syn;

use core::fmt;
use std::fmt::Display;

use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::{Data, Index, Meta, Path};

#[derive(Copy, Clone)]
pub struct Symbol(pub &'static str);

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
    Struct,
    Tuple,
    NewType,
    Unit,
}

/// An enumeration representing a field in a compound type. This enumeration helps to keep track of
/// fields that may have been renamed when transmuting it.
#[derive(Clone)]
pub enum FieldIdentity {
    /// A named field containing its identifier.
    Named(Ident),
    /// A renamed field containing its new identifier and original identifier. This field may have
    /// previously been named or anonymous.
    Renamed {
        new_identity: String,
        old_identity: Ident,
    },
    /// An anonymous field containing its index in the parent structure.
    Anonymous(Index),
}

impl FieldIdentity {
    /// Returns this [`FieldName`] represented as an [`Ident`]ifier. For renamed fields, this function
    /// returns the original field identifier represented and not the new name. For unnamed fields,
    /// this function returns a new identifier in the format of `__self_index`, where `index` is
    /// the ordinal of the field.
    pub fn as_ident(&self) -> Ident {
        match self {
            FieldIdentity::Named(ident) => ident.clone(),
            FieldIdentity::Renamed { old_identity, .. } => old_identity.clone(),
            FieldIdentity::Anonymous(index) => {
                Ident::new(&format!("__self_{}", index.index), index.span)
            }
        }
    }
}

impl ToString for FieldIdentity {
    fn to_string(&self) -> String {
        match self {
            FieldIdentity::Named(ident) => ident.to_string(),
            FieldIdentity::Renamed { new_identity, .. } => new_identity.to_string(),
            FieldIdentity::Anonymous(index) => format!("__self_{}", index.index),
        }
    }
}

impl ToTokens for FieldIdentity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            FieldIdentity::Named(ident) => ident.to_tokens(tokens),
            FieldIdentity::Renamed { old_identity, .. } => old_identity.to_tokens(tokens),
            FieldIdentity::Anonymous(index) => index.to_tokens(tokens),
        }
    }
}

/// An error context for building errors while parsing a token stream.
#[derive(Default)]
pub struct Context {
    errors: Vec<syn::Error>,
}

impl Context {
    /// Pushes an error into the context.
    pub fn error_spanned_by<A: ToTokens, T: Display>(&mut self, obj: A, msg: T) {
        self.errors
            .push(syn::Error::new_spanned(obj.into_token_stream(), msg));
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

/// Consumes a vector of errors and produces a compiler error.
pub fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

/// Deconstructs a structure or enumeration into its fields. For example:
/// ```
/// struct S {
///     a: i32,
///     b: i32
/// }
/// ```
///
/// Will produce the following:
/// ```compile_fail
/// { a, b }
/// ```
pub fn deconstruct_type(
    compound_type: &CompoundTypeKind,
    fields: &[&FieldIdentity],
) -> TokenStream2 {
    let fields: Vec<_> = fields
        .iter()
        .map(|name| match &name {
            FieldIdentity::Named(ident) => {
                quote! { #ident }
            }
            FieldIdentity::Renamed { old_identity, .. } => {
                quote! { #old_identity }
            }
            un @ FieldIdentity::Anonymous(_) => {
                let binding = &un.as_ident();
                quote! { #binding }
            }
        })
        .collect();

    match compound_type {
        CompoundTypeKind::Struct => quote! { { #(ref #fields,)* } },
        CompoundTypeKind::Tuple => quote! { ( #(ref #fields,)* ) },
        CompoundTypeKind::NewType => quote! { ( #(ref #fields,)* ) },
        CompoundTypeKind::Unit => quote!(),
    }
}

/// Returns a vector of metadata for the provided [`Attribute`] that matches the provided
/// [`Symbol`]. An error that is encountered is added to the [`Context`] and a [`Result::Err`] is
/// returned.
pub fn get_attribute_meta(
    ctx: &mut Context,
    attr: &syn::Attribute,
    symbol: Symbol,
) -> Result<Vec<syn::NestedMeta>, ()> {
    if attr.path != symbol {
        Ok(Vec::new())
    } else {
        match attr.parse_meta() {
            Ok(Meta::List(meta)) => Ok(meta.nested.into_iter().collect()),
            Ok(other) => {
                ctx.error_spanned_by(
                    other,
                    &format!("Invalid attribute. Expected #[{}(...)]", symbol),
                );
                Err(())
            }
            Err(e) => {
                ctx.error_spanned_by(attr, e.to_compile_error());
                Err(())
            }
        }
    }
}
