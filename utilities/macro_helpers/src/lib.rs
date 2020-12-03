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

use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::export::{Formatter, TokenStream2};
use syn::{Attribute, Data, ExprPath, Index, Lit, Meta, Path};

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
    Struct,
    Tuple,
    NewType,
    Unit,
}

/// An enumeration representing a field or a compound type. This enumeration helps to keep track of
/// elements that may have been renamed when transmuting.
#[derive(Clone)]
pub enum Label {
    /// A named element containing its original identifier as written in source.
    Unmodified(Ident),
    /// A renamed element containing its new identifier and original identifier. This element may
    /// have previously been named or anonymous.
    Renamed { new_label: String, old_label: Ident },
    /// An anonymous element containing its index in the parent structure.
    Anonymous(Index),
    /// A field that will be used to get the label, along with that field's type.
    Foreign(Ident, TokenStream2, Ident),
}

impl Debug for Label {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Label {
    pub fn is_foreign(&self) -> bool {
        matches!(self, Label::Foreign(_, _, _))
    }

    pub fn is_modified(&self) -> bool {
        !matches!(self, Label::Unmodified(_))
    }

    /// Returns this `Label` represented as an `Ident`ifier. For renamed fields, this function
    /// returns the original field identifier represented and not the new name. For unnamed fields,
    /// this function returns a new identifier in the format of `__self_index`, where `index` is
    /// the ordinal of the field.
    pub fn as_ident(&self) -> Ident {
        match self {
            Label::Unmodified(ident) => ident.clone(),
            Label::Renamed { old_label, .. } => old_label.clone(),
            Label::Anonymous(index) => Ident::new(&format!("__self_{}", index.index), index.span),
            Label::Foreign(ident, ..) => ident.clone(),
        }
    }

    pub fn original(&self) -> Ident {
        match self {
            Label::Unmodified(ident) => ident.clone(),
            Label::Renamed { old_label, .. } => old_label.clone(),
            Label::Anonymous(index) => Ident::new(&format!("__self_{}", index.index), index.span),
            Label::Foreign(_ident, _ts, original) => original.clone(),
        }
    }

    pub fn to_name(&self, clone: bool) -> TokenStream2 {
        match self {
            Label::Unmodified(ident) => {
                let name = ident.to_string();
                quote!(#name)
            }
            Label::Renamed { new_label, .. } => {
                let name = new_label.to_string();
                quote!(#name)
            }
            Label::Anonymous(index) => {
                let name = format!("__self_{}", index.index);
                quote!(#name)
            }
            Label::Foreign(ident, ..) => {
                let maybe_clone = if clone { quote!(.clone()) } else { quote!() };
                quote!(swim_common::form::Tag::as_string(&#ident#maybe_clone))
            }
        }
    }
}

impl ToString for Label {
    fn to_string(&self) -> String {
        match self {
            Label::Unmodified(ident) => ident.to_string(),
            Label::Renamed { new_label, .. } => new_label.to_string(),
            Label::Anonymous(index) => format!("__self_{}", index.index),
            Label::Foreign(ident, ..) => ident.to_string(),
        }
    }
}

impl ToTokens for Label {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            Label::Unmodified(ident) => ident.to_tokens(tokens),
            Label::Renamed { old_label, .. } => old_label.to_tokens(tokens),
            Label::Anonymous(index) => index.to_tokens(tokens),
            Label::Foreign(ident, ..) => ident.to_tokens(tokens),
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
    fields: &[&Label],
    as_ref: bool,
) -> TokenStream2 {
    let fields: Vec<_> = fields
        .iter()
        .map(|name| match &name {
            Label::Unmodified(ident) => {
                quote! { #ident }
            }
            Label::Renamed { old_label, .. } => {
                quote! { #old_label }
            }
            Label::Foreign(ident, ..) => {
                quote! { #ident }
            }
            un @ Label::Anonymous(_) => {
                let binding = &un.as_ident();
                quote! { #binding }
            }
        })
        .collect();

    if as_ref {
        match compound_type {
            CompoundTypeKind::Struct => quote! { { #(ref #fields,)* } },
            CompoundTypeKind::Tuple => quote! { ( #(ref #fields,)* ) },
            CompoundTypeKind::NewType => quote! { ( #(ref #fields,)* ) },
            CompoundTypeKind::Unit => quote!(),
        }
    } else {
        match compound_type {
            CompoundTypeKind::Struct => quote! { { #(#fields,)* } },
            CompoundTypeKind::Tuple => quote! { ( #(#fields,)* ) },
            CompoundTypeKind::NewType => quote! { ( #(#fields,)* ) },
            CompoundTypeKind::Unit => quote!(),
        }
    }
}

/// Returns a vector of metadata for the provided `Attribute` that matches the provided
/// [`Symbol`]. An error that is encountered is added to the [`Context`] and a [`Result::Err`] is
/// returned.
pub fn get_attribute_meta(
    ctx: &mut Context,
    attr: &Attribute,
    path: Symbol,
) -> Result<Vec<syn::NestedMeta>, ()> {
    if attr.path != path {
        Ok(Vec::new())
    } else {
        match attr.parse_meta() {
            Ok(Meta::List(meta)) => Ok(meta.nested.into_iter().collect()),
            Ok(other) => {
                ctx.error_spanned_by(
                    other,
                    &format!("Invalid attribute. Expected #[{}(...)]", path),
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

pub fn lit_str_to_expr_path(ctx: &mut Context, lit: &Lit) -> Result<ExprPath, ()> {
    match lit {
        Lit::Str(lit_str) => {
            let token_stream = syn::parse_str(&lit_str.value()).map_err(|e| {
                ctx.error_spanned_by(lit_str, e.to_string());
            })?;
            match syn::parse2::<ExprPath>(token_stream) {
                Ok(path) => Ok(path),
                Err(e) => {
                    ctx.error_spanned_by(lit, e.to_string());
                    Err(())
                }
            }
        }
        _ => {
            ctx.error_spanned_by(lit, "Expected a String literal");
            Err(())
        }
    }
}
