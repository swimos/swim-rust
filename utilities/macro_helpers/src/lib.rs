extern crate proc_macro;
extern crate proc_macro2;
#[allow(unused_imports)]
#[macro_use]
extern crate quote;
#[allow(unused_imports)]
#[macro_use]
extern crate syn;

use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use std::cell::RefCell;
use std::fmt::Display;
use syn::export::TokenStream2;
use syn::punctuated::Punctuated;
use syn::{Index, Meta};

pub enum TypeContents<'t> {
    Enum(Vec<EnumVariant<'t>>),
    Struct(CompoundType, Vec<Field<'t>>),
}

pub struct EnumVariant<'t> {
    pub ident: syn::Ident,
    pub style: CompoundType,
    pub fields: Vec<Field<'t>>,
    pub original: &'t syn::Variant,
}

#[derive(Clone, Copy, PartialEq)]
pub enum CompoundType {
    Struct,
    Tuple,
    NewType,
    Unit,
}

pub struct Field<'t> {
    pub member: syn::Member,
    pub ty: &'t syn::Type,
    pub original: &'t syn::Field,
    pub name: FieldName,
}

#[derive(Clone)]
pub enum FieldName {
    /// A named field containing its identifier.
    Named(Ident),
    /// A renamed field containing its new name (0) and original identifier (1).
    Renamed(Ident, Ident),
    /// An unnamed field containing its index in the parent structure.
    Unnamed(Index),
}

impl FieldName {
    /// Returns this [`FieldName`] represented as an [`Ident`]ifier. For renamed fields, this function
    /// returns the original field identifier represented and not the new name. For unnamed fields,
    /// this function returns a new identifier in the format of `__self_index`, where `index` is
    /// the ordinal of the field.
    pub fn as_ident(&self) -> Ident {
        match self {
            FieldName::Named(ident) => ident.clone(),
            FieldName::Renamed(_, ident) => ident.clone(),
            FieldName::Unnamed(index) => Ident::new(&format!("__self_{}", index.index), index.span),
        }
    }
}

impl ToTokens for FieldName {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            FieldName::Named(ident) => ident.to_tokens(tokens),
            FieldName::Renamed(_, ident) => ident.to_tokens(tokens),
            FieldName::Unnamed(index) => index.to_tokens(tokens),
        }
    }
}

pub struct Context {
    errors: RefCell<Option<Vec<syn::Error>>>,
}

impl Context {
    pub fn default() -> Context {
        Context {
            errors: RefCell::new(Some(Vec::new())),
        }
    }
}

impl Context {
    pub fn error_spanned_by<A: ToTokens, T: Display>(&self, obj: A, msg: T) {
        self.errors
            .borrow_mut()
            .as_mut()
            .unwrap()
            .push(syn::Error::new_spanned(obj.into_token_stream(), msg));
    }

    pub fn check(self) -> Result<(), Vec<syn::Error>> {
        let errors = self.errors.borrow_mut().take().unwrap();
        match errors.len() {
            0 => Ok(()),
            _ => Err(errors),
        }
    }
}

pub fn fields_from_ast(fields: &Punctuated<syn::Field, syn::Token![,]>) -> Vec<Field> {
    fields
        .iter()
        .enumerate()
        .map(|(index, original)| Field {
            member: match &original.ident {
                Some(ident) => syn::Member::Named(ident.clone()),
                None => syn::Member::Unnamed(index.into()),
            },
            ty: &original.ty,
            original,
            name: {
                match &original.ident {
                    Some(ident) => FieldName::Named(ident.clone()),
                    None => FieldName::Unnamed(Index::from(index)),
                }
            },
        })
        .collect()
}

pub fn fold_quote<L: IntoIterator, F>(init: TokenStream2, items: L, mut f: F) -> TokenStream2
where
    L::Item: ToTokens,
    L::IntoIter: IntoIterator,
    F: FnMut(L::Item, proc_macro2::TokenStream) -> proc_macro2::TokenStream,
{
    items.into_iter().fold(init, |result, item| f(item, result))
}

pub fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

pub fn deconstruct_type<'t>(
    compound_type: &CompoundType,
    fields: &Vec<&Field<'t>>,
) -> TokenStream2 {
    let fields: Vec<_> = fields
        .iter()
        .map(|f| match &f.name {
            FieldName::Named(ident) => {
                quote! { #ident }
            }
            FieldName::Renamed(_, ident) => {
                quote! { #ident }
            }
            FieldName::Unnamed(_) => {
                let binding = &f.name.as_ident();
                quote! { #binding }
            }
        })
        .collect();

    match compound_type {
        CompoundType::Struct => quote! { { #(#fields,)* } },
        CompoundType::Tuple => quote! { ( #(#fields,)* ) },
        CompoundType::NewType => quote! { ( #(#fields,)* ) },
        CompoundType::Unit => quote!(),
    }
}

pub fn get_attribute_meta(
    ctx: &Context,
    attr: &syn::Attribute,
    path: &'static str,
) -> Result<Vec<syn::NestedMeta>, ()> {
    if !attr.path.is_ident(path) {
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
