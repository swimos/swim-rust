// Copyright 2015-2023 Swim Inc.
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

use std::{borrow::Cow, str::FromStr};

use convert_case::{Case, Casing};
use proc_macro2::Literal;
use quote::ToTokens;
use thiserror::Error;

use crate::attributes::NestedMetaConsumer;

/// Case conventions supported for Swim derive macros.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CaseConvention {
    #[default]
    CamelLower,
    CamelUpper,
    SnakeLower,
    SnakeUpper,
    KebabLower,
    KebabUpper,
}

#[derive(Debug, Error)]
#[error("{0} is not a valid case convention.")]
pub struct InvalidCaseConvention(pub String);

impl FromStr for CaseConvention {
    type Err = InvalidCaseConvention;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "camel" | "camel-lower" => Ok(CaseConvention::CamelLower),
            "camel-upper" => Ok(CaseConvention::CamelUpper),
            "snake" | "snake-lower" => Ok(CaseConvention::SnakeLower),
            "snake-upper" => Ok(CaseConvention::SnakeUpper),
            "kebab" | "kebab-lower" => Ok(CaseConvention::KebabLower),
            "kebab-upper" => Ok(CaseConvention::KebabUpper),
            ow => Err(InvalidCaseConvention(ow.to_string())),
        }
    }
}

/// Name transformation strategy that is applied to an entire type, transforming the names
/// of all sub-items of that type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TypeLevelNameTransform {
    #[default]
    Identity,
    Convention(CaseConvention),
}

impl TypeLevelNameTransform {
    pub fn is_identity(&self) -> bool {
        matches!(self, TypeLevelNameTransform::Identity)
    }

    pub fn try_add(
        self,
        meta: &syn::NestedMeta,
        trans: Transformation,
    ) -> Result<Self, syn::Error> {
        match (self, trans) {
            (TypeLevelNameTransform::Identity, Transformation::Rename(_)) => Err(
                syn::Error::new_spanned(meta, "Only renaming conventions may be applied here."),
            ),
            (TypeLevelNameTransform::Identity, Transformation::Convention(conv)) => {
                Ok(TypeLevelNameTransform::Convention(conv))
            }
            _ => Err(syn::Error::new_spanned(
                meta,
                "Duplicate name transformations.",
            )),
        }
    }

    /// Combine together two levels of transformation (for example an enum type could have a transformation
    /// defined for the type and one for one of its variants). The child transformation takes precedence,
    /// where it is defined.
    pub fn combine(&self, child: TypeLevelNameTransform) -> Self {
        match child {
            TypeLevelNameTransform::Identity => *self,
            ow => ow,
        }
    }

    /// Resolve the name transformation defined for a single item (such as a field) against this top level
    /// transformation. The item transformation takes precedence, where it is defined.
    pub fn resolve(&self, field: NameTransform) -> NameTransform {
        match field {
            NameTransform::Identity => match self {
                TypeLevelNameTransform::Identity => NameTransform::Identity,
                TypeLevelNameTransform::Convention(case_conv) => {
                    NameTransform::Convention(*case_conv)
                }
            },
            ow => ow,
        }
    }
}

/// A name transformation that is applied to a single item (such as a field). It may either
/// specify a new name explicitly or define how it should be renamed by a convention.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum NameTransform {
    #[default]
    Identity,
    Rename(String),
    Convention(CaseConvention),
}

impl NameTransform {
    pub fn is_identity(&self) -> bool {
        matches!(self, NameTransform::Identity)
    }

    pub fn try_add<T: ToTokens>(
        &mut self,
        meta: T,
        trans: Transformation,
    ) -> Result<(), syn::Error> {
        match (&mut *self, trans) {
            (NameTransform::Identity, Transformation::Rename(name)) => {
                *self = NameTransform::Rename(name);
                Ok(())
            }
            (NameTransform::Identity, Transformation::Convention(conv)) => {
                *self = NameTransform::Convention(conv);
                Ok(())
            }
            _ => Err(syn::Error::new_spanned(
                meta,
                "Duplicate name transformations.",
            )),
        }
    }

    pub fn transform_cow<'a>(&'a self, name: String) -> Cow<'a, str> {
        match self {
            NameTransform::Identity => Cow::Owned(name),
            NameTransform::Rename(new_name) => Cow::Borrowed(&new_name),
            NameTransform::Convention(case_conv) => {
                Cow::Owned(name.to_case(Case::from(*case_conv)))
            }
        }
    }

    pub fn transform<F, S>(&self, name: F) -> Literal
    where
        F: Fn() -> S,
        S: AsRef<str>,
    {
        match self {
            NameTransform::Identity => Literal::string(name().as_ref()),
            NameTransform::Rename(new_name) => Literal::string(new_name),
            NameTransform::Convention(case_conv) => {
                let transformed = name().as_ref().to_case(Case::from(*case_conv));
                Literal::string(&transformed)
            }
        }
    }
}

impl From<CaseConvention> for Case {
    fn from(value: CaseConvention) -> Self {
        match value {
            CaseConvention::CamelLower => Case::Camel,
            CaseConvention::CamelUpper => Case::UpperCamel,
            CaseConvention::SnakeLower => Case::Snake,
            CaseConvention::SnakeUpper => Case::UpperSnake,
            CaseConvention::KebabLower => Case::Kebab,
            CaseConvention::KebabUpper => Case::UpperKebab,
        }
    }
}

#[derive(Error)]
pub enum NameTransformError<'a> {
    #[error("Renaming directive is not a string: {0:?}")]
    NonStringName(&'a syn::Lit),
    #[error("Renaming directive is empty.")]
    EmptyName(&'a syn::LitStr),
    #[error("{0} is not a valid renaming directive.")]
    UnknownAttributeName(String, &'a dyn ToTokens),
    #[error("Invalid renaming directive.")]
    UnknownAttribute(&'a syn::NestedMeta),
    #[error("{0} is not a valid case convention.")]
    InvalidCaseConvention(String, &'a syn::LitStr),
}

impl<'a> From<NameTransformError<'a>> for syn::Error {
    fn from(err: NameTransformError<'a>) -> Self {
        match err {
            NameTransformError::NonStringName(name) => {
                syn::Error::new_spanned(name, "Expected a string literal")
            }
            NameTransformError::EmptyName(name) => {
                syn::Error::new_spanned(name, "New tag cannot be empty")
            }
            NameTransformError::UnknownAttributeName(name, tok) => {
                syn::Error::new_spanned(tok, format!("Unknown container attribute: {}", name))
            }
            NameTransformError::UnknownAttribute(tok) => {
                syn::Error::new_spanned(tok, "Unknown container attribute")
            }
            NameTransformError::InvalidCaseConvention(_, lit) => {
                syn::Error::new_spanned(lit, "Invalid case convention")
            }
        }
    }
}

impl<'a> std::fmt::Debug for NameTransformError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonStringName(arg0) => f.debug_tuple("NonStringName").field(arg0).finish(),
            Self::EmptyName(arg0) => f.debug_tuple("EmptyName").field(arg0).finish(),
            Self::UnknownAttributeName(arg0, _) => f
                .debug_tuple("UnknownAttributeName")
                .field(arg0)
                .field(&"...")
                .finish(),
            Self::UnknownAttribute(arg0) => f.debug_tuple("UnknownAttribute").field(arg0).finish(),
            Self::InvalidCaseConvention(arg0, _) => {
                f.debug_tuple("InvalidCaseConvention").field(arg0).finish()
            }
        }
    }
}

/// Description of name transformation strategy that can be read from an attribute applied to an item.
pub enum Transformation {
    Rename(String),
    Convention(CaseConvention),
}

impl From<Transformation> for NameTransform {
    fn from(value: Transformation) -> Self {
        match value {
            Transformation::Rename(name) => NameTransform::Rename(name),
            Transformation::Convention(conv) => NameTransform::Convention(conv),
        }
    }
}

pub struct TypeLevelNameTransformConsumer<'a> {
    convention_tag: &'a str,
}

impl<'a> TypeLevelNameTransformConsumer<'a> {
    pub fn new(convention_tag: &'a str) -> Self {
        TypeLevelNameTransformConsumer { convention_tag }
    }
}

impl<'a> NestedMetaConsumer<CaseConvention> for TypeLevelNameTransformConsumer<'a> {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<CaseConvention>, syn::Error> {
        let TypeLevelNameTransformConsumer { convention_tag } = self;
        match meta {
            syn::NestedMeta::Meta(syn::Meta::NameValue(name))
                if name.path.is_ident(convention_tag) =>
            {
                match &name.lit {
                    syn::Lit::Str(s) => {
                        let tag = s.value();
                        match tag.parse::<CaseConvention>() {
                            Ok(convention) => Ok(Some(convention)),
                            Err(InvalidCaseConvention(name)) => {
                                Err(NameTransformError::InvalidCaseConvention(name, s).into())
                            }
                        }
                    }
                    ow => Err(NameTransformError::NonStringName(ow).into()),
                }
            }
            _ => Ok(None),
        }
    }
}

pub struct NameTransformConsumer<'a> {
    rename_tag: &'a str,
    convention_tag: &'a str,
}

impl<'a> NameTransformConsumer<'a> {
    pub fn new(rename_tag: &'a str, convention_tag: &'a str) -> Self {
        NameTransformConsumer {
            rename_tag,
            convention_tag,
        }
    }
}

impl<'a> NestedMetaConsumer<Transformation> for NameTransformConsumer<'a> {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<Transformation>, syn::Error> {
        let NameTransformConsumer {
            rename_tag,
            convention_tag,
        } = self;
        match meta {
            syn::NestedMeta::Meta(syn::Meta::NameValue(name)) => {
                if name.path.is_ident(rename_tag) || name.path.is_ident(convention_tag) {
                    match &name.lit {
                        syn::Lit::Str(s) => {
                            let tag = s.value();
                            if tag.is_empty() {
                                Err(NameTransformError::EmptyName(s).into())
                            } else if name.path.is_ident(rename_tag) {
                                Ok(Some(Transformation::Rename(tag)))
                            } else {
                                match tag.parse::<CaseConvention>() {
                                    Ok(convention) => {
                                        Ok(Some(Transformation::Convention(convention)))
                                    }
                                    Err(InvalidCaseConvention(name)) => {
                                        Err(NameTransformError::InvalidCaseConvention(name, s)
                                            .into())
                                    }
                                }
                            }
                        }
                        ow => Err(NameTransformError::NonStringName(ow).into()),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

/// Assemble an [`NameTransform`] from transformations read from the attributes on an item.
pub fn combine_name_transform<T: ToTokens>(
    meta: &T,
    transforms: Vec<Transformation>,
) -> Result<NameTransform, syn::Error> {
    transforms
        .into_iter()
        .try_fold(NameTransform::Identity, |trans, t| {
            if trans.is_identity() {
                match t {
                    Transformation::Rename(name) => Ok(NameTransform::Rename(name)),
                    Transformation::Convention(conv) => Ok(NameTransform::Convention(conv)),
                }
            } else {
                Err(syn::Error::new_spanned(
                    meta,
                    "Duplicate renaming specifion.",
                ))
            }
        })
}
