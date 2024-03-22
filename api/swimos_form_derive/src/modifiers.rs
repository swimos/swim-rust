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

use crate::structural::model::field::FieldSelector;
use crate::SynValidation;
use macro_utilities::attr_names::{CONV_NAME, FIELDS_NAME, NEWTYPE_PATH, SCHEMA_NAME, TAG_NAME};
use macro_utilities::attributes::{IgnoreConsumer, NestedMetaConsumer};
use macro_utilities::{
    CaseConvention, NameTransform, NameTransformConsumer, Symbol, Transformation,
    TypeLevelNameTransform, TypeLevelNameTransformConsumer,
};
use quote::ToTokens;
use swimos_utilities::errors::validation::{Validation, ValidationItExt};
use swimos_utilities::errors::Errors;

/// Fold the attributes present on some syntactic element, accumulating errors.
pub fn fold_attr_meta<'a, It, S, F>(path: Symbol, attrs: It, init: S, mut f: F) -> SynValidation<S>
where
    It: Iterator<Item = &'a syn::Attribute> + 'a,
    F: FnMut(S, syn::NestedMeta) -> SynValidation<S>,
{
    attrs.filter(|a| a.path == path).validate_fold(
        Validation::valid(init),
        false,
        move |state, attribute| match attribute.parse_meta() {
            Ok(syn::Meta::List(list)) => {
                list.nested
                    .into_iter()
                    .validate_fold(Validation::valid(state), false, &mut f)
            }
            Ok(_) => {
                let err = syn::Error::new_spanned(
                    attribute,
                    format!("Invalid attribute. Expected #[{}(...)]", path),
                );
                Validation::Validated(state, err.into())
            }
            Err(e) => {
                let err = syn::Error::new_spanned(attribute, e.to_compile_error());
                Validation::Validated(state, err.into())
            }
        },
    )
}

/// Components used to build a [`StructTransform`].
pub enum StructTransformPart<'a> {
    /// A directive to rename the struct.
    Rename(Transformation),
    /// A directive to rename the fields of the struct according to a convention.
    FieldRename(CaseConvention),
    /// A directive to delegate to a field of the struct.
    Newtype(Option<FieldSelector<'a>>),
    /// Indicates an explicitly ignore attribute.
    Ignored,
}

/// Directives to alter the interpretation of a enum definition, extracted from the attributes that
/// were attached to the it.
#[derive(Debug, Default)]
pub struct EnumTransform {
    /// Directive to rename the variants of the enumeration.
    pub variant_rename: TypeLevelNameTransform,
    /// Directive to rename the fields of the variants of the enumeration.
    pub field_rename: TypeLevelNameTransform,
}

/// Directives to alter the interpretation of a struct definition, extracted from the attributes that
/// were attached to the it.
#[derive(Debug)]
pub enum StructTransform<'a> {
    Standard {
        rename: NameTransform,
        field_rename: TypeLevelNameTransform,
    },
    Newtype(Option<FieldSelector<'a>>),
}

impl<'a> Default for StructTransform<'a> {
    fn default() -> Self {
        StructTransform::Standard {
            rename: Default::default(),
            field_rename: Default::default(),
        }
    }
}

/// Components used to build a [`EnumTransform`].
pub enum EnumTransformPart {
    Variants(CaseConvention),
    Fields(CaseConvention),
    Ignored,
}

pub struct EnumPartConsumer {
    variants: TypeLevelNameTransformConsumer<'static>,
    fields: TypeLevelNameTransformConsumer<'static>,
    ignore: IgnoreConsumer,
}

impl Default for EnumPartConsumer {
    fn default() -> Self {
        Self {
            variants: TypeLevelNameTransformConsumer::new(CONV_NAME),
            fields: TypeLevelNameTransformConsumer::new(FIELDS_NAME),
            ignore: IgnoreConsumer::new(SCHEMA_NAME),
        }
    }
}

impl NestedMetaConsumer<EnumTransformPart> for EnumPartConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<EnumTransformPart>, syn::Error> {
        match self.variants.try_consume(meta) {
            Ok(Some(part)) => return Ok(Some(EnumTransformPart::Variants(part))),
            Err(e) => return Err(e),
            _ => {}
        }
        match self.fields.try_consume(meta) {
            Ok(Some(part)) => return Ok(Some(EnumTransformPart::Fields(part))),
            Err(e) => return Err(e),
            _ => {}
        }
        match self.ignore.try_consume(meta) {
            Ok(Some(_)) => Ok(Some(EnumTransformPart::Ignored)),
            Err(e) => Err(e),
            _ => Ok(None),
        }
    }
}

/// Attempts to create an [`EnumTransform`] for a number of [`EnumTransformPart`]s that were
/// extracted from attributes attached to the enum.
pub fn combine_enum_trans_parts<T: ToTokens + ?Sized>(
    meta: &T,
    parts: Vec<EnumTransformPart>,
) -> SynValidation<EnumTransform> {
    parts.into_iter().validate_fold(
        Validation::valid(EnumTransform::default()),
        true,
        |mut acc, part| {
            let EnumTransform {
                variant_rename,
                field_rename,
            } = &mut acc;
            match part {
                EnumTransformPart::Variants(conv) => {
                    if variant_rename.is_identity() {
                        *variant_rename = TypeLevelNameTransform::Convention(conv);
                        Validation::valid(acc)
                    } else {
                        Validation::fail(Errors::of(syn::Error::new_spanned(
                            meta,
                            "Duplicate variant naming convention.",
                        )))
                    }
                }
                EnumTransformPart::Fields(conv) => {
                    if field_rename.is_identity() {
                        *field_rename = TypeLevelNameTransform::Convention(conv);
                        Validation::valid(acc)
                    } else {
                        Validation::fail(Errors::of(syn::Error::new_spanned(
                            meta,
                            "Duplicate variant naming convention.",
                        )))
                    }
                }
                EnumTransformPart::Ignored => Validation::valid(acc),
            }
        },
    )
}

/// Attempts to create an [`StructTransform`] for a number of [`StructTransformPart`]s that were
/// extracted from attributes attached to the struct.
pub fn combine_struct_trans_parts<T: ToTokens + ?Sized>(
    meta: &T,
    parts: Vec<StructTransformPart<'static>>,
) -> SynValidation<StructTransform<'static>> {
    parts.into_iter().validate_fold(
        Validation::valid(StructTransform::default()),
        true,
        |mut acc, part| match (&mut acc, part) {
            (StructTransform::Standard { rename, .. }, StructTransformPart::Rename(t)) => {
                if rename.is_identity() {
                    *rename = t.into();
                    Validation::valid(acc)
                } else {
                    Validation::fail(Errors::of(syn::Error::new_spanned(
                        meta,
                        "Duplicate struct tag renaming directive",
                    )))
                }
            }
            (
                StructTransform::Standard { field_rename, .. },
                StructTransformPart::FieldRename(conv),
            ) => {
                if field_rename.is_identity() {
                    *field_rename = TypeLevelNameTransform::Convention(conv);
                    Validation::valid(acc)
                } else {
                    Validation::fail(Errors::of(syn::Error::new_spanned(
                        meta,
                        "Duplicate field renaming directive",
                    )))
                }
            }
            (
                StructTransform::Standard {
                    rename,
                    field_rename,
                },
                StructTransformPart::Newtype(s),
            ) => {
                if rename.is_identity() && field_rename.is_identity() {
                    acc = StructTransform::Newtype(s);
                    Validation::valid(acc)
                } else {
                    Validation::fail(Errors::of(syn::Error::new_spanned(
                        meta,
                        "'newtype' cannot be combined with renaming directives.",
                    )))
                }
            }
            (StructTransform::Newtype(_), StructTransformPart::Newtype(_)) => {
                Validation::fail(Errors::of(syn::Error::new_spanned(
                    meta,
                    "'newtype' can only be applied once.",
                )))
            }
            (StructTransform::Newtype(_), _) => {
                Validation::fail(Errors::of(syn::Error::new_spanned(
                    meta,
                    "'newtype' cannot be combined with renaming directives.",
                )))
            }
            _ => Validation::valid(acc),
        },
    )
}

pub struct StructTransformPartConsumer {
    rename: NameTransformConsumer<'static>,
    field_rename: TypeLevelNameTransformConsumer<'static>,
    schema_ignore: IgnoreConsumer,
}

impl Default for StructTransformPartConsumer {
    fn default() -> Self {
        Self {
            rename: NameTransformConsumer::new(TAG_NAME, CONV_NAME),
            field_rename: TypeLevelNameTransformConsumer::new(FIELDS_NAME),
            schema_ignore: IgnoreConsumer::new(SCHEMA_NAME),
        }
    }
}

impl NestedMetaConsumer<StructTransformPart<'static>> for StructTransformPartConsumer {
    fn try_consume(
        &self,
        meta: &syn::NestedMeta,
    ) -> Result<Option<StructTransformPart<'static>>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) if path == NEWTYPE_PATH => {
                Ok(Some(StructTransformPart::Newtype(None)))
            }
            _ => {
                let StructTransformPartConsumer {
                    rename,
                    field_rename,
                    schema_ignore,
                } = self;
                match rename.try_consume(meta) {
                    Ok(Some(trans)) => return Ok(Some(StructTransformPart::Rename(trans))),
                    Err(e) => return Err(e),
                    _ => {}
                }
                match field_rename.try_consume(meta) {
                    Ok(Some(trans)) => return Ok(Some(StructTransformPart::FieldRename(trans))),
                    Err(e) => return Err(e),
                    _ => {}
                }
                match schema_ignore.try_consume(meta) {
                    Ok(Some(_)) => Ok(Some(StructTransformPart::Ignored)),
                    Err(e) => Err(e),
                    _ => Ok(None),
                }
            }
        }
    }
}
