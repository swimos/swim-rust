// Copyright 2015-2024 Swim Inc.
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

use std::marker::PhantomData;

use frunk::{HCons, HNil};
use syn::Meta;

/// Process the attributes attached to some element of the processed Rust source (e.g. type,
/// item, field, etc.) using the provided interpretation strategy.
///
/// The attributes are assumed to have the form:
///
/// `#[name(part[,part]*)]`
///
/// The interpreted parts will be returned in a vector along with an vector of errors for
/// any failures.
///
/// # Arguments
///
/// * `tag` - The expected name of the attribute.
/// * `attributes` - The attributes to process.
/// * `f` - Consumer for the parts extracted from the attribute.
pub fn consume_attributes<'a, It, T, F>(
    tag: &str,
    attributes: It,
    f: F,
) -> (Vec<T>, Vec<syn::Error>)
where
    It: IntoIterator<Item = &'a syn::Attribute> + 'a,
    F: NestedMetaConsumer<T>,
{
    attributes
        .into_iter()
        .filter_map(|a| {
            if a.path.is_ident(tag) {
                if let Ok(syn::Meta::List(list)) = a.parse_meta() {
                    Some(list.nested)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .flatten()
        .fold((vec![], vec![]), |(mut values, mut errors), nested| {
            match f.try_consume(&nested) {
                Ok(Some(t)) => {
                    values.push(t);
                }
                Ok(_) => errors.push(syn::Error::new_spanned(
                    nested,
                    format!("Unrecognized attribute for '{}'.", tag),
                )),
                Err(e) => {
                    errors.push(e);
                }
            }
            (values, errors)
        })
}

/// A strategy for interpreting the parts of an attribute value.
pub trait NestedMetaConsumer<T> {
    /// Attempt to extract a single part. This may return an interpreted part, an error
    /// if the part is invalid or decline to process the part (by returning `Ok(None)`).
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<T>, syn::Error>;

    /// Transform the type of the results of this consumer.
    fn map<U, F>(self, f: F) -> MapConsumer<T, U, Self, F>
    where
        Self: Sized,
        F: Fn(T) -> U,
    {
        MapConsumer {
            inner: self,
            f,
            _type: PhantomData,
        }
    }
}

impl<T> NestedMetaConsumer<T> for HNil {
    fn try_consume(&self, _meta: &syn::NestedMeta) -> Result<Option<T>, syn::Error> {
        Ok(None)
    }
}

impl<T, Head, Tail> NestedMetaConsumer<T> for HCons<Head, Tail>
where
    Head: NestedMetaConsumer<T>,
    Tail: NestedMetaConsumer<T>,
{
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<T>, syn::Error> {
        let HCons { head, tail } = self;
        match head.try_consume(meta) {
            Ok(Some(t)) => Ok(Some(t)),
            Ok(None) => tail.try_consume(meta),
            Err(e) => Err(e),
        }
    }
}

pub struct MapConsumer<T, U, C, F> {
    inner: C,
    f: F,
    _type: PhantomData<fn(T) -> U>,
}

impl<T, U, C, F> NestedMetaConsumer<U> for MapConsumer<T, U, C, F>
where
    C: NestedMetaConsumer<T>,
    F: Fn(T) -> U,
{
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<U>, syn::Error> {
        self.inner.try_consume(meta).map(|maybe| maybe.map(&self.f))
    }
}

/// Explicitly ignores any parts of the form `name(...)`.
pub struct IgnoreConsumer {
    name: &'static str,
}

impl IgnoreConsumer {
    pub fn new(name: &'static str) -> Self {
        IgnoreConsumer { name }
    }
}

impl NestedMetaConsumer<()> for IgnoreConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<()>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(Meta::List(list)) if list.path.is_ident(self.name) => {
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }
}
