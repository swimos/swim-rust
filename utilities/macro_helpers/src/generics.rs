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

use crate::form::{EnumRepr, StructRepr, TypeContents};
use crate::SynOriginal;
use std::collections::HashSet;
use syn::punctuated::Pair;
use syn::visit::{visit_path, Visit};
use syn::{
    Field, Generics, Ident, Path, PredicateType, TraitBound, TraitBoundModifier, Type,
    TypeParamBound, TypePath, WherePredicate,
};

#[allow(unused_imports)]
use syn::token::Token;

/// Adds the provided bound to all generic parameters used in the fields. Fields for which
/// `filter_fn` returns false are ignored.
pub fn add_bound<'t, D, F>(
    type_contents: &TypeContents<'t, D, F>,
    tyc_generics: &Generics,
    filter_fn: fn(&F) -> bool,
    bound: &Path,
) -> Generics
where
    F: SynOriginal,
{
    struct Visitor<'l> {
        all: HashSet<Ident>,
        relevant: HashSet<Ident>,
        associated: Vec<&'l TypePath>,
    }
    impl<'l> Visit<'l> for Visitor<'l> {
        fn visit_field(&mut self, field: &'l Field) {
            if let Type::Path(ty) = ungroup(&field.ty) {
                if let Some(Pair::Punctuated(t, _)) = ty.path.segments.pairs().next() {
                    if self.all.contains(&t.ident) {
                        self.associated.push(ty);
                    }
                }
            }
            self.visit_type(&field.ty);
        }

        fn visit_path(&mut self, path: &'l Path) {
            if path.leading_colon.is_none() && path.segments.len() == 1 {
                let id = &path.segments[0].ident;
                if self.all.contains(id) {
                    self.relevant.insert(id.clone());
                }
            }

            visit_path(self, path);
        }
    }

    let all = tyc_generics
        .type_params()
        .map(|param| param.ident.clone())
        .collect();

    let mut visitor = Visitor {
        all,
        relevant: HashSet::new(),
        associated: Vec::new(),
    };

    match type_contents {
        TypeContents::Enum(EnumRepr { variants, .. }) => {
            for variant in variants.iter() {
                let relevant_fields = variant.fields.iter().filter(|field| filter_fn(field));
                for field in relevant_fields {
                    visitor.visit_field(field.original());
                }
            }
        }
        TypeContents::Struct(StructRepr { fields, .. }) => {
            for field in fields.iter().filter(|field| filter_fn(field)) {
                visitor.visit_field(field.original());
            }
        }
    }

    let relevant = visitor.relevant;
    let associated = visitor.associated;

    let new_generics = tyc_generics
        .type_params()
        .map(|param| param.ident.clone())
        .filter(|id| relevant.contains(id))
        .map(|id| TypePath {
            qself: None,
            path: id.into(),
        })
        .chain(associated.into_iter().cloned())
        .map(|path| {
            WherePredicate::Type(PredicateType {
                lifetimes: None,
                bounded_ty: Type::Path(path),
                colon_token: <Token![:]>::default(),
                bounds: vec![TypeParamBound::Trait(TraitBound {
                    paren_token: None,
                    modifier: TraitBoundModifier::None,
                    lifetimes: None,
                    path: bound.clone(),
                })]
                .into_iter()
                .collect(),
            })
        });

    let mut generics = tyc_generics.clone();

    generics.make_where_clause().predicates.extend(new_generics);

    generics
}

fn ungroup(mut ty: &Type) -> &Type {
    while let Type::Group(group) = ty {
        ty = &group.elem;
    }
    ty
}
