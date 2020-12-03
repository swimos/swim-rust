// Copyright 2015-2020 SWIM.AI inc.
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
//
// pub fn with_bound(
//     cont: &Container,
//     generics: &syn::Generics,
//     filter: fn(&attr::Field, Option<&attr::Variant>) -> bool,
//     bound: &syn::Path,
// ) -> syn::Generics {
//     struct FindTyParams<'ast> {
//         // Set of all generic type parameters on the current struct (A, B, C in
//         // the example). Initialized up front.
//         all_type_params: HashSet<syn::Ident>,
//
//         // Set of generic type parameters used in fields for which filter
//         // returns true (A and B in the example). Filled in as the visitor sees
//         // them.
//         relevant_type_params: HashSet<syn::Ident>,
//
//         // Fields whose type is an associated type of one of the generic type
//         // parameters.
//         associated_type_usage: Vec<&'ast syn::TypePath>,
//     }
//     impl<'ast> Visit<'ast> for FindTyParams<'ast> {
//         fn visit_field(&mut self, field: &'ast syn::Field) {
//             if let syn::Type::Path(ty) = ungroup(&field.ty) {
//                 if let Some(Pair::Punctuated(t, _)) = ty.path.segments.pairs().next() {
//                     if self.all_type_params.contains(&t.ident) {
//                         self.associated_type_usage.push(ty);
//                     }
//                 }
//             }
//             self.visit_type(&field.ty);
//         }
//
//         fn visit_path(&mut self, path: &'ast syn::Path) {
//             if let Some(seg) = path.segments.last() {
//                 if seg.ident == "PhantomData" {
//                     // Hardcoded exception, because PhantomData<T> implements
//                     // Serialize and Deserialize whether or not T implements it.
//                     return;
//                 }
//             }
//             if path.leading_colon.is_none() && path.segments.len() == 1 {
//                 let id = &path.segments[0].ident;
//                 if self.all_type_params.contains(id) {
//                     self.relevant_type_params.insert(id.clone());
//                 }
//             }
//             visit::visit_path(self, path);
//         }
//
//         // Type parameter should not be considered used by a macro path.
//         //
//         //     struct TypeMacro<T> {
//         //         mac: T!(),
//         //         marker: PhantomData<T>,
//         //     }
//         fn visit_macro(&mut self, _mac: &'ast syn::Macro) {}
//     }
//
//     let all_type_params = generics
//         .type_params()
//         .map(|param| param.ident.clone())
//         .collect();
//
//     let mut visitor = FindTyParams {
//         all_type_params,
//         relevant_type_params: HashSet::new(),
//         associated_type_usage: Vec::new(),
//     };
//     match &cont.data {
//         Data::Enum(variants) => {
//             for variant in variants.iter() {
//                 let relevant_fields = variant
//                     .fields
//                     .iter()
//                     .filter(|field| filter(&field.attrs, Some(&variant.attrs)));
//                 for field in relevant_fields {
//                     visitor.visit_field(field.original);
//                 }
//             }
//         }
//         Data::Struct(_, fields) => {
//             for field in fields.iter().filter(|field| filter(&field.attrs, None)) {
//                 visitor.visit_field(field.original);
//             }
//         }
//     }
//
//     let relevant_type_params = visitor.relevant_type_params;
//     let associated_type_usage = visitor.associated_type_usage;
//     let new_predicates = generics
//         .type_params()
//         .map(|param| param.ident.clone())
//         .filter(|id| relevant_type_params.contains(id))
//         .map(|id| syn::TypePath {
//             qself: None,
//             path: id.into(),
//         })
//         .chain(associated_type_usage.into_iter().cloned())
//         .map(|bounded_ty| {
//             syn::WherePredicate::Type(syn::PredicateType {
//                 lifetimes: None,
//                 // the type parameter that is being bounded e.g. T
//                 bounded_ty: syn::Type::Path(bounded_ty),
//                 colon_token: <Token![:]>::default(),
//                 // the bound e.g. Serialize
//                 bounds: vec![syn::TypeParamBound::Trait(syn::TraitBound {
//                     paren_token: None,
//                     modifier: syn::TraitBoundModifier::None,
//                     lifetimes: None,
//                     path: bound.clone(),
//                 })]
//                 .into_iter()
//                 .collect(),
//             })
//         });
//
//     let mut generics = generics.clone();
//     generics
//         .make_where_clause()
//         .predicates
//         .extend(new_predicates);
//     generics
// }
