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

use core::fmt;
use core::fmt::{Debug, Formatter};
use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::Index;

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
    Foreign(Ident, TokenStream, Ident),
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

    /// Returns this [`FieldName`] represented as an [`Ident`]ifier. For renamed fields, this function
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

    pub fn to_name(&self, clone: bool) -> TokenStream {
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
