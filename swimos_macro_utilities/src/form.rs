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

use crate::label::Label;
use crate::{CompoundTypeKind, SynOriginal};
use syn::Variant;
use syn::{DeriveInput, Field};

pub mod attr_names {

    use crate::Symbol;

    pub const FORM_NAME: &str = "form";
    pub const NAME_NAME: &str = "name";
    pub const CONV_NAME: &str = "convention";
    pub const TAG_NAME: &str = "tag";
    pub const FIELDS_NAME: &str = "fields_convention";
    pub const SCHEMA_NAME: &str = "schema";

    pub const FORM_PATH: Symbol = Symbol(FORM_NAME);
    pub const HEADER_PATH: Symbol = Symbol("header");
    pub const ATTR_PATH: Symbol = Symbol("attr");
    pub const SLOT_PATH: Symbol = Symbol("slot");
    pub const BODY_PATH: Symbol = Symbol("body");
    pub const HEADER_BODY_PATH: Symbol = Symbol("header_body");
    pub const TAG_PATH: Symbol = Symbol(TAG_NAME);
    pub const SKIP_PATH: Symbol = Symbol("skip");
    pub const SCHEMA_PATH: Symbol = Symbol(SCHEMA_NAME);
    pub const NEWTYPE_PATH: Symbol = Symbol("newtype");
}

/// An enumeration representing the contents of an input.
#[derive(Clone)]
pub enum TypeContents<'t, D, F> {
    /// An enumeration input. Containing a vector of enumeration variants.
    Enum(EnumRepr<'t, D, F>),
    /// A struct input containing its representation.
    Struct(StructRepr<'t, D, F>),
}

/// A representation of a parsed struct from the AST.
#[derive(Clone)]
pub struct StructRepr<'t, D, F> {
    /// The original AST parsed by syn.
    pub input: &'t DeriveInput,
    /// The struct's type: tuple, named, unit or newtype.
    pub compound_type: CompoundTypeKind,
    /// The field members of the struct.
    pub fields: Vec<F>,
    /// A form descriptor
    pub descriptor: D,
}

/// A representation of a parsed enumeration from the AST.
#[derive(Clone)]
pub struct EnumRepr<'t, D, F> {
    /// The original AST parsed by syn.
    pub input: &'t DeriveInput,
    /// The variants in the enumeration.
    pub variants: Vec<EnumVariant<'t, D, F>>,
}

/// A representation of a parsed enumeration variant from the AST.
#[derive(Clone)]
pub struct EnumVariant<'t, D, F> {
    pub syn_variant: &'t Variant,
    /// The name of the variant.
    pub name: Label,
    /// The variant's type: tuple, named, unit or newtype.
    pub compound_type: CompoundTypeKind,
    /// The field members of the variant.
    pub fields: Vec<F>,
    /// A form descriptor
    pub descriptor: D,
}

/// A representation of a parsed field for a form from the AST.
#[derive(Clone)]
pub struct FormField<'a> {
    /// The original field from the [`DeriveInput`].
    pub original: &'a syn::Field,
    /// The name of the field.
    pub label: Label,
    /// The kind of the field from its attribute.
    pub kind: FieldKind,
}

impl<'a> SynOriginal for FormField<'a> {
    fn original(&self) -> &Field {
        self.original
    }
}

impl<'a> FormField<'a> {
    pub fn is_skipped(&self) -> bool {
        self.kind == FieldKind::Skip && !self.label.is_foreign()
    }

    pub fn is_attr(&self) -> bool {
        self.kind == FieldKind::Attr
    }

    pub fn is_slot(&self) -> bool {
        self.kind == FieldKind::Item
    }

    pub fn is_body(&self) -> bool {
        self.kind == FieldKind::Body
    }

    pub fn is_header_body(&self) -> bool {
        self.kind == FieldKind::HeaderBody
    }

    pub fn is_header(&self) -> bool {
        self.kind == FieldKind::Header
    }
}

/// Enumeration of ways in which fields can be serialized in Recon documents. Unannotated fields
/// are assumed to be annotated as `Item::Slot`.
#[derive(PartialEq, Debug, Eq, Hash, Copy, Clone, Default)]
pub enum FieldKind {
    /// The field should be written as a slot in the tag attribute.
    Header,
    /// The field should be written as an attribute.
    Attr,
    /// The field should be written as an item in the main body (or the header if another field is
    /// marked as `FieldKind::Body`
    #[default]
    Item,
    /// The field should be used to form the entire body of the record, all other fields that are
    /// marked as slots will be promoted to headers. At most one field may be marked with this.
    Body,
    /// The field should be moved into the body of the tag attribute (unlabelled). If there are no
    /// header fields it will form the entire body of the tag, otherwise it will be the first item
    /// of the tag body. At most one field may be marked with this.
    HeaderBody,
    /// The field will be ignored during transformations. The decorated field must implement
    /// [`Default`].
    Skip,
    Tagged,
}
