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

use bitflags::bitflags;
use frunk::hlist;
use macro_utilities::{
    attributes::{consume_attributes, NestedMetaConsumer},
    NameTransform, NameTransformConsumer, Transformation,
};
use std::hash::Hash;
use swim_utilities::errors::{
    validation::{Validation, ValidationItExt},
    Errors,
};
use syn::{
    AngleBracketedGenericArguments, Data, DataStruct, DeriveInput, Field, GenericArgument, Ident,
    PathArguments, PathSegment, Type, TypePath,
};

/// Model of a struct type for the AgentLaneModel derivation macro.
pub struct LanesModel<'a> {
    pub agent_type: &'a Ident,
    pub lanes: Vec<ItemModel<'a>>,
}

impl<'a> LanesModel<'a> {
    /// #Arguments
    /// * `agent_type` - The name of the target of the derive macro.
    /// * `lanes` - Description of each lane in the agent (the name of the corresponding field
    /// and the lane kind with types).
    fn new(agent_type: &'a Ident, lanes: Vec<ItemModel<'a>>) -> Self {
        LanesModel { agent_type, lanes }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ItemKind {
    Lane,
    Store,
}

/// The kinds of item that can be inferred from the type of a field.
#[derive(Clone, Copy, Debug)]
pub enum ItemSpec<'a> {
    Command(&'a Type),
    Demand(&'a Type),
    DemandMap(&'a Type, &'a Type),
    Value(ItemKind, &'a Type),
    Map(ItemKind, &'a Type, &'a Type),
    JoinValue(&'a Type, &'a Type),
    Http(HttpLaneSpec<'a>),
}

impl<'a> ItemSpec<'a> {
    pub fn lane(&self) -> Option<WarpLaneSpec<'a>> {
        match self {
            ItemSpec::Command(t) => Some(WarpLaneSpec::Command(t)),
            ItemSpec::Demand(t) => Some(WarpLaneSpec::Demand(t)),
            ItemSpec::DemandMap(k, v) => Some(WarpLaneSpec::DemandMap(k, v)),
            ItemSpec::Value(ItemKind::Lane, t) => Some(WarpLaneSpec::Value(t)),
            ItemSpec::Map(ItemKind::Lane, k, v) => Some(WarpLaneSpec::Map(k, v)),
            ItemSpec::JoinValue(k, v) => Some(WarpLaneSpec::JoinValue(k, v)),
            _ => None,
        }
    }

    pub fn http(&self) -> Option<HttpLaneSpec<'a>> {
        if let ItemSpec::Http(spec) = self {
            Some(*spec)
        } else {
            None
        }
    }

    pub fn item_kind(&self) -> ItemKind {
        match self {
            ItemSpec::Value(k, _) => *k,
            ItemSpec::Map(k, _, _) => *k,
            ItemSpec::Command(_) => ItemKind::Lane,
            ItemSpec::JoinValue(_, _) => ItemKind::Lane,
            ItemSpec::Demand(_) => ItemKind::Lane,
            ItemSpec::DemandMap(_, _) => ItemKind::Lane,
            ItemSpec::Http(_) => ItemKind::Lane,
        }
    }
}

/// The kinds of lane that can be inferred from the type of a field.
#[derive(Clone, Copy, Debug)]
pub enum WarpLaneSpec<'a> {
    Command(&'a Type),
    Demand(&'a Type),
    DemandMap(&'a Type, &'a Type),
    Value(&'a Type),
    Map(&'a Type, &'a Type),
    JoinValue(&'a Type, &'a Type),
}

#[derive(Clone, Copy, Debug)]
pub struct HttpLaneSpec<'a> {
    pub get: &'a Type,
    pub post: &'a Type,
    pub put: &'a Type,
    pub codec: Option<&'a Type>,
}

impl<'a> ItemSpec<'a> {
    pub fn is_stateful(&self) -> bool {
        !matches!(self, ItemSpec::Command(_) | ItemSpec::Demand(_))
    }
}

bitflags! {

    #[derive(Default)]
    pub struct ItemFlags: u8 {
        /// The state of the lane should not be persisted.
        const TRANSIENT = 0b01;
    }
}

/// Description of an item (its name the the kind of the item, along with types).
#[derive(Clone)]
pub struct ItemModel<'a> {
    pub name: &'a Ident,
    pub kind: ItemSpec<'a>,
    pub flags: ItemFlags,
    pub transform: NameTransform,
}

impl<'a> ItemModel<'a> {
    pub fn lane(&self) -> Option<WarpLaneModel<'a>> {
        let ItemModel {
            name,
            kind,
            flags,
            transform,
        } = self;
        kind.lane().map(move |kind| WarpLaneModel {
            name,
            kind,
            flags: *flags,
            transform: transform.clone(),
        })
    }

    pub fn http(&self) -> Option<HttpLaneModel<'a>> {
        let ItemModel {
            name,
            kind,
            transform,
            ..
        } = self;
        kind.http().map(move |kind| HttpLaneModel {
            name,
            kind,
            transform: transform.clone(),
        })
    }

    pub fn item_kind(&self) -> ItemKind {
        self.kind.item_kind()
    }
}

/// Description of an lane (its name the the kind of the lane, along with types).
#[derive(Clone)]
pub struct WarpLaneModel<'a> {
    pub name: &'a Ident,
    pub kind: WarpLaneSpec<'a>,
    pub flags: ItemFlags,
    pub transform: NameTransform,
}

/// Description of an HTTP lane (its name the the kind of the lane, along with types).
#[derive(Clone)]
pub struct HttpLaneModel<'a> {
    pub name: &'a Ident,
    pub kind: HttpLaneSpec<'a>,
    pub transform: NameTransform,
}

impl<'a> WarpLaneModel<'a> {
    /// The name of the lane as a string literal.
    pub fn literal(&self) -> proc_macro2::Literal {
        self.transform.transform(|| self.name.to_string())
    }
}

impl<'a> ItemModel<'a> {
    /// #Arguments
    /// * `name` - The name of the field in the struct (mapped to the name of the lane in the agent).
    /// * `kind` - The kind of the lane, along with any types.
    /// * `flags` - Modifiers applied to the lane.
    /// * `transform` - Transformation to apply to the name.
    fn new(
        name: &'a Ident,
        kind: ItemSpec<'a>,
        flags: ItemFlags,
        transform: NameTransform,
    ) -> ItemModel<'a> {
        ItemModel {
            name,
            kind,
            flags,
            transform,
        }
    }

    /// The name of the item as a string literal.
    pub fn literal(&self) -> proc_macro2::Literal {
        self.transform.transform(|| self.name.to_string())
    }

    /// Determine if the lane needs to persist its state.
    pub fn is_stateful(&self) -> bool {
        self.kind.is_stateful() && !self.flags.contains(ItemFlags::TRANSIENT)
    }
}

impl<'a> HttpLaneModel<'a> {
    /// The name of the lane as a string literal.
    pub fn literal(&self) -> proc_macro2::Literal {
        self.transform.transform(|| self.name.to_string())
    }
}

const NO_LANES: &str = "An agent must have at least one lane.";
const NOT_A_STRUCT: &str = "Type is not a struct type.";
const NO_GENERICS: &str = "Generic agents are not yet supported.";
const NOT_LANE_TYPE: &str = "Field is not of a lane type.";
const NO_TUPLES: &str = "Tuple structs are not supported.";
const BAD_PARAMS: &str = "Lane generic parameters are invalid.";
const INVALID_FIELD_ATTR: &str = "Invalid field attribute. Valid attributes are: '[#transient]'.";
const TRANSIENT_ATTR_NAME: &str = "transient";

/// Extract the model of the type from the type definition, collecting any
/// errors.
pub fn validate_input(value: &DeriveInput) -> Validation<LanesModel<'_>, Errors<syn::Error>> {
    if !value.generics.params.is_empty() {
        return Validation::fail(syn::Error::new_spanned(&value.ident, NO_GENERICS));
    }
    if let Data::Struct(body) = &value.data {
        try_from_struct(&value.ident, body)
    } else {
        Validation::fail(syn::Error::new_spanned(&value.ident, NOT_A_STRUCT))
    }
}

fn try_from_struct<'a>(
    name: &'a Ident,
    definition: &'a DataStruct,
) -> Validation<LanesModel<'a>, Errors<syn::Error>> {
    definition
        .fields
        .iter()
        .validate_fold(Validation::valid(vec![]), false, |mut lanes, field| {
            extract_lane_model(field).map(move |lane_model| {
                lanes.push(lane_model);
                lanes
            })
        })
        .and_then_append(|lanes| {
            if lanes.is_empty() {
                Validation::Validated(
                    lanes,
                    Some(syn::Error::new_spanned(&definition.fields, NO_LANES)),
                )
            } else {
                Validation::valid(lanes)
            }
        })
        .map(|lanes| LanesModel::new(name, lanes))
}

const COMMAND_LANE_NAME: &str = "CommandLane";
const DEMAND_LANE_NAME: &str = "DemandLane";
const DEMAND_MAP_LANE_NAME: &str = "DemandMapLane";
const VALUE_LANE_NAME: &str = "ValueLane";
const VALUE_STORE_NAME: &str = "ValueStore";
const MAP_LANE_NAME: &str = "MapLane";
const MAP_STORE_NAME: &str = "MapStore";
const JOIN_VALUE_LANE_NAME: &str = "JoinValueLane";
const HTTP_LANE_NAME: &str = "HttpLane";
const SIMPLE_HTTP_LANE_NAME: &str = "SimpleHttpLane";

const LANE_TAG: &str = "lane";

fn extract_lane_model(field: &Field) -> Validation<ItemModel<'_>, Errors<syn::Error>> {
    if let (Some(fld_name), Type::Path(TypePath { qself: None, path })) = (&field.ident, &field.ty)
    {
        if let Some(PathSegment { ident, arguments }) = path.segments.last() {
            let type_name = ident.to_string();
            let (item_attrs, errors) =
                consume_attributes(LANE_TAG, &field.attrs, make_attr_consumer());
            let modifiers = Validation::Validated(item_attrs, Errors::from(errors))
                .and_then(|item_attrs| combine_attrs(field, item_attrs));

            modifiers.and_then(
                |ItemModifiers {
                     transform,
                     flags: lane_flags,
                 }| {
                    match type_name.as_str() {
                        COMMAND_LANE_NAME => {
                            match single_param(arguments) {
                                Ok(param) => Validation::valid(ItemModel::new(
                                    fld_name,
                                    ItemSpec::Command(param),
                                    ItemFlags::TRANSIENT, //Command lanes are always transient.
                                    transform,
                                )),
                                Err(e) => Validation::fail(Errors::of(e)),
                            }
                        }
                        DEMAND_LANE_NAME => {
                            match single_param(arguments) {
                                Ok(param) => Validation::valid(ItemModel::new(
                                    fld_name,
                                    ItemSpec::Demand(param),
                                    ItemFlags::TRANSIENT, //Demand lanes are always transient.
                                    transform,
                                )),
                                Err(e) => Validation::fail(Errors::of(e)),
                            }
                        }
                        DEMAND_MAP_LANE_NAME => {
                            match two_params(arguments) {
                                Ok((param1, param2)) => Validation::valid(ItemModel::new(
                                    fld_name,
                                    ItemSpec::DemandMap(param1, param2),
                                    ItemFlags::TRANSIENT, //Demand map lanes are always transient.
                                    transform,
                                )),
                                Err(e) => Validation::fail(Errors::of(e)),
                            }
                        }
                        VALUE_LANE_NAME => match single_param(arguments) {
                            Ok(param) => Validation::valid(ItemModel::new(
                                fld_name,
                                ItemSpec::Value(ItemKind::Lane, param),
                                lane_flags,
                                transform,
                            )),
                            Err(e) => Validation::fail(Errors::of(e)),
                        },
                        VALUE_STORE_NAME => match single_param(arguments) {
                            Ok(param) => Validation::valid(ItemModel::new(
                                fld_name,
                                ItemSpec::Value(ItemKind::Store, param),
                                lane_flags,
                                transform,
                            )),
                            Err(e) => Validation::fail(Errors::of(e)),
                        },
                        MAP_LANE_NAME => match two_params(arguments) {
                            Ok((param1, param2)) => Validation::valid(ItemModel::new(
                                fld_name,
                                ItemSpec::Map(ItemKind::Lane, param1, param2),
                                lane_flags,
                                transform,
                            )),
                            Err(e) => Validation::fail(Errors::of(e)),
                        },
                        MAP_STORE_NAME => match two_params(arguments) {
                            Ok((param1, param2)) => Validation::valid(ItemModel::new(
                                fld_name,
                                ItemSpec::Map(ItemKind::Store, param1, param2),
                                lane_flags,
                                transform,
                            )),
                            Err(e) => Validation::fail(Errors::of(e)),
                        },
                        JOIN_VALUE_LANE_NAME => match two_params(arguments) {
                            Ok((param1, param2)) => Validation::valid(ItemModel::new(
                                fld_name,
                                ItemSpec::JoinValue(param1, param2),
                                lane_flags,
                                transform,
                            )),
                            Err(e) => Validation::fail(Errors::of(e)),
                        },
                        name @ (HTTP_LANE_NAME | SIMPLE_HTTP_LANE_NAME) => {
                            match http_params(arguments, name == SIMPLE_HTTP_LANE_NAME) {
                                Ok(spec) => Validation::valid(ItemModel::new(
                                    fld_name,
                                    ItemSpec::Http(spec),
                                    lane_flags,
                                    transform,
                                )),
                                Err(e) => Validation::fail(Errors::of(e)),
                            }
                        }
                        _ => Validation::fail(Errors::of(syn::Error::new_spanned(
                            &field.ty,
                            NOT_LANE_TYPE,
                        ))),
                    }
                },
            )
        } else {
            Validation::fail(Errors::of(syn::Error::new_spanned(
                &field.ty,
                NOT_LANE_TYPE,
            )))
        }
    } else if field.ident.is_none() {
        Validation::fail(Errors::of(syn::Error::new_spanned(&field.ty, NO_TUPLES)))
    } else {
        Validation::fail(Errors::of(syn::Error::new_spanned(
            &field.ty,
            NOT_LANE_TYPE,
        )))
    }
}

fn single_param(args: &PathArguments) -> Result<&Type, syn::Error> {
    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args {
        let mut selected = None;
        let it = args.iter();
        for arg in it {
            if let (GenericArgument::Type(ty), None) = (arg, &selected) {
                selected = Some(ty);
            } else {
                return Err(syn::Error::new_spanned(args, BAD_PARAMS));
            }
        }
        selected.ok_or_else(|| syn::Error::new_spanned(args, BAD_PARAMS))
    } else {
        Err(syn::Error::new_spanned(args, BAD_PARAMS))
    }
}

fn http_params(args: &PathArguments, simple: bool) -> Result<HttpLaneSpec<'_>, syn::Error> {
    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args {
        let params = args.iter().try_fold(vec![], |mut params, param| {
            if let GenericArgument::Type(ty) = param {
                params.push(ty);
                Ok(params)
            } else {
                Err(syn::Error::new_spanned(args, BAD_PARAMS))
            }
        })?;
        let http_type = match params.as_slice() {
            [get] => HttpLaneSpec {
                get,
                post: get,
                put: get,
                codec: None,
            },
            [get, codec] if simple => HttpLaneSpec {
                get,
                post: get,
                put: get,
                codec: Some(*codec),
            },
            [get, post] => HttpLaneSpec {
                get,
                post,
                put: post,
                codec: None,
            },
            [get, post, put] if !simple => HttpLaneSpec {
                get,
                post,
                put,
                codec: None,
            },
            [get, post, put, codec] if !simple => HttpLaneSpec {
                get,
                post,
                put,
                codec: Some(*codec),
            },
            _ => return Err(syn::Error::new_spanned(args, BAD_PARAMS)),
        };
        Ok(http_type)
    } else {
        Err(syn::Error::new_spanned(args, BAD_PARAMS))
    }
}

fn two_params(args: &PathArguments) -> Result<(&Type, &Type), syn::Error> {
    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args {
        let mut first = None;
        let mut second = None;
        let it = args.iter();
        for arg in it {
            match (arg, &first, &second) {
                (GenericArgument::Type(ty), None, None) => {
                    first = Some(ty);
                }
                (GenericArgument::Type(ty), _, None) => {
                    second = Some(ty);
                }
                _ => {
                    return Err(syn::Error::new_spanned(args, BAD_PARAMS));
                }
            }
        }
        first
            .zip(second)
            .ok_or_else(|| syn::Error::new_spanned(args, BAD_PARAMS))
    } else {
        Err(syn::Error::new_spanned(args, BAD_PARAMS))
    }
}

struct TransientFlagConsumer;

pub enum ItemAttr {
    Transient,
    Transform(Transformation),
}

impl NestedMetaConsumer<ItemAttr> for TransientFlagConsumer {
    fn try_consume(&self, meta: &syn::NestedMeta) -> Result<Option<ItemAttr>, syn::Error> {
        match meta {
            syn::NestedMeta::Meta(syn::Meta::Path(path)) => match path.segments.first() {
                Some(seg) => {
                    if seg.ident == TRANSIENT_ATTR_NAME {
                        if path.segments.len() == 1 && seg.arguments.is_empty() {
                            Ok(Some(ItemAttr::Transient))
                        } else {
                            Err(syn::Error::new_spanned(meta, INVALID_FIELD_ATTR))
                        }
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

const RENAME_TAG: &str = "name";
const CONV_TAG: &str = "convention";

fn make_attr_consumer() -> impl NestedMetaConsumer<ItemAttr> {
    let trans_consumer = NameTransformConsumer::new(RENAME_TAG, CONV_TAG);
    hlist![
        TransientFlagConsumer,
        trans_consumer.map(ItemAttr::Transform)
    ]
}

#[derive(Debug, Default)]
pub struct ItemModifiers {
    transform: NameTransform,
    flags: ItemFlags,
}

fn combine_attrs(
    field: &Field,
    attrs: Vec<ItemAttr>,
) -> Validation<ItemModifiers, Errors<syn::Error>> {
    attrs.into_iter().validate_fold(
        Validation::valid(ItemModifiers::default()),
        false,
        |mut modifiers, attr| {
            let mut errors = Errors::empty();
            match attr {
                ItemAttr::Transient => modifiers.flags.insert(ItemFlags::TRANSIENT),
                ItemAttr::Transform(t) => {
                    if let Err(e) = modifiers.transform.try_add(field, t) {
                        errors.push(e);
                    }
                }
            }
            Validation::valid(modifiers)
        },
    )
}
