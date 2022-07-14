// Copyright 2015-2021 Swim Inc.
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

use quote::ToTokens;
use swim_utilities::errors::validation::{Validation, ValidationItExt};
use syn::{
    AngleBracketedGenericArguments, Data, DataStruct, DeriveInput, Field, GenericArgument, Ident,
    PathArguments, PathSegment, Type, TypePath,
};

pub struct LanesModel<'a> {
    pub agent_type: &'a Ident,
    pub lanes: Vec<LaneModel<'a>>,
}

impl<'a> LanesModel<'a> {
    fn new(agent_type: &'a Ident, lanes: Vec<LaneModel<'a>>) -> Self {
        LanesModel { agent_type, lanes }
    }
}

#[derive(Clone, Copy)]
pub enum LaneKind<'a> {
    CommandLane(&'a Type),
    ValueLane(&'a Type),
    MapLane(&'a Type, &'a Type),
}

#[derive(Clone, Copy)]
pub struct LaneModel<'a> {
    pub name: &'a Ident,
    pub kind: LaneKind<'a>,
}

impl<'a> LaneModel<'a> {
    fn new(name: &'a Ident, kind: LaneKind<'a>) -> LaneModel<'a> {
        LaneModel { name, kind }
    }

    pub fn literal(&self) -> proc_macro2::Literal {
        let name_str = self.name.to_string();
        proc_macro2::Literal::string(name_str.as_str())
    }
}

pub struct AgentTypeError<'a> {
    location: &'a dyn ToTokens,
    kind: AgentTypeErrorKind<'a>,
}

pub struct LaneError<'a> {
    location: &'a dyn ToTokens,
    kind: LaneErrorKind,
}

impl<'a> LaneError<'a> {
    fn new<T: ToTokens>(loc: &'a T, kind: LaneErrorKind) -> Self {
        LaneError {
            location: loc,
            kind,
        }
    }
}

impl<'a> AgentTypeError<'a> {
    fn new<T: ToTokens>(loc: &'a T, kind: AgentTypeErrorKind<'a>) -> Self {
        AgentTypeError {
            location: loc,
            kind,
        }
    }
}

pub enum AgentTypeErrorKind<'a> {
    GenericAgentsNotSupported,
    NotAStruct,
    InvalidFields(Vec<LaneError<'a>>),
}

pub enum LaneErrorKind {
    BadLaneGenericParams,
    TupleStructsNotSupported,
    NotALaneType,
}

impl<'a> TryFrom<&'a DeriveInput> for LanesModel<'a> {
    type Error = AgentTypeError<'a>;

    fn try_from(value: &'a DeriveInput) -> Result<Self, Self::Error> {
        if !value.generics.params.is_empty() {
            return Err(AgentTypeError::new(
                &value.generics,
                AgentTypeErrorKind::GenericAgentsNotSupported,
            ));
        }
        if let Data::Struct(body) = &value.data {
            try_from_struct(&value.ident, body)
        } else {
            Err(AgentTypeError::new(
                &value.ident,
                AgentTypeErrorKind::NotAStruct,
            ))
        }
    }
}

fn try_from_struct<'a>(
    name: &'a Ident,
    definition: &'a DataStruct,
) -> Result<LanesModel<'a>, AgentTypeError<'a>> {
    definition
        .fields
        .iter()
        .append_fold(
            Validation::valid(vec![]),
            false,
            |mut lanes, field| match extract_lane_model(field) {
                Ok(lane_model) => {
                    lanes.push(lane_model);
                    Validation::valid(lanes)
                }
                Err(e) => Validation::Validated(lanes, Some(e)),
            },
        )
        .map(|lanes| LanesModel::new(name, lanes))
        .into_result()
        .map_err(|errs| {
            AgentTypeError::new(&definition.fields, AgentTypeErrorKind::InvalidFields(errs))
        })
}

const COMMAND_LANE_NAME: &str = "CommandLane";
const VALUE_LANE_NAME: &str = "ValueLane";
const MAP_LANE_NAME: &str = "MapLane";

fn extract_lane_model<'a>(field: &'a Field) -> Result<LaneModel<'a>, LaneError<'a>> {
    if let (Some(fld_name), Type::Path(TypePath { qself: None, path })) = (&field.ident, &field.ty)
    {
        if let Some(PathSegment { ident, arguments }) = path.segments.last() {
            let type_name = ident.to_string();
            match type_name.as_str() {
                COMMAND_LANE_NAME => {
                    let param = single_param(arguments)?;
                    Ok(LaneModel::new(fld_name, LaneKind::CommandLane(param)))
                }
                VALUE_LANE_NAME => {
                    let param = single_param(arguments)?;
                    Ok(LaneModel::new(fld_name, LaneKind::ValueLane(param)))
                }
                MAP_LANE_NAME => {
                    let (param1, param2) = two_params(arguments)?;
                    Ok(LaneModel::new(fld_name, LaneKind::MapLane(param1, param2)))
                }
                _ => Err(LaneError::new(&field.ty, LaneErrorKind::NotALaneType)),
            }
        } else {
            Err(LaneError::new(&field.ty, LaneErrorKind::NotALaneType))
        }
    } else {
        if field.ident.is_none() {
            Err(LaneError::new(
                field,
                LaneErrorKind::TupleStructsNotSupported,
            ))
        } else {
            Err(LaneError::new(&field.ty, LaneErrorKind::NotALaneType))
        }
    }
}

fn single_param<'a>(args: &'a PathArguments) -> Result<&'a Type, LaneError<'a>> {
    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args {
        let mut selected = None;
        let mut it = args.iter();
        while let Some(arg) = it.next() {
            if let (GenericArgument::Type(ty), None) = (arg, &selected) {
                selected = Some(ty);
            } else {
                return Err(LaneError::new(args, LaneErrorKind::BadLaneGenericParams));
            }
        }
        selected.ok_or_else(|| LaneError::new(args, LaneErrorKind::BadLaneGenericParams))
    } else {
        Err(LaneError::new(args, LaneErrorKind::BadLaneGenericParams))
    }
}

fn two_params<'a>(args: &'a PathArguments) -> Result<(&'a Type, &'a Type), LaneError<'a>> {
    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args {
        let mut first = None;
        let mut second = None;
        let mut it = args.iter();
        while let Some(arg) = it.next() {
            match (arg, &first, &second) {
                (GenericArgument::Type(ty), None, None) => {
                    first = Some(ty);
                }
                (GenericArgument::Type(ty), _, None) => {
                    second = Some(ty);
                }
                _ => {
                    return Err(LaneError::new(args, LaneErrorKind::BadLaneGenericParams));
                }
            }
        }
        first
            .zip(second)
            .ok_or_else(|| LaneError::new(args, LaneErrorKind::BadLaneGenericParams))
    } else {
        Err(LaneError::new(args, LaneErrorKind::BadLaneGenericParams))
    }
}
