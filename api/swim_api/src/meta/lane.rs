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

use std::borrow::Borrow;
use std::fmt::{Display, Formatter};

use swim_form::structural::read::error::ExpectedEvent;
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use swim_form::structural::read::ReadError;
use swim_form::structural::write::{PrimitiveWriter, StructuralWritable, StructuralWriter};
use swim_form::structural::Tag;
use swim_form::Form;
use swim_model::{Text, ValueKind};

use crate::agent::UplinkKind;

/// An enumeration representing the type of a lane.
#[derive(Tag, Debug, PartialEq, Eq, Clone, Copy)]
#[form_root(::swim_form)]
pub enum LaneKind {
    Action,
    Command,
    Demand,
    DemandMap,
    Map,
    JoinMap,
    JoinValue,
    Supply,
    Spatial,
    Value,
}

impl LaneKind {
    pub fn uplink_kind(&self) -> UplinkKind {
        match self {
            LaneKind::Map | LaneKind::DemandMap | LaneKind::JoinMap => UplinkKind::Map,
            LaneKind::Supply => UplinkKind::Supply,
            LaneKind::Spatial => todo!("Spatial uplinks not supported."),
            _ => UplinkKind::Value,
        }
    }
}

/// Lane information metadata that can be retrieved when syncing to
/// `/swim:meta:node/percent-encoded-nodeuri/lanes`.
///
/// E.g: `swim:meta:node/unit%2Ffoo/lanes/`
#[derive(Debug, Clone, PartialEq, Eq, Form)]
#[form_root(::swim_form)]
pub struct LaneInfo {
    /// The URI of the lane.
    #[form(name = "laneUri")]
    lane_uri: Text,
    /// The type of the lane.
    #[form(name = "laneType")]
    lane_type: LaneKind,
}

impl LaneInfo {
    pub fn new<L>(lane_uri: L, lane_type: LaneKind) -> Self
    where
        L: Into<Text>,
    {
        LaneInfo {
            lane_uri: lane_uri.into(),
            lane_type,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct LaneKindParseErr;

impl<'a> TryFrom<&'a str> for LaneKind {
    type Error = LaneKindParseErr;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "Action" => Ok(LaneKind::Action),
            "Command" => Ok(LaneKind::Command),
            "Demand" => Ok(LaneKind::Demand),
            "DemandMap" => Ok(LaneKind::DemandMap),
            "Map" => Ok(LaneKind::Map),
            "JoinMap" => Ok(LaneKind::JoinMap),
            "JoinValue" => Ok(LaneKind::JoinValue),
            "Supply" => Ok(LaneKind::Supply),
            "Spatial" => Ok(LaneKind::Spatial),
            "Value" => Ok(LaneKind::Value),
            _ => Err(LaneKindParseErr),
        }
    }
}

impl Display for LaneKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_str: &str = self.as_ref();
        write!(f, "{}", as_str)
    }
}

pub struct LaneKindRecognizer;

impl Recognizer for LaneKindRecognizer {
    type Target = LaneKind;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                Some(
                    LaneKind::try_from(txt.borrow()).map_err(|_| ReadError::Malformatted {
                        text: txt.into(),
                        message: Text::new("Not a valid Lane kind."),
                    }),
                )
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl RecognizerReadable for LaneKind {
    type Rec = LaneKindRecognizer;
    type AttrRec = SimpleAttrBody<LaneKindRecognizer>;
    type BodyRec = SimpleRecBody<LaneKindRecognizer>;

    fn make_recognizer() -> Self::Rec {
        LaneKindRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(LaneKindRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(LaneKindRecognizer)
    }
}

impl StructuralWritable for LaneKind {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn num_attributes(&self) -> usize {
        0
    }
}
