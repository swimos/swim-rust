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

use std::borrow::Borrow;
use std::time::Duration;

use swim_model::{Text, ValueKind};

use crate::structural::read::error::ExpectedEvent;
use crate::structural::read::event::ReadEvent;
use crate::structural::read::recognizer::primitive::{U32Recognizer, U64Recognizer};
use crate::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::structural::read::ReadError;
use crate::structural::tags::{DURATION_TAG, NANOS_TAG, SECS_TAG};

#[derive(Default)]
pub struct DurationRecognizer {
    stage: DurationStage,
    secs: Option<u64>,
    nanos: Option<u32>,
}

#[derive(Default)]
enum DurationStage {
    #[default]
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(DurationField),
    Field(DurationField),
}

#[derive(Clone, Copy)]
enum DurationField {
    Secs,
    Nanos,
}

impl RecognizerReadable for Duration {
    type Rec = DurationRecognizer;
    type AttrRec = SimpleAttrBody<DurationRecognizer>;
    type BodyRec = SimpleRecBody<DurationRecognizer>;

    fn make_recognizer() -> Self::Rec {
        DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        })
    }
}

impl Recognizer for DurationRecognizer {
    type Target = Duration;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            DurationStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == DURATION_TAG {
                        self.stage = DurationStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(DURATION_TAG),
                    )))))
                }
            }
            DurationStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = DurationStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            DurationStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = DurationStage::InBody;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            DurationStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    SECS_TAG => {
                        self.stage = DurationStage::Slot(DurationField::Secs);
                        None
                    }
                    NANOS_TAG => {
                        self.stage = DurationStage::Slot(DurationField::Nanos);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(Duration::new(
                    self.secs.unwrap_or_default(),
                    self.nanos.unwrap_or_default(),
                ))),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            DurationStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = DurationStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            DurationStage::Field(DurationField::Secs) => match U64Recognizer.feed_event(input) {
                Some(Ok(n)) => {
                    self.secs = Some(n);
                    self.stage = DurationStage::InBody;
                    None
                }
                Some(Err(e)) => Some(Err(e)),
                _ => Some(Err(ReadError::InconsistentState)),
            },
            DurationStage::Field(DurationField::Nanos) => match U32Recognizer.feed_event(input) {
                Some(Ok(n)) => {
                    self.nanos = Some(n);
                    self.stage = DurationStage::InBody;
                    None
                }
                Some(Err(e)) => Some(Err(e)),
                _ => Some(Err(ReadError::InconsistentState)),
            },
        }
    }

    fn reset(&mut self) {
        let DurationRecognizer { stage, secs, nanos } = self;
        *stage = DurationStage::Init;
        *secs = None;
        *nanos = None;
    }
}
