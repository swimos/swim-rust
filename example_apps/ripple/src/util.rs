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

use crate::agent::Color;
use std::fmt::Display;
use swimos::model::{Text, ValueKind};
use swimos_form::read::{
    ExpectedEvent, ReadError, ReadEvent, Recognizer, RecognizerReadable, SimpleAttrBody,
    SimpleRecBody,
};
use swimos_form::write::{PrimitiveWriter, StructuralWritable, StructuralWriter};

impl RecognizerReadable for Color {
    type Rec = ColorRecognizer;
    type AttrRec = SimpleAttrBody<Self::Rec>;
    type BodyRec = SimpleRecBody<Self::Rec>;

    fn make_recognizer() -> Self::Rec {
        ColorRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(ColorRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(ColorRecognizer)
    }
}

impl StructuralWritable for Color {
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

#[doc(hidden)]
pub struct ColorRecognizer;

impl Recognizer for ColorRecognizer {
    type Target = Color;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                Some(
                    Color::try_from(txt.as_ref()).map_err(|_| ReadError::Malformatted {
                        text: txt.into(),
                        message: Text::new("Not a valid color."),
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

pub struct ColorParseErr;

impl TryFrom<&str> for Color {
    type Error = ColorParseErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "#80dc1a" => Ok(Color::Green),
            "#c200fa" => Ok(Color::Magenta),
            "#56dbb6" => Ok(Color::Cyan),
            _ => Err(ColorParseErr),
        }
    }
}

impl Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Color {
    fn as_ref(&self) -> &str {
        match self {
            Color::Green => "#80dc1a",
            Color::Magenta => "#c200fa",
            Color::Cyan => "#56dbb6",
        }
    }
}
