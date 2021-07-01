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

use std::borrow::Borrow;
use swim_common::form::structural::read::event::ReadEvent;
use swim_common::form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody,
};
use swim_common::form::structural::read::ReadError;
use swim_common::form::structural::StringRepresentable;
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::text::Text;
use swim_common::model::ValueKind;

#[test]
fn header_body_replace() {
    #[derive(Clone)]
    enum Level {
        Info,
        Trace,
    }

    struct LevelRec;

    impl RecognizerReadable for Level {
        type Rec = LevelRec;
        type AttrRec = SimpleAttrBody<LevelRec>;

        fn make_recognizer() -> Self::Rec {
            LevelRec
        }

        fn make_attr_recognizer() -> Self::AttrRec {
            SimpleAttrBody::new(LevelRec)
        }
    }

    impl Recognizer for LevelRec {
        type Target = Level;

        fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
            match input {
                ReadEvent::TextValue(txt) => match txt.borrow() {
                    "Info" => Some(Ok(Level::Info)),
                    "Trace" => Some(Ok(Level::Trace)),
                    _ => Some(Err(ReadError::Malformatted {
                        text: txt.into(),
                        message: Text::new("Possible values are 'Info' and 'Trace'"),
                    })),
                },
                ow => Some(Err(ow.kind_error())),
            }
        }

        fn reset(&mut self) {}
    }

    impl AsRef<str> for Level {
        fn as_ref(&self) -> &str {
            match self {
                Level::Info => "Info",
                Level::Trace => "Trace",
            }
        }
    }

    const LEVEL_UNIVERSE: [&str; 2] = ["Info", "Trace"];

    impl StringRepresentable for Level {
        fn try_from_str(txt: &str) -> Result<Self, Text> {
            match txt {
                "Info" => Ok(Level::Info),
                "Trace" => Ok(Level::Trace),
                _ => Err(Text::new("Possible values are 'Info' and 'Trace'")),
            }
        }

        fn universe() -> &'static [&'static str] {
            &LEVEL_UNIVERSE
        }
    }

    #[derive(Form, ValidatedForm)]
    #[form(schema(all_items(of_kind(ValueKind::Int32))))]
    struct S {
        #[form(tag)]
        level: Level,
        a: i32,
        b: i32,
    }
}
