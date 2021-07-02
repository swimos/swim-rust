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

use swim_common::form::structural::StringRepresentable;
use swim_common::form::Form;
use swim_common::model::text::Text;
use swim_common::model::time::Timestamp;

#[test]
fn header_body_replace() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Level {
        Trace,
        Error,
    }

    impl AsRef<str> for Level {
        fn as_ref(&self) -> &str {
            match self {
                Level::Trace => "Trace",
                Level::Error => "Error",
            }
        }
    }

    const LEVEL_UNIVERSE: [&str; 2] = ["Trace", "Error"];

    impl StringRepresentable for Level {
        fn try_from_str(txt: &str) -> Result<Self, Text> {
            match txt {
                "Trace" => Ok(Level::Trace),
                "Error" => Ok(Level::Error),
                _ => Err(Text::new("Possible values are 'Trace' and 'Error'")),
            }
        }

        fn universe() -> &'static [&'static str] {
            &LEVEL_UNIVERSE
        }
    }

    #[derive(Debug, PartialEq, Clone)]
    struct LogEntry<F: Form> {
        //#[form(tag)]
        level: Level,
        //#[form(header)]
        time: Timestamp,
        message: F,
    }
}
