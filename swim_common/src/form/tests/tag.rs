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

use crate::form::Tag;

mod swim_common {
    pub use crate::*;
}

#[test]
fn test_derivation() {
    #[derive(Tag, Debug, PartialEq)]
    enum Level {
        Info,
        Warn,
        Error,
    }

    assert_eq!(
        Level::enumerated(),
        vec![Level::Info, Level::Warn, Level::Error]
    );

    assert_eq!("info", Level::Info.as_string());
    assert_eq!("warn", Level::Warn.as_string());
    assert_eq!("error", Level::Error.as_string());

    assert_eq!(Ok(Level::Info), Level::from_string("info".to_string()));
    assert_eq!(Ok(Level::Warn), Level::from_string("warn".to_string()));
    assert_eq!(Ok(Level::Error), Level::from_string("error".to_string()));
    assert_eq!(Ok(Level::Error), Level::from_string("ERROR".to_string()));

    assert_eq!(Err(()), Level::from_string("trace".to_string()));
}
