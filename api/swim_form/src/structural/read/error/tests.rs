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

use crate::structural::read::error::ExpectedEvent;
use crate::structural::read::ReadError;
use swim_model::Text;
use swim_model::ValueKind;

#[test]
fn expected_event_display() {
    assert_eq!(
        format!("{}", ExpectedEvent::ValueEvent(ValueKind::Text)),
        "A value of kind Text"
    );

    assert_eq!(
        format!("{}", ExpectedEvent::Attribute(None)),
        "An attribute"
    );

    assert_eq!(
        format!("{}", ExpectedEvent::Attribute(Some(Text::new("attr")))),
        "An attribute named 'attr'"
    );

    assert_eq!(format!("{}", ExpectedEvent::RecordBody), "A record body");

    assert_eq!(format!("{}", ExpectedEvent::Slot), "A slot divider");

    assert_eq!(
        format!("{}", ExpectedEvent::EndOfRecord),
        "The end of the record body"
    );

    assert_eq!(
        format!("{}", ExpectedEvent::EndOfAttribute),
        "The end of the attribute"
    );

    let or = ExpectedEvent::Or(vec![
        ExpectedEvent::Attribute(Some(Text::new("name"))),
        ExpectedEvent::RecordBody,
    ]);
    let expected = "One of: [An attribute named 'name', A record body]";
    assert_eq!(format!("{}", or), expected);
}

#[test]
fn read_error_display() {
    assert_eq!(
        format!("{}", ReadError::Message(Text::new("Boom!"))),
        "Boom!"
    );
    assert_eq!(
        format!("{}", ReadError::IncompleteRecord),
        "The record ended before all parts of the value were deserialized."
    );
    assert_eq!(
        format!(
            "{}",
            ReadError::UnexpectedKind {
                actual: ValueKind::Extant,
                expected: Some(ExpectedEvent::RecordBody)
            }
        ),
        "Unexpected value kind: Extant, expected: A record body."
    );
    assert_eq!(
        format!(
            "{}",
            ReadError::UnexpectedKind {
                actual: ValueKind::Extant,
                expected: None
            }
        ),
        "Unexpected value kind: Extant"
    );
    assert_eq!(
        format!("{}", ReadError::UnexpectedField(Text::new("name"))),
        "Unexpected field: 'name'"
    );
    assert_eq!(
        format!("{}", ReadError::UnexpectedAttribute(Text::new("name"))),
        "Unexpected attribute: 'name'"
    );
    assert_eq!(
        format!("{}", ReadError::InconsistentState),
        "The deserialization state became corrupted."
    );
    assert_eq!(
        format!(
            "{}",
            ReadError::Malformatted {
                text: Text::new("example"),
                message: Text::new("bad")
            }
        ),
        "Text value 'example' is invalid: bad"
    );
    assert_eq!(
        format!(
            "{}",
            ReadError::MissingFields(vec![Text::new("a"), Text::new("b")])
        ),
        "Fields [a, b] are required."
    );
    assert_eq!(
        format!("{}", ReadError::NumberOutOfRange),
        "Number out of range."
    );
    assert_eq!(
        format!("{}", ReadError::MissingTag),
        "Missing tag attribute for record type."
    );
    assert_eq!(
        format!("{}", ReadError::UnexpectedItem),
        "Unexpected item in record."
    );
    assert_eq!(
        format!("{}", ReadError::ReaderUnderflow),
        "Stack underflow deserializing the value."
    );
    assert_eq!(
        format!("{}", ReadError::ReaderOverflow),
        "Record more deeply nested than expected for type."
    );
    assert_eq!(
        format!("{}", ReadError::DuplicateField(Text::new("name"))),
        "Field 'name' occurred more than once."
    );
    assert_eq!(
        format!("{}", ReadError::UnexpectedSlot),
        "Unexpected slot in record."
    );
    assert_eq!(
        format!("{}", ReadError::DoubleSlot),
        "Slot divider encountered within the value of a slot."
    );
}
