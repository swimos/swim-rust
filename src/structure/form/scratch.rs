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

use std::borrow::{Borrow, BorrowMut};
use crate::model::{Attr, Item, Value};

#[derive(Copy, Clone, Debug)]
enum State {
    ReadingAttribute,
    ReadingSequence,
    None,
}

#[derive(Debug)]
struct Serializer {
    current_state: SerializerState,
    stack: Vec<SerializerState>,
}

#[derive(Debug, Clone)]
struct SerializerState {
    output: Value,
    state: State,
}

impl Serializer {
    fn output(&mut self) -> Value {
        self.current_state.output.to_owned()
    }

    fn push_state(&mut self, ss: SerializerState) {
        self.stack.push(self.current_state.clone());
        self.current_state = ss;
    }

    fn pop_state(&mut self) -> SerializerState {
        match self.stack.pop() {
            Some(s) => s,
            None => panic!("Stack underflow")
        }
    }

    fn push_value(&mut self, value: Value) {
        match &mut self.current_state.output {
            Value::Record(_, ref mut items) => {
                items.push(Item::ValueItem(value))
            }
            _ => {
                panic!("...")
            }
        }
    }

    fn enter_sequence(&mut self) {
        let ss = SerializerState {
            output: Value::Record(Vec::new(), Vec::new()),
            state: State::ReadingSequence,
        };

        self.push_state(ss);
    }

    fn exit_sequence(&mut self) {
        let mut previous_state = self.pop_state();

        match self.current_state.state {
            State::ReadingSequence => {
                match previous_state.output {
                    Value::Record(_, ref mut items) => {
                        items.push(Item::ValueItem(self.current_state.clone().output));
                        self.current_state = previous_state.to_owned();
                    }
                    _ => {
                        panic!("...")
                    }
                }
            }
            _ => {
                panic!("...")
            }
        }
    }
}

#[test]
fn test_sequence() {
    let vector = vec![
        vec!["a"],
        vec!["b"]
    ];
}

#[test]
fn vecs() {
    let mut serializer = Serializer {
        current_state: SerializerState {
            output: Value::Record(Vec::new(), Vec::new()),
            state: State::ReadingSequence,
        },
        stack: vec![],
    };

    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(1));
    serializer.push_value(Value::Int32Value(2));
    serializer.exit_sequence();

    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(3));
    serializer.push_value(Value::Int32Value(4));
    serializer.exit_sequence();

    println!("{:?}", serializer.output());
}

#[test]
fn nested() {
    let mut serializer = Serializer {
        current_state: SerializerState {
            output: Value::Record(Vec::new(), Vec::new()),
            state: State::ReadingSequence,
        },
        stack: vec![],
    };

    serializer.enter_sequence();

    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(1));
    serializer.push_value(Value::Int32Value(2));
    serializer.exit_sequence();

    serializer.enter_sequence();
    serializer.push_value(Value::Int32Value(3));
    serializer.push_value(Value::Int32Value(4));
    serializer.exit_sequence();

    serializer.exit_sequence();

    println!("{:?}", serializer.output());
}