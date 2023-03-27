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

use std::{collections::BTreeMap, fmt::Debug};

use swim_api::protocol::map::{extract_header_str, MapMessage};
use swim_model::Value;
use swim_recon::{
    parser::{parse_value, MessageExtractError, ParseError},
    printer::print_recon,
};

#[derive(Debug)]
pub struct LinkStateError(pub String);

impl From<MessageExtractError> for LinkStateError {
    fn from(value: MessageExtractError) -> Self {
        LinkStateError(format!("Bad map message: {}", value))
    }
}

impl From<ParseError> for LinkStateError {
    fn from(value: ParseError) -> Self {
        LinkStateError(format!("Bad recon: {}", value))
    }
}

pub trait LinkState: Debug + Send + Sync {
    fn update(&mut self, data: &str) -> Result<(), LinkStateError>;

    fn snapshot(&self) -> Vec<String>;
}

pub type BoxLinkState = Box<dyn LinkState>;

pub fn event_link() -> BoxLinkState {
    Box::<EventLink>::default()
}

pub fn map_link() -> BoxLinkState {
    Box::<MapLink>::default()
}

#[derive(Default, Debug)]
struct EventLink {
    state: Option<String>,
}

impl LinkState for EventLink {
    fn update(&mut self, data: &str) -> Result<(), LinkStateError> {
        self.state = Some(data.to_string());
        Ok(())
    }

    fn snapshot(&self) -> Vec<String> {
        self.state
            .as_ref()
            .map(|value| format!("State = {}", value))
            .into_iter()
            .collect()
    }
}

#[derive(Default, Debug)]
struct MapLink {
    state: BTreeMap<Value, Value>,
}

impl LinkState for MapLink {
    fn update(&mut self, data: &str) -> Result<(), LinkStateError> {
        match extract_header_str(data)? {
            MapMessage::Update { key, value } => {
                let k = parse_value(key, false)?;
                let v = parse_value(value, false)?;
                self.state.insert(k, v);
            }
            MapMessage::Remove { key } => {
                let k = parse_value(key, false)?;
                self.state.remove(&k);
            }
            MapMessage::Clear => {
                self.state.clear();
            }
            MapMessage::Take(n) => {
                let n = n as usize;
                self.state = std::mem::take(&mut self.state)
                    .into_iter()
                    .take(n)
                    .collect();
            }
            MapMessage::Drop(n) => {
                let n = n as usize;
                self.state = std::mem::take(&mut self.state)
                    .into_iter()
                    .skip(n)
                    .collect();
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Vec<String> {
        self.state
            .iter()
            .map(|(k, v)| format!("{} => {}", print_recon(k), print_recon(v)))
            .collect()
    }
}
