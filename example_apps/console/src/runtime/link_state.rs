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

use std::{collections::BTreeMap, fmt::Debug};

use swimos_agent_protocol::{peeling::extract_header_str, MapMessage};
use swimos_model::Value;
use swimos_recon::{
    parser::{parse_recognize, MessageExtractError, ParseError},
    print_recon,
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

    fn sync(&mut self);

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
    synced: bool,
}

impl LinkState for EventLink {
    fn update(&mut self, data: &str) -> Result<(), LinkStateError> {
        self.state = Some(data.to_string());
        Ok(())
    }

    fn snapshot(&self) -> Vec<String> {
        let mut messages = vec![];
        if let Some(value) = self.state.as_ref() {
            if self.synced {
                messages.push("Link is synced.".to_string());
            }
            messages.push(format!("State => {}", value));
        }
        messages
    }

    fn sync(&mut self) {
        self.synced = true;
    }
}

#[derive(Default, Debug)]
struct MapLink {
    state: BTreeMap<Value, Value>,
    synced: bool,
}

impl LinkState for MapLink {
    fn update(&mut self, data: &str) -> Result<(), LinkStateError> {
        match extract_header_str(data)? {
            MapMessage::Update { key, value } => {
                let k = parse_recognize(key, false)?;
                let v = parse_recognize(value, false)?;
                self.state.insert(k, v);
            }
            MapMessage::Remove { key } => {
                let k = parse_recognize(key, false)?;
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

    fn sync(&mut self) {
        self.synced = true;
    }

    fn snapshot(&self) -> Vec<String> {
        let mut messages = vec![];
        if self.synced {
            messages.push("Link is synced.".to_string());
        }
        messages.extend(
            self.state
                .iter()
                .map(|(k, v)| format!("{} => {}", print_recon(k), print_recon(v))),
        );
        messages
    }
}
