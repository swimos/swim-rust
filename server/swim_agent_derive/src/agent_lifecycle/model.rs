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

use std::collections::{HashMap, HashSet};

use swim_utilities::errors::{validation::Validation, Errors};
use syn::{Ident, Path, Type, Item};

pub fn validate_input<'a>(
    agent_type: &'a Path,
    item: &'a Item,
) -> Validation<AgentLifecycleDescriptor<'a>, Errors<syn::Error>> {
    todo!()
}

pub struct AgentLifecycleDescriptor<'a> {
    pub agent_type: &'a Path,
    pub lifecycle_type: &'a Type,
    pub on_start: Option<&'a Ident>,
    pub on_stop: Option<&'a Ident>,
    pub lane_lifecycles: HashMap<&'a Ident, LaneLifecycle<'a>>,
}

const DUPLICATE_ON_STOP: &str = "Duplicate on_stop event handler.";
const DUPLICATE_ON_START: &str = "Duplicate on_start event handler.";

impl<'a> AgentLifecycleDescriptor<'a> {
    pub fn new(agent_type: &'a Path, lifecycle_type: &'a Type) -> Self {
        AgentLifecycleDescriptor {
            agent_type,
            lifecycle_type,
            on_start: None,
            on_stop: None,
            lane_lifecycles: HashMap::new(),
        }
    }

    pub fn add_on_stop(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor { on_stop, .. } = self;
        if on_stop.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_STOP))
        } else {
            *on_stop = Some(method);
            Ok(())
        }
    }

    pub fn add_on_start(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor { on_start, .. } = self;
        if on_start.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_START))
        } else {
            *on_start = Some(method);
            Ok(())
        }
    }

    pub fn add_on_command(&mut self, name: &'a Ident, handler_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Duplicate on_command handler for '{}'.", name)))
            }
            Some(LaneLifecycle::Value(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and value lane event handlers.", name)))
            },
            Some(LaneLifecycle::Map(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and map lane event handlers.", name)))
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Command(CommandLifecycleDescriptor::new(name, handler_type, method)));
                Ok(())
            }
        } 
    }

    pub fn add_on_event(&mut self, name: &'a Ident, handler_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> { 
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get_mut(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and value lane event handlers.", name)))
            }
            Some(LaneLifecycle::Value(desc)) => {
                desc.add_on_event(handler_type, method)
            },
            Some(LaneLifecycle::Map(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both value and map lane event handlers.", name)))
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Value(ValueLifecycleDescriptor::new_on_event(name, handler_type, method)));
                Ok(())
            }
        }
    }

    pub fn add_on_set(&mut self, name: &'a Ident, handler_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> { 
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get_mut(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and value lane event handlers.", name)))
            }
            Some(LaneLifecycle::Value(desc)) => {
                desc.add_on_set(handler_type, method)
            },
            Some(LaneLifecycle::Map(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both value and map lane event handlers.", name)))
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Value(ValueLifecycleDescriptor::new_on_set(name, handler_type, method)));
                Ok(())
            }
        }
    }

    pub fn add_on_update(&mut self, name: &'a Ident, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> { 
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get_mut(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and map lane event handlers.", name)))
            }
            Some(LaneLifecycle::Value(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both value and map lane event handlers.", name)))
            },
            Some(LaneLifecycle::Map(desc)) => {
                desc.add_on_update(key_type, value_type, method)
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Map(MapLifecycleDescriptor::new_on_update(name, key_type, value_type, method)));
                Ok(())
            }
        }
    }

    pub fn add_on_remove(&mut self, name: &'a Ident, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> { 
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get_mut(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and map lane event handlers.", name)))
            }
            Some(LaneLifecycle::Value(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both value and map lane event handlers.", name)))
            },
            Some(LaneLifecycle::Map(desc)) => {
                desc.add_on_remove(key_type, value_type, method)
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Map(MapLifecycleDescriptor::new_on_remove(name, key_type, value_type, method)));
                Ok(())
            }
        }
    }

    pub fn add_on_clear(&mut self, name: &'a Ident, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> { 
        let AgentLifecycleDescriptor { lane_lifecycles, .. } = self;
        match lane_lifecycles.get_mut(name) {
            Some(LaneLifecycle::Command(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both command and map lane event handlers.", name)))
            }
            Some(LaneLifecycle::Value(_)) => {
                Err(syn::Error::new_spanned(method, format!("Lane '{}' has both value and map lane event handlers.", name)))
            },
            Some(LaneLifecycle::Map(desc)) => {
                desc.add_on_clear(key_type, value_type, method)
            },
            _ => {
                lane_lifecycles.insert(name, LaneLifecycle::Map(MapLifecycleDescriptor::new_on_clear(name, key_type, value_type, method)));
                Ok(())
            }
        }
    }

}

pub enum LaneLifecycle<'a> {
    Value(ValueLifecycleDescriptor<'a>),
    Command(CommandLifecycleDescriptor<'a>),
    Map(MapLifecycleDescriptor<'a>),
}

pub struct ValueLifecycleDescriptor<'a> {
    name: &'a Ident,
    primary_lane_type: &'a Type,
    alternative_lane_types: HashSet<&'a Type>,
    on_event: Option<&'a Ident>,
    on_set: Option<&'a Ident>,
}

impl<'a> ValueLifecycleDescriptor<'a> {

    pub fn new_on_event(name: &'a Ident, primary_lane_type: &'a Type, on_event: &'a Ident) -> Self {
        ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types: Default::default(),
            on_event: Some(on_event),
            on_set: None
        }
    }

    pub fn new_on_set(name: &'a Ident, primary_lane_type: &'a Type, on_set: &'a Ident) -> Self {
        ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types: Default::default(),
            on_event: None,
            on_set: Some(on_set),
        }
    }

    pub fn add_on_event(&mut self, lane_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let ValueLifecycleDescriptor { name, primary_lane_type, alternative_lane_types, on_event, .. } = self;
        if on_event.is_some() {
            Err(syn::Error::new_spanned(method, format!("Duplicate on_event handler for '{}'.", name)))
        } else {
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_event = Some(method);
            Ok(())
        }
    }

    pub fn add_on_set(&mut self, lane_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let ValueLifecycleDescriptor { name, primary_lane_type, alternative_lane_types, on_set, .. } = self;
        if on_set.is_some() {
            Err(syn::Error::new_spanned(method, format!("Duplicate on_set handler for '{}'.", name)))
        } else {
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_set = Some(method);
            Ok(())
        }
    }

}

pub struct CommandLifecycleDescriptor<'a> {
    name: &'a Ident,
    primary_lane_type: &'a Type,
    on_command: &'a Ident,
}

impl<'a> CommandLifecycleDescriptor<'a> {

    pub fn new(name: &'a Ident, primary_lane_type: &'a Type, on_command: &'a Ident) -> Self {
        CommandLifecycleDescriptor { name, primary_lane_type, on_command }
    }

}

#[derive(PartialEq, Eq, Hash)]
pub struct MapType<'a> {
    key_type: &'a Type,
    value_type: &'a Type,
}

pub struct MapLifecycleDescriptor<'a> {
    name: &'a Ident,
    primary_lane_type: MapType<'a>,
    alternative_lane_types: HashSet<MapType<'a>>,
    on_update: Option<&'a Ident>,
    on_remove: Option<&'a Ident>,
    on_clear: Option<&'a Ident>,
}

impl<'a> MapLifecycleDescriptor<'a> {
    pub fn new_on_update(name: &'a Ident, key_type: &'a Type, value_type: &'a Type, on_update: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: MapType { key_type, value_type },
            alternative_lane_types: Default::default(),
            on_update: Some(on_update),
            on_remove: None,
            on_clear: None,
        }
    }

    pub fn new_on_remove(name: &'a Ident, key_type: &'a Type, value_type: &'a Type, on_remove: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: MapType { key_type, value_type },
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: Some(on_remove),
            on_clear: None,
        }
    }

    pub fn new_on_clear(name: &'a Ident, key_type: &'a Type, value_type: &'a Type, on_clear: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: MapType { key_type, value_type },
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: None,
            on_clear: Some(on_clear),
        }
    }

    pub fn add_on_update(&mut self, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor { name, primary_lane_type, alternative_lane_types, on_update, .. } = self;
        if on_update.is_some() {
            Err(syn::Error::new_spanned(method, format!("Duplicate on_update handler for '{}'.", name)))
        } else {
            let lane_type = MapType { key_type, value_type };
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_update = Some(method);
            Ok(())
        }
    }

    pub fn add_on_remove(&mut self, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor { name, primary_lane_type, alternative_lane_types, on_remove, .. } = self;
        if on_remove.is_some() {
            Err(syn::Error::new_spanned(method, format!("Duplicate on_remove handler for '{}'.", name)))
        } else {
            let lane_type = MapType { key_type, value_type };
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_remove = Some(method);
            Ok(())
        }
    }

    pub fn add_on_clear(&mut self, key_type: &'a Type, value_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor { name, primary_lane_type, alternative_lane_types, on_clear, .. } = self;
        if on_clear.is_some() {
            Err(syn::Error::new_spanned(method, format!("Duplicate on_clear handler for '{}'.", name)))
        } else {
            let lane_type = MapType { key_type, value_type };
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_clear = Some(method);
            Ok(())
        }
    }
}
