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

use crate::downlink::model::map::UntypedMapModification;
use crate::downlink::model::value::SharedValue;
use crate::downlink::Command;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_warp::envelope::RequestEnvelope;

#[cfg(test)]
mod tests;

/// Convert a downlink [`Command`] into a Warp [`RequestEnvelope`].
fn envelope_for<T, F>(to_body: F, path: RelativePath, command: Command<T>) -> RequestEnvelope
where
    F: Fn(T) -> Value,
{
    match command {
        Command::Sync => RequestEnvelope::Sync(path, Default::default(), None),
        Command::Link => RequestEnvelope::Link(path, Default::default(), None),
        Command::Action(v) => RequestEnvelope::Command(path, Some(to_body(v))),
        Command::Unlink => RequestEnvelope::Unlink(path, None),
    }
}

/// Convert a downlink [`Command`], from a value downlink, into a Warp [`RequestEnvelope`].
pub fn value_envelope(path: RelativePath, command: Command<SharedValue>) -> RequestEnvelope {
    envelope_for(value::envelope_body, path, command)
}

/// Convert a downlink [`Command`], from a map downlink, into a Warp [`RequestEnvelope`].
pub fn map_envelope(
    path: RelativePath,
    command: Command<UntypedMapModification<Value>>,
) -> RequestEnvelope {
    envelope_for(map::envelope_body, path, command)
}

/// Convert a downlink [`Command`], from a command downkink, into a Warp [`RequestEnvelope`].
pub fn command_envelope(path: RelativePath, command: Command<Value>) -> RequestEnvelope {
    envelope_for(|v| v, path, command)
}

/// Convert a downlink [`Command`], from a event downlink, into a Warp [`RequestEnvelope`].
pub fn dummy_envelope(path: RelativePath, command: Command<()>) -> RequestEnvelope {
    envelope_for(|_| Value::Extant, path, command)
}

pub(in crate::downlink) mod value {
    use crate::downlink::model::value::SharedValue;
    use crate::downlink::Message;
    use swim_model::Value;
    use swim_warp::envelope::ResponseEnvelope;

    pub(in crate::downlink) fn envelope_body(v: SharedValue) -> Value {
        (*v).clone()
    }

    pub(in crate::downlink) fn from_envelope(incoming: ResponseEnvelope) -> Message<Value> {
        match incoming {
            ResponseEnvelope::Linked(..) => Message::Linked,
            ResponseEnvelope::Synced(..) => Message::Synced,
            ResponseEnvelope::Unlinked(..) => Message::Unlinked,
            ResponseEnvelope::Event(_, Some(body)) => Message::Action(body),
            _ => Message::Action(Value::Extant),
        }
    }
}

pub(in crate::downlink) mod map {
    use crate::downlink::model::map::UntypedMapModification;
    use crate::downlink::Message;
    use swim_form::Form;
    use swim_model::Value;
    use swim_warp::envelope::ResponseEnvelope;
    use tracing::warn;

    pub(super) fn envelope_body(cmd: UntypedMapModification<Value>) -> Value {
        Form::into_value(cmd)
    }

    pub(in crate::downlink) fn from_envelope(
        incoming: ResponseEnvelope,
    ) -> Message<UntypedMapModification<Value>> {
        match incoming {
            ResponseEnvelope::Linked(..) => Message::Linked,
            ResponseEnvelope::Synced(..) => Message::Synced,
            ResponseEnvelope::Unlinked(..) => Message::Unlinked,
            ResponseEnvelope::Event(_, Some(body)) => match Form::try_convert(body) {
                Ok(modification) => Message::Action(modification),
                Err(e) => Message::BadEnvelope(format!("{}", e)),
            },
            _ => {
                warn!("Bad envelope: {:?}", incoming);
                Message::BadEnvelope("Event envelope had no body.".to_string())
            }
        }
    }
}
