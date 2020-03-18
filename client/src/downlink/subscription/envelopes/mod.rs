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

use crate::downlink::model::map::MapModification;
use crate::downlink::model::value::SharedValue;
use crate::downlink::Command;
use common::model::Value;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use deserialize::FormDeserializeErr;
use std::sync::Arc;

pub enum EnvInterpError {
    MissingBody,
    BadMessageKind,
    InvalidBody(FormDeserializeErr),
}

pub fn envelope_for<T, F>(
    to_body: F,
    path: &AbsolutePath,
    command: Command<T>,
) -> (String, Envelope)
where
    F: Fn(T) -> Option<Value>,
{
    let host = path.host.clone();
    let node = path.node.clone();
    let lane = path.lane.clone();
    (
        host,
        match command {
            Command::Sync => Envelope::sync(node, lane),
            Command::Action(v) => Envelope::command(node, lane, to_body(v)),
            Command::Unlink => Envelope::unlink(node, lane),
        },
    )
}

pub fn value_envelope(path: &AbsolutePath, command: Command<SharedValue>) -> (String, Envelope) {
    envelope_for(value::envelope_body, path, command)
}

pub fn map_envelope(
    path: &AbsolutePath,
    command: Command<MapModification<Arc<Value>>>,
) -> (String, Envelope) {
    envelope_for(map::envelope_body, path, command)
}

pub mod value {
    use crate::downlink::model::value::SharedValue;
    use crate::downlink::subscription::envelopes::EnvInterpError;
    use crate::downlink::Message;
    use common::model::Value;
    use common::warp::envelope::{Envelope, LaneAddressed};

    pub fn envelope_body(v: SharedValue) -> Option<Value> {
        Some((*v).clone())
    }

    pub fn try_from_envelope(env: Envelope) -> Result<Message<Value>, EnvInterpError> {
        match env {
            Envelope::LinkedResponse(_) => Ok(Message::Linked),
            Envelope::SyncedResponse(_) => Ok(Message::Synced),
            Envelope::UnlinkedResponse(_) => Ok(Message::Unlinked),
            Envelope::EventMessage(LaneAddressed {
                body: Some(body), ..
            }) => Ok(Message::Action(body)),
            Envelope::EventMessage(_) => Err(EnvInterpError::MissingBody),
            _ => Err(EnvInterpError::BadMessageKind),
        }
    }
}

pub mod map {
    use crate::downlink::model::map::MapModification;
    use crate::downlink::subscription::envelopes::EnvInterpError;
    use crate::downlink::Message;
    use common::model::Value;
    use common::warp::envelope::{Envelope, LaneAddressed};
    use form::Form;
    use std::sync::Arc;

    pub(super) fn envelope_body(cmd: MapModification<Arc<Value>>) -> Option<Value> {
        Some(cmd.envelope_body())
    }

    pub fn try_from_envelope(
        env: Envelope,
    ) -> Result<Message<MapModification<Value>>, EnvInterpError> {
        match env {
            Envelope::LinkedResponse(_) => Ok(Message::Linked),
            Envelope::SyncedResponse(_) => Ok(Message::Synced),
            Envelope::UnlinkedResponse(_) => Ok(Message::Unlinked),
            Envelope::EventMessage(LaneAddressed {
                body: Some(body), ..
            }) => match Form::try_convert(body) {
                Ok(modification) => Ok(Message::Action(modification)),
                Err(e) => Err(EnvInterpError::InvalidBody(e)),
            },
            Envelope::EventMessage(_) => Err(EnvInterpError::MissingBody),
            _ => Err(EnvInterpError::BadMessageKind),
        }
    }
}
