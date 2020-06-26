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

use std::marker::PhantomData;
use std::fmt::{Debug, Formatter};
use std::any::type_name;

/// Model for a lane that can receive commands and optionally produce responses. It is entirely
/// stateless so has no fields.
pub struct ActionLaneModel<Command, Response>(PhantomData<fn(Command) -> Response>);

/// An action lane model that produces no response.
pub type CommandLaneModel<Command> = ActionLaneModel<Command, ()>;

impl<Command, Response> Default for ActionLaneModel<Command, Response> {
    fn default() -> Self {
        ActionLaneModel(PhantomData)
    }
}

impl<Command, Response> Debug for ActionLaneModel<Command, Response> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let cmd_name = type_name::<Command>();
        let resp_name = type_name::<Response>();
        if resp_name == type_name::<()>() {
            write!(f, "CommandLaneModel({})", cmd_name)
        } else {
            write!(f, "ActionLaneModel({} -> {})", cmd_name, resp_name)
        }
    }
}
