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

use bytes::Bytes;
use swim_model::Text;
use uuid::Uuid;

use crate::event_handler::EventHandler;

pub trait OnReceive<'a, Context>: Send {
    type OnCommandHandler: EventHandler<Context, Completion = ()> + Send + 'a;
    type OnSyncHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_command(&'a mut self, lane: Text, body: Bytes) -> Self::OnCommandHandler;
    fn on_sync(&'a mut self, lane: Text, id: Uuid) -> Self::OnSyncHandler;
}