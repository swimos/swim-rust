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

pub mod value;
pub mod map;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DownlinkState {
    Unlinked,
    Linked,
    Synced,
}

struct DownlinkTask<E> {
    join_handle: JoinHandle<Result<(), E>>,
    stop_trigger: oneshot::Sender<()>,
}

impl<E> DownlinkTask<E> {
    async fn stop(self) -> Result<(), E> {
        match self.stop_trigger.send(()) {
            Ok(_) => match self.join_handle.await {
                Ok(r) => r,
                Err(_) => Ok(()), //TODO Ignoring the case where the downlink task panicked. Can maybe do better?
            },
            Err(_) => Ok(()),
        }
    }
}
