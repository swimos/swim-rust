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

use futures::future::{ready, Ready};
use futures::stream::{empty, Empty};
use stm::transaction::RetryManager;

pub struct ExactlyOnce;

impl RetryManager for ExactlyOnce {
    type ContentionManager = Empty<()>;
    type RetryFut = Ready<bool>;

    fn contention_manager(&self) -> Self::ContentionManager {
        empty()
    }

    fn retry(&mut self) -> Self::RetryFut {
        ready(false)
    }
}
