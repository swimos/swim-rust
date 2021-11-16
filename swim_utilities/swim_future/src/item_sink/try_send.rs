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

use tokio::sync::mpsc;

pub struct TrySendError<I>(pub I);

pub trait TrySend<T> {
    type Error;

    /// Attempt to send an item into the sink.
    fn try_send_item(&mut self, value: T) -> Result<(), Self::Error>;
}

impl<T> TrySend<T> for mpsc::Sender<T> {
    type Error = mpsc::error::TrySendError<T>;

    fn try_send_item(&mut self, value: T) -> Result<(), Self::Error> {
        self.try_send(value)
    }
}
