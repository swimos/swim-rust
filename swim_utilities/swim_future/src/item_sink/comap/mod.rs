// Copyright 2015-2021 SWIM.AI inc.
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

use crate::item_sink::ItemSink;

#[derive(Clone, Debug)]
pub struct ItemSenderComap<Sender, F> {
    sender: Sender,
    f: F,
}

impl<Sender, F> ItemSenderComap<Sender, F> {
    pub fn new(sender: Sender, f: F) -> ItemSenderComap<Sender, F> {
        ItemSenderComap { sender, f }
    }
}

impl<'a, S, T, Sender, F> ItemSink<'a, S> for ItemSenderComap<Sender, F>
where
    Sender: ItemSink<'a, T>,
    F: FnMut(S) -> T,
{
    type Error = Sender::Error;
    type SendFuture = Sender::SendFuture;

    fn send_item(&'a mut self, value: S) -> Self::SendFuture {
        let ItemSenderComap { sender, f } = self;
        sender.send_item(f(value))
    }
}
