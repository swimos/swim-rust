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

use futures::future::BoxFuture;
use swim_api::error::FrameIoError;

use crate::event_handler::BoxEventHandler;

pub trait DownlinkChannel<Context> {
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), FrameIoError>>>;

    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>>;
}

pub type BoxDownlinkChannel<Context> = Box<dyn DownlinkChannel<Context> + Send>;

pub trait DownlinkChannelExt<Context>: DownlinkChannel<Context> {
    fn boxed(self) -> BoxDownlinkChannel<Context>
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl<Context, C> DownlinkChannelExt<Context> for C where C: DownlinkChannel<Context> {}
