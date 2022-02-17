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

use crate::model::lifecycle::EventDownlinkLifecycle;

use swim_api::downlink::DownlinkConfig;
use swim_api::error::DownlinkTaskError;

use swim_form::Form;
use swim_model::path::Path;

use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::EventDownlinkModel;

pub async fn event_dowinlink_task<T, LC>(
    _model: EventDownlinkModel<T, LC>,
    _path: Path,
    _config: DownlinkConfig,
    _input: ByteReader,
    _output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: EventDownlinkLifecycle<T>,
{
    todo!()
}
