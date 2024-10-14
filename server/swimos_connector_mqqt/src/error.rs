// Copyright 2015-2024 Swim Inc.
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

use rumqttc::{ClientError, ConnectionError, OptionError};
use swimos_connector::selector::{BadSelector, InvalidLaneSpec, InvalidLanes};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MqttConnectorError {
    #[error("The connector was not initialized correctly.")]
    NotInitialized,
    #[error(transparent)]
    Lanes(#[from] InvalidLanes),
    #[error(transparent)]
    Options(#[from] OptionError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
}

impl From<BadSelector> for MqttConnectorError {
    fn from(err: BadSelector) -> Self {
        MqttConnectorError::Lanes(InvalidLanes::Spec(InvalidLaneSpec::Selector(err)))
    }
}
