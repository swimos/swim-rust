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

use swim_model::Text;
use tokio::sync::{mpsc, oneshot};

pub enum NodeIntrospectionError {
    NoSuchAgent { node_uri: Text },
    IntrospectionStopped,
}

impl<T> From<mpsc::error::SendError<T>> for NodeIntrospectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        NodeIntrospectionError::IntrospectionStopped
    }
}

impl From<oneshot::error::RecvError> for NodeIntrospectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        NodeIntrospectionError::IntrospectionStopped
    }
}

pub enum LaneIntrospectionError {
    NoSuchAgent { node_uri: Text },
    NoSuchLane { node_uri: Text, lane_name: Text },
    IntrospectionStopped,
}

impl<T> From<mpsc::error::SendError<T>> for LaneIntrospectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        LaneIntrospectionError::IntrospectionStopped
    }
}

impl From<oneshot::error::RecvError> for LaneIntrospectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        LaneIntrospectionError::IntrospectionStopped
    }
}
