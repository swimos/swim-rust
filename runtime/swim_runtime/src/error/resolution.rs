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

// use crate::routing::RoutingAddr;
// use swim_model::path::RelativePath;
// use swim_utilities::errors::Recoverable;
// use swim_utilities::routing::uri::RelativeUri;
// use thiserror::Error;

// #[derive(Error, Clone, Debug, PartialEq)]
// pub enum ResolutionError {
//     #[error("Unresolvable routing address: `{0}`")]
//     Addr(RoutingAddr),
//     #[error("Unresolvable host: `{0}`")]
//     Host(String),
//     #[error("No agent at route: `{0}`")]
//     Path(RelativePath),
//     #[error("The routing channel has been dropped")]
//     Dropped,
// }
//
// impl Recoverable for ResolutionError {
//     fn is_fatal(&self) -> bool {
//         true
//     }
// }
