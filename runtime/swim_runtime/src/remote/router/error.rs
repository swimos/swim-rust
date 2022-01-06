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

use crate::error::{ConnectionError, ResolutionError};
use swim_utilities::errors::Recoverable;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum RoutingError {
    #[error("`{0:?}`")]
    Resolution(Option<String>),
    #[error("`{0}`")]
    Connection(#[from] ConnectionError),
    #[error("Router dropped")]
    RouterDropped,
}

impl Recoverable for RoutingError {
    fn is_fatal(&self) -> bool {
        match self {
            RoutingError::Resolution(_) => false,
            RoutingError::Connection(e) => e.is_fatal(),
            RoutingError::RouterDropped => true,
        }
    }
}

impl From<ResolutionError> for RoutingError {
    fn from(_: ResolutionError) -> Self {
        RoutingError::Resolution(None)
    }
}

// #[derive(Debug)]
// pub struct RouterError {
//     kind: RouterErrorKind,
//     cause: Option<Box<dyn Error + Send + Sync + 'static>>,
// }
//
// impl RouterError {
//     pub fn new(kind: RouterErrorKind) -> RouterError {
//         RouterError { kind, cause: None }
//     }
//
//     pub fn with_cause<E>(kind: RouterErrorKind, cause: E) -> RouterError
//     where
//         E: Error + Send + Sync + 'static,
//     {
//         RouterError {
//             kind,
//             cause: Some(Box::new(cause)),
//         }
//     }
//
//     pub fn is_resolution(&self) -> bool {
//         matches!(
//             self.kind,
//             RouterErrorKind::NoAgentAtRoute | RouterErrorKind::Resolution
//         )
//     }
// }
//
// impl Display for RouterError {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         let RouterError { kind, cause } = self;
//         match cause {
//             Some(cause) => {
//                 write!(f, "RouterError({:?}, {})", kind, cause)
//             }
//             None => {
//                 write!(f, "RouterError({:?})", kind)
//             }
//         }
//     }
// }
//
// impl Error for RouterError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         self.cause.as_ref().map(|e| &**e as _)
//     }
// }
//
// #[derive(Debug)]
// pub enum RouterErrorKind {
//     NoAgentAtRoute,
//     ConnectionFailure,
//     RouterDropped,
//     Resolution,
// }
//
// impl From<ResolutionError> for RouterError {
//     fn from(e: ResolutionError) -> Self {
//         RouterError::with_cause(RouterErrorKind::Resolution, e)
//     }
// }
