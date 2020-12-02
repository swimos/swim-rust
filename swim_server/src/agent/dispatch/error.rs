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

use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::AttachError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

#[derive(Debug)]
pub enum DispatcherError {
    AttachmentFailed(AttachError),
    LaneTaskFailed(LaneIoError),
}

impl Recoverable for DispatcherError {
    fn is_fatal(&self) -> bool {
        !matches!(
            self,
            DispatcherError::AttachmentFailed(AttachError::LaneDoesNotExist(_))
        )
    }
}

#[derive(Debug)]
pub struct DispatcherErrors(bool, Vec<DispatcherError>);

impl Default for DispatcherErrors {
    fn default() -> Self {
        DispatcherErrors::new()
    }
}

impl Display for DispatcherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DispatcherErrors(is_fatal, errors) = self;
        write!(f, "Agent dispatcher terminated with ")?;
        if errors.is_empty() {
            write!(f, "no errors.")
        } else {
            if *is_fatal {
                write!(f, "fatal ")?;
            }
            write!(f, "errors: [")?;
            let mut it = errors.iter();
            if let Some(first) = it.next() {
                write!(f, "{}", first)?;
            }
            for err in it {
                write!(f, ", {}", err)?;
            }
            write!(f, "]")
        }
    }
}

impl Error for DispatcherErrors {}

impl DispatcherErrors {
    pub fn new() -> Self {
        DispatcherErrors(false, vec![])
    }

    pub(super) fn push(&mut self, error: DispatcherError) {
        let DispatcherErrors(is_fatal, errors) = self;
        *is_fatal = *is_fatal || error.is_fatal();
        errors.push(error);
    }

    pub fn is_empty(&self) -> bool {
        let DispatcherErrors(_, errors) = self;
        errors.is_empty()
    }

    pub fn errors(&self) -> &[DispatcherError] {
        let DispatcherErrors(_, errors) = self;
        errors.as_slice()
    }
}

impl Recoverable for DispatcherErrors {
    fn is_fatal(&self) -> bool {
        let DispatcherErrors(is_fatal, _) = self;
        *is_fatal
    }
}

impl Display for DispatcherError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherError::AttachmentFailed(err) => write!(f, "{}", err),
            DispatcherError::LaneTaskFailed(err) => write!(f, "{}", err),
        }
    }
}

impl Error for DispatcherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DispatcherError::AttachmentFailed(err) => Some(err),
            DispatcherError::LaneTaskFailed(err) => Some(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::agent::dispatch::error::{DispatcherError, DispatcherErrors};
    use crate::agent::lane::channels::task::LaneIoError;
    use crate::agent::lane::channels::update::UpdateError;
    use crate::agent::AttachError;
    use std::error::Error;
    use swim_common::warp::path::RelativePath;

    #[test]
    fn display_dispatcher_error() {
        let string = format!(
            "{}",
            DispatcherError::AttachmentFailed(AttachError::LaneStoppedReporting)
        );
        assert_eq!(string, format!("{}", AttachError::LaneStoppedReporting));
        let io_err = || {
            LaneIoError::new(
                RelativePath::new("node", "lane"),
                UpdateError::FeedbackChannelDropped,
                vec![],
            )
        };
        let string = format!("{}", DispatcherError::LaneTaskFailed(io_err()));
        assert_eq!(string, format!("{}", io_err()));
    }

    #[test]
    fn dispatcher_error_source() {
        let error = DispatcherError::AttachmentFailed(AttachError::LaneStoppedReporting);
        let source = error.source();
        let typed = source.and_then(|e| e.downcast_ref::<AttachError>());
        assert!(typed.is_some());
        assert_eq!(typed.unwrap(), &AttachError::LaneStoppedReporting);

        let io_err = LaneIoError::new(
            RelativePath::new("node", "lane"),
            UpdateError::FeedbackChannelDropped,
            vec![],
        );

        let error = DispatcherError::LaneTaskFailed(io_err);
        let source = error.source();
        let typed = source.and_then(|e| e.downcast_ref::<LaneIoError>());
        match typed {
            Some(LaneIoError {
                route,
                update_error: Some(UpdateError::FeedbackChannelDropped),
                uplink_errors,
            }) if uplink_errors.is_empty() => {
                assert_eq!(route, &RelativePath::new("node", "lane"));
            }
            ow => {
                panic!("Unexpected source: {:?}", ow);
            }
        }
    }

    #[test]
    fn display_dispatcher_errors_none() {
        let errors = DispatcherErrors::new();

        let string = format!("{}", errors);
        assert_eq!(string, "Agent dispatcher terminated with no errors.");
    }

    #[test]
    fn display_dispatcher_errors_non_fatal() {
        let mut errors = DispatcherErrors::new();
        errors.push(DispatcherError::AttachmentFailed(
            AttachError::LaneDoesNotExist("lane".to_string()),
        ));

        let string = format!("{}", errors);
        assert_eq!(
            string,
            "Agent dispatcher terminated with errors: [A lane named \"lane\" does not exist.]"
        );
    }

    #[test]
    fn display_dispatcher_errors_fatal() {
        let mut errors = DispatcherErrors::new();
        errors.push(DispatcherError::AttachmentFailed(
            AttachError::LaneDoesNotExist("lane".to_string()),
        ));
        errors.push(DispatcherError::AttachmentFailed(
            AttachError::LaneStoppedReporting,
        ));

        let string = format!("{}", errors);
        assert_eq!(string, "Agent dispatcher terminated with fatal errors: [A lane named \"lane\" does not exist., Failed to attach as the lane stopped reporting its state.]");
    }
}
