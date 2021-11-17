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

use core::fmt::Debug;
use swim_future::request::Request;
use tracing::{event, Level};

/// Provides extension methods for [`Request`] that log errors.
pub trait RequestExt<T> {
    fn send_debug<M: tracing::Value + Debug>(self, data: T, message: M);

    fn send_warn<M: tracing::Value + Debug>(self, data: T, message: M);
}

/// Provides extension methods for [`Request`], where the value type is a [`Result`],
/// that log errors.
pub trait TryRequestExt<T, E> {
    fn send_ok_debug<M: tracing::Value + Debug>(self, data: T, message: M);

    fn send_ok_warn<M: tracing::Value + Debug>(self, data: T, message: M);

    fn send_err_debug<M: tracing::Value + Debug>(self, err: E, message: M);

    fn send_err_warn<M: tracing::Value + Debug>(self, err: E, message: M);
}

impl<T> RequestExt<T> for Request<T> {
    fn send_debug<M: tracing::Value + Debug>(self, data: T, message: M) {
        if self.send(data).is_err() {
            event!(Level::DEBUG, message);
        }
    }
    fn send_warn<M: tracing::Value + Debug>(self, data: T, message: M) {
        if self.send(data).is_err() {
            event!(Level::WARN, message);
        }
    }
}

impl<T, E> TryRequestExt<T, E> for Request<Result<T, E>> {
    fn send_ok_debug<M: tracing::Value + Debug>(self, data: T, message: M) {
        self.send_debug(Ok(data), message)
    }

    fn send_ok_warn<M: tracing::Value + Debug>(self, data: T, message: M) {
        self.send_warn(Ok(data), message)
    }

    fn send_err_debug<M: tracing::Value + Debug>(self, err: E, message: M) {
        self.send_debug(Err(err), message)
    }

    fn send_err_warn<M: tracing::Value + Debug>(self, err: E, message: M) {
        self.send_warn(Err(err), message)
    }
}
