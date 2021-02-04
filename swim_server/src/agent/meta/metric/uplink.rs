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

use std::time::{Duration, Instant};

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::agent::meta::metric::sender::{Surjection, TransformedSender};
use crate::agent::meta::metric::ObserverEvent;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct UplinkProfile {
    pub event_delta: i32,
    pub event_rate: i32,
    pub event_count: i64,
    pub command_delta: i32,
    pub command_rate: i32,
    pub command_count: i64,
}

impl UplinkProfile {
    fn build(&mut self) {
        // todo
    }
}

pub struct UplinkSurjection(pub RelativePath);
impl Surjection<UplinkProfile> for UplinkSurjection {
    fn onto(&self, input: UplinkProfile) -> ObserverEvent {
        ObserverEvent::Uplink(self.0.clone(), input)
    }
}

pub struct UplinkObserver {
    sender: TransformedSender<UplinkSurjection, UplinkProfile>,
    last_report: Instant,
    report_interval: Duration,
    profile: UplinkProfile,
}

impl UplinkObserver {
    pub fn new(
        sender: TransformedSender<UplinkSurjection, UplinkProfile>,
        report_interval: Duration,
    ) -> UplinkObserver {
        UplinkObserver {
            sender,
            last_report: Instant::now(),
            report_interval,
            profile: UplinkProfile::default(),
        }
    }

    fn flush(&mut self) {
        let UplinkObserver {
            sender,
            last_report,
            report_interval,
            profile,
        } = self;

        if last_report.elapsed() > *report_interval {
            profile.build();

            match sender.try_send(profile.clone()) {
                Ok(()) => {
                    *last_report = Instant::now();
                }
                Err(_) => {
                    // agent has been stopped or a capacity limit has been reached
                }
            }
        }
    }

    pub fn on_event(&mut self) {
        self.profile.event_count = self.profile.event_count.wrapping_add(1);
        self.flush();
    }

    pub fn on_command(&mut self) {
        self.profile.command_count = self.profile.command_count.wrapping_add(1);
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_uplink_surjection() {
        let path = RelativePath::new("/node", "/lane");
        let (tx, mut rx) = mpsc::channel(1);
        let sender = TransformedSender::new(UplinkSurjection(path.clone()), tx);
        let profile = UplinkProfile::default();

        assert!(sender.try_send(profile.clone()).is_ok());
        assert_eq!(
            rx.recv().now_or_never().unwrap().unwrap(),
            ObserverEvent::Uplink(path, profile)
        );
    }
}
